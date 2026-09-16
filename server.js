const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"]
});

const PORT = process.env.PORT || 10000;

// ----------------------------------------------------
// EXPRESS
// ----------------------------------------------------

app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    online: users.size,
    time: new Date().toISOString()
  });
});

// ----------------------------------------------------
// MEMORY STORAGE
// ----------------------------------------------------

// username -> user object
const users = new Map();

// socket.id -> username
const socketUsers = new Map();

// username -> socket.id
const userSockets = new Map();

// conversationId -> messages[]
const conversations = new Map();

// username -> profile
const profiles = new Map();

// ----------------------------------------------------
// LIMITS
// ----------------------------------------------------

const MAX_USERNAME = 40;
const MAX_MESSAGE = 4000;
const MAX_STATUS = 120;
const MAX_HISTORY = 500;
const MAX_USERS = 1200;

// ----------------------------------------------------
// HELPERS
// ----------------------------------------------------

function cleanText(value, maxLength = MAX_MESSAGE) {
  if (typeof value !== "string") return "";
  return value.trim().slice(0, maxLength);
}

function normalizeUsername(username) {
  return cleanText(username, MAX_USERNAME)
    .replace(/\s+/g, " ")
    .trim();
}

function userKey(username) {
  return username.toLowerCase();
}

function makeConversationId(a, b) {
  return [userKey(a), userKey(b)].sort().join("__");
}

function now() {
  return new Date().toISOString();
}

function getPublicUsers() {
  return Array.from(users.values()).map(user => ({
    username: user.username,
    online: user.online,
    profile: profiles.get(user.username) || {
      avatar: "",
      status: ""
    }
  }));
}

function sendUserList() {
  io.emit("users:update", getPublicUsers());
}

// ----------------------------------------------------
// SOCKET.IO
// ----------------------------------------------------

io.on("connection", socket => {
  console.log("ZIQVONA socket connected:", socket.id);

  // --------------------------------------------------
  // LOGIN / REGISTER
  // --------------------------------------------------

  socket.on("user:login", data => {
    try {
      const username = normalizeUsername(data?.username);

      if (!username) {
        socket.emit("login:error", {
          message: "Tanpri antre non itilizatè a."
        });
        return;
      }

      if (username.length < 2) {
        socket.emit("login:error", {
          message: "Non itilizatè a dwe gen omwen 2 karaktè."
        });
        return;
      }

      const key = userKey(username);

      // Si user deja konekte sou yon lòt device,
      // dekonekte ansyen socket la.
      const oldSocketId = userSockets.get(key);

      if (oldSocketId && oldSocketId !== socket.id) {
        const oldSocket = io.sockets.sockets.get(oldSocketId);

        if (oldSocket) {
          oldSocket.emit("session:replaced");
          oldSocket.disconnect(true);
        }

        socketUsers.delete(oldSocketId);
      }

      const existingProfile = profiles.get(username) || {
        avatar: "",
        status: ""
      };

      const user = {
        username,
        online: true,
        socketId: socket.id,
        connectedAt: now()
      };

      users.set(key, user);
      socketUsers.set(socket.id, key);
      userSockets.set(key, socket.id);

      if (!profiles.has(username)) {
        profiles.set(username, existingProfile);
      }

      socket.username = username;
      socket.userKey = key;

      socket.emit("login:success", {
        username,
        profile: profiles.get(username),
        users: getPublicUsers()
      });

      socket.broadcast.emit("user:online", {
        username
      });

      sendUserList();

      console.log("ZIQVONA login:", username);
    } catch (error) {
      console.error("LOGIN ERROR:", error);

      socket.emit("login:error", {
        message: "Erè pandan koneksyon an."
      });
    }
  });

  // --------------------------------------------------
  // GET USERS
  // --------------------------------------------------

  socket.on("users:get", () => {
    socket.emit("users:update", getPublicUsers());
  });

  // --------------------------------------------------
  // PROFILE
  // --------------------------------------------------

  socket.on("profile:update", data => {
    if (!socket.username) return;

    const username = socket.username;

    const current = profiles.get(username) || {
      avatar: "",
      status: ""
    };

    const avatar =
      typeof data?.avatar === "string"
        ? data.avatar.slice(0, 2 * 1024 * 1024)
        : current.avatar;

    const status = cleanText(
      data?.status ?? current.status,
      MAX_STATUS
    );

    const profile = {
      avatar,
      status
    };

    profiles.set(username, profile);

    socket.emit("profile:updated", {
      username,
      profile
    });

    io.emit("profile:changed", {
      username,
      profile
    });

    sendUserList();
  });

  // --------------------------------------------------
  // PRIVATE CHAT JOIN
  // --------------------------------------------------

  socket.on("chat:join", data => {
    if (!socket.username) return;

    const target = normalizeUsername(data?.username);

    if (!target) return;

    const conversationId = makeConversationId(
      socket.username,
      target
    );

    socket.join(conversationId);

    const history = conversations.get(conversationId) || [];

    socket.emit("chat:history", {
      conversationId,
      with: target,
      messages: history
    });
  });

  // --------------------------------------------------
  // PRIVATE MESSAGE
  // --------------------------------------------------

  socket.on("message:send", data => {
    if (!socket.username) return;

    const to = normalizeUsername(data?.to);
    const text = cleanText(data?.text);

    if (!to || !text) return;

    const targetKey = userKey(to);

    const targetUser = users.get(targetKey);

    if (!targetUser) {
      socket.emit("message:error", {
        message: "Itilizatè sa a pa konekte kounye a."
      });

      return;
    }

    const conversationId = makeConversationId(
      socket.username,
      to
    );

    const message = {
      id:
        Date.now().toString(36) +
        Math.random().toString(36).slice(2),
      conversationId,
      from: socket.username,
      to,
      text,
      type: "text",
      createdAt: now()
    };

    if (!conversations.has(conversationId)) {
      conversations.set(conversationId, []);
    }

    const history = conversations.get(conversationId);

    history.push(message);

    if (history.length > MAX_HISTORY) {
      history.splice(0, history.length - MAX_HISTORY);
    }

    // Voye sèlman bay moun k ap pale yo
    const targetSocketId = userSockets.get(targetKey);

    socket.emit("message:new", message);

    if (targetSocketId) {
      io.to(targetSocketId).emit("message:new", message);
    }
  });

  // --------------------------------------------------
  // TYPING
  // --------------------------------------------------

  socket.on("typing:start", data => {
    if (!socket.username) return;

    const to = normalizeUsername(data?.to);

    if (!to) return;

    const targetSocketId = userSockets.get(userKey(to));

    if (!targetSocketId) return;

    io.to(targetSocketId).emit("typing:start", {
      from: socket.username
    });
  });

  socket.on("typing:stop", data => {
    if (!socket.username) return;

    const to = normalizeUsername(data?.to);

    if (!to) return;

    const targetSocketId = userSockets.get(userKey(to));

    if (!targetSocketId) return;

    io.to(targetSocketId).emit("typing:stop", {
      from: socket.username
    });
  });

  // --------------------------------------------------
  // CALL: START
  // --------------------------------------------------

  socket.on("call:start", data => {
    if (!socket.username) return;

    const to = normalizeUsername(data?.to);
    const callType =
      data?.type === "video"
        ? "video"
        : "audio";

    if (!to) return;

    const targetSocketId = userSockets.get(userKey(to));

    if (!targetSocketId) {
      socket.emit("call:error", {
        message: `${to} pa online kounye a.`
      });

      return;
    }

    const callId =
      Date.now().toString(36) +
      Math.random().toString(36).slice(2);

    const callerProfile =
      profiles.get(socket.username) || {
        avatar: "",
        status: ""
      };

    io.to(targetSocketId).emit("call:incoming", {
      callId,
      from: socket.username,
      type: callType,
      profile: callerProfile
    });

    socket.emit("call:started", {
      callId,
      to,
      type: callType
    });

    console.log(
      `CALL START ${socket.username} -> ${to} (${callType})`
    );
  });

  // --------------------------------------------------
  // CALL ACCEPT
  // --------------------------------------------------

  socket.on("call:accept", data => {
    if (!socket.username) return;

    const callId = cleanText(data?.callId, 100);
    const from = normalizeUsername(data?.from);

    if (!callId || !from) return;

    const callerSocketId = userSockets.get(userKey(from));

    if (!callerSocketId) return;

    io.to(callerSocketId).emit("call:accepted", {
      callId,
      from: socket.username
    });

    console.log(
      `CALL ACCEPT ${socket.username} <- ${from}`
    );
  });

  // --------------------------------------------------
  // CALL REJECT
  // --------------------------------------------------

  socket.on("call:reject", data => {
    if (!socket.username) return;

    const callId = cleanText(data?.callId, 100);
    const from = normalizeUsername(data?.from);

    if (!callId || !from) return;

    const callerSocketId = userSockets.get(userKey(from));

    if (!callerSocketId) return;

    io.to(callerSocketId).emit("call:rejected", {
      callId,
      from: socket.username
    });
  });

  // --------------------------------------------------
  // CALL END
  // --------------------------------------------------

  socket.on("call:end", data => {
    if (!socket.username) return;

    const callId = cleanText(data?.callId, 100);
    const to = normalizeUsername(data?.to);

    if (!to) return;

    const targetSocketId = userSockets.get(userKey(to));

    if (!targetSocketId) return;

    io.to(targetSocketId).emit("call:ended", {
      callId,
      from: socket.username
    });

    socket.emit("call:ended", {
      callId,
      from: socket.username
    });
  });

  // --------------------------------------------------
  // WEBRTC SIGNALING
  // --------------------------------------------------

  /*
    Sa a se pati ki pèmèt 2 telefòn yo voye:

    - offer
    - answer
    - ICE candidates

    Socket.IO pa transpòte son/video.
    Li sèlman ede 2 devices yo jwenn youn lòt.
  */

  socket.on("webrtc:offer", data => {
    if (!socket.username) return;

    const to = normalizeUsername(data?.to);

    if (!to || !data?.offer) return;

    const targetSocketId = userSockets.get(userKey(to));

    if (!targetSocketId) {
      socket.emit("webrtc:error", {
        message: `${to} pa konekte.`
      });

      return;
    }

    io.to(targetSocketId).emit("webrtc:offer", {
      from: socket.username,
      offer: data.offer,
      callId: data.callId || null
    });
  });

  socket.on("webrtc:answer", data => {
    if (!socket.username) return;

    const to = normalizeUsername(data?.to);

    if (!to || !data?.answer) return;

    const targetSocketId = userSockets.get(userKey(to));

    if (!targetSocketId) return;

    io.to(targetSocketId).emit("webrtc:answer", {
      from: socket.username,
      answer: data.answer,
      callId: data.callId || null
    });
  });

  socket.on("webrtc:ice-candidate", data => {
    if (!socket.username) return;

    const to = normalizeUsername(data?.to);

    if (!to || !data?.candidate) return;

    const targetSocketId = userSockets.get(userKey(to));

    if (!targetSocketId) return;

    io.to(targetSocketId).emit("webrtc:ice-candidate", {
      from: socket.username,
      candidate: data.candidate,
      callId: data.callId || null
    });
  });

  // --------------------------------------------------
  // DISCONNECT
  // --------------------------------------------------

  socket.on("disconnect", reason => {
    const key = socketUsers.get(socket.id);

    if (!key) {
      console.log(
        "Socket disconnected:",
        socket.id,
        reason
      );

      return;
    }

    const user = users.get(key);

    if (user && user.socketId === socket.id) {
      user.online = false;

      socketUsers.delete(socket.id);
      userSockets.delete(key);

      // Nou kenbe user la nan users Map la
      // pou pwofil li toujou egziste.
      users.set(key, user);

      io.emit("user:offline", {
        username: user.username
      });

      sendUserList();

      console.log(
        `ZIQVONA offline: ${user.username}`
      );
    }
  });
});

// ----------------------------------------------------
// SERVER START
// ----------------------------------------------------

server.listen(PORT, "0.0.0.0", () => {
  console.log("=================================");
  console.log("       ZIQVONA SERVER");
  console.log("=================================");
  console.log(`Port: ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || "development"}`);
  console.log("Socket.IO: READY");
  console.log("WebRTC signaling: READY");
  console.log("Private messaging: READY");
  console.log("Profiles: READY");
  console.log("=================================");
});

// ----------------------------------------------------
// ERROR HANDLING
// ----------------------------------------------------

process.on("uncaughtException", error => {
  console.error("UNCAUGHT EXCEPTION:", error);
});

process.on("unhandledRejection", error => {
  console.error("UNHANDLED REJECTION:", error);
});
