const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 10000;

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"],
  maxHttpBufferSize: 20 * 1024 * 1024
});

app.use(express.json({ limit: "1mb" }));

/* =========================
   LIMITS
========================= */

const MAX_USERS = 1200;
const MAX_USERNAME = 32;
const MAX_TEXT = 4000;
const MAX_HISTORY = 5000;
const MAX_STATUS = 120;
const MAX_IMAGE_BYTES = 6 * 1024 * 1024;

/* =========================
   MEMORY DATA
========================= */

const users = new Map();
const profiles = new Map();
const contacts = new Map();
const conversations = new Map();
const calls = new Map();

/* =========================
   HELPERS
========================= */

function normalizeUsername(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .slice(0, MAX_USERNAME);
}

function cleanDisplayName(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, MAX_USERNAME);
}

function validUsername(username) {
  return /^[a-zA-Z0-9_.-]{2,32}$/.test(username);
}

function conversationId(a, b) {
  return [a, b].sort().join("::");
}

function publicUser(username) {
  const key = normalizeUsername(username);
  const user = users.get(key);

  if (!user) return null;

  const profile = profiles.get(key) || {};

  return {
    username: key,
    displayName: profile.displayName || user.displayName || key,
    online: !!user.online,
    avatar: profile.avatar || "",
    status: profile.status || ""
  };
}

function allUsers() {
  return Array.from(users.keys())
    .map(publicUser)
    .filter(Boolean)
    .sort((a, b) => {
      if (a.online !== b.online) return a.online ? -1 : 1;
      return a.displayName.localeCompare(b.displayName);
    });
}

function getContacts(username) {
  const set = contacts.get(username) || new Set();

  return Array.from(set)
    .map(publicUser)
    .filter(Boolean)
    .sort((a, b) => {
      if (a.online !== b.online) return a.online ? -1 : 1;
      return a.displayName.localeCompare(b.displayName);
    });
}

function addContact(owner, target) {
  if (!contacts.has(owner)) {
    contacts.set(owner, new Set());
  }

  contacts.get(owner).add(target);
}

function removeContact(owner, target) {
  if (contacts.has(owner)) {
    contacts.get(owner).delete(target);
  }
}

function sendContacts(username) {
  const user = users.get(username);

  if (!user || !user.socketId) return;

  io.to(user.socketId).emit("contacts:update", {
    contacts: getContacts(username)
  });
}

function sendUsers(username) {
  const user = users.get(username);

  if (!user || !user.socketId) return;

  io.to(user.socketId).emit("users:update", {
    users: allUsers()
  });
}

function getSocketForUser(username) {
  const user = users.get(username);

  if (!user || !user.socketId) return null;

  return io.sockets.sockets.get(user.socketId) || null;
}

function isParticipant(call, username) {
  return call &&
    (call.caller === username || call.callee === username);
}

function otherParticipant(call, username) {
  if (!call) return null;
  return call.caller === username ? call.callee : call.caller;
}

function emitToUser(username, event, payload) {
  const socket = getSocketForUser(username);

  if (socket) {
    socket.emit(event, payload);
  }
}

function buildIceServers() {
  const servers = [
    {
      urls: [
        "stun:stun.l.google.com:19302",
        "stun:stun1.l.google.com:19302"
      ]
    }
  ];

  const turnUsername = process.env.TURN_USERNAME;
  const turnPassword = process.env.TURN_PASSWORD;
  const turnHost =
    process.env.TURN_HOST || "global.relay.metered.ca";

  if (turnUsername && turnPassword) {
    servers.push(
      {
        urls: `turn:${turnHost}:80`,
        username: turnUsername,
        credential: turnPassword
      },
      {
        urls: `turn:${turnHost}:80?transport=tcp`,
        username: turnUsername,
        credential: turnPassword
      },
      {
        urls: `turn:${turnHost}:443`,
        username: turnUsername,
        credential: turnPassword
      },
      {
        urls: `turns:${turnHost}:443?transport=tcp`,
        username: turnUsername,
        credential: turnPassword
      }
    );
  }

  return servers;
}

/* =========================
   HTTP
========================= */

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    users: users.size,
    online: Array.from(users.values()).filter(u => u.online).length,
    turnConfigured: !!(
      process.env.TURN_USERNAME &&
      process.env.TURN_PASSWORD
    )
  });
});

app.get("/api/turn", (req, res) => {
  res.json({
    iceServers: buildIceServers(),
    turnConfigured: !!(
      process.env.TURN_USERNAME &&
      process.env.TURN_PASSWORD
    )
  });
});

app.use(express.static(path.join(__dirname, "public")));

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

/* =========================
   SOCKET.IO
========================= */

io.on("connection", socket => {
  let username = null;

  /* =========================
     LOGIN
  ========================= */

  socket.on("login", data => {
    const requested = cleanDisplayName(data?.username);
    const key = normalizeUsername(requested);

    if (!validUsername(key)) {
      socket.emit("login:error", {
        message:
          "Non itilizatè a dwe gen 2-32 karaktè: lèt, chif, _, -, oswa ."
      });
      return;
    }

    if (!users.has(key) && users.size >= MAX_USERS) {
      socket.emit("login:error", {
        message: "ZIQVONA rive nan limit itilizatè aktyèl la."
      });
      return;
    }

    username = key;

    if (!profiles.has(key)) {
      profiles.set(key, {
        displayName: requested || key,
        avatar: "",
        status: ""
      });
    }

    if (!contacts.has(key)) {
      contacts.set(key, new Set());
    }

    const existing = users.get(key);

    if (existing?.socketId && existing.socketId !== socket.id) {
      const oldSocket = io.sockets.sockets.get(existing.socketId);

      if (oldSocket) {
        oldSocket.emit("session:replaced");
        oldSocket.disconnect(true);
      }
    }

    users.set(key, {
      username: key,
      displayName: requested || key,
      socketId: socket.id,
      online: true,
      connectedAt: Date.now()
    });

    socket.data.username = key;

    socket.emit("login:success", {
      user: publicUser(key),
      users: allUsers(),
      contacts: getContacts(key),
      profile: profiles.get(key)
    });

    io.emit("users:update", {
      users: allUsers()
    });
  });

  /* =========================
     PROFILE
  ========================= */

  socket.on("profile:update", data => {
    if (!username) return;

    const oldProfile = profiles.get(username) || {};

    const displayName =
      cleanDisplayName(data?.displayName) ||
      oldProfile.displayName ||
      username;

    const status = String(data?.status || "")
      .trim()
      .slice(0, MAX_STATUS);

    let avatar = oldProfile.avatar || "";

    if (typeof data?.avatar === "string") {
      if (
        data.avatar === "" ||
        /^data:image\/(png|jpeg|jpg|webp|gif);base64,/i.test(data.avatar)
      ) {
        if (Buffer.byteLength(data.avatar, "utf8") <= 3 * 1024 * 1024) {
          avatar = data.avatar;
        }
      }
    }

    profiles.set(username, {
      displayName,
      avatar,
      status
    });

    const user = users.get(username);

    if (user) {
      user.displayName = displayName;
      users.set(username, user);
    }

    socket.emit("profile:updated", {
      profile: profiles.get(username)
    });

    io.emit("users:update", {
      users: allUsers()
    });

    for (const key of users.keys()) {
      sendContacts(key);
    }
  });

  /* =========================
     CONTACTS
  ========================= */

  socket.on("contact:add", data => {
    if (!username) return;

    const target = normalizeUsername(data?.username);

    if (!users.has(target) || target === username) {
      socket.emit("contact:error", {
        message: "Moun sa pa disponib."
      });
      return;
    }

    addContact(username, target);

    sendContacts(username);

    socket.emit("contact:added", {
      username: target
    });
  });

  socket.on("contact:remove", data => {
    if (!username) return;

    const target = normalizeUsername(data?.username);

    removeContact(username, target);

    sendContacts(username);

    socket.emit("contact:removed", {
      username: target
    });
  });

  /* =========================
     CHAT JOIN
  ========================= */

  socket.on("chat:join", data => {
    if (!username) return;

    const target = normalizeUsername(data?.username);

    if (!users.has(target) || target === username) {
      socket.emit("chat:error", {
        message: "Moun sa pa egziste oswa li pa disponib."
      });
      return;
    }

    const room = conversationId(username, target);

    socket.join(room);

    addContact(username, target);

    const history = conversations.get(room) || [];

    socket.emit("chat:history", {
      with: target,
      messages: history
    });

    sendContacts(username);
  });

  /* =========================
     MESSAGES
  ========================= */

  socket.on("message:send", data => {
    if (!username) return;

    const target = normalizeUsername(data?.to);

    if (!target || target === username || !users.has(target)) {
      socket.emit("message:error", {
        message: "Destinatè a pa disponib."
      });
      return;
    }

    const type = data?.type === "image" ? "image" : "text";

    let message;

    if (type === "text") {
      const text = String(data?.text || "").trim();

      if (!text) return;

      if (text.length > MAX_TEXT) {
        socket.emit("message:error", {
          message: "Mesaj la twò long."
        });
        return;
      }

      message = {
        id: crypto.randomUUID(),
        from: username,
        to: target,
        type: "text",
        text,
        createdAt: Date.now()
      };
    } else {
      const image = String(data?.data || "");

      if (
        !/^data:image\/(png|jpeg|jpg|webp|gif);base64,/i.test(image)
      ) {
        socket.emit("message:error", {
          message: "Foto a pa valid."
        });
        return;
      }

      if (Buffer.byteLength(image, "utf8") > MAX_IMAGE_BYTES) {
        socket.emit("message:error", {
          message: "Foto a twò gwo. Eseye yon foto ki pi piti."
        });
        return;
      }

      message = {
        id: crypto.randomUUID(),
        from: username,
        to: target,
        type: "image",
        data: image,
        fileName: String(data?.fileName || "photo.jpg").slice(0, 100),
        createdAt: Date.now()
      };
    }

    const room = conversationId(username, target);

    if (!conversations.has(room)) {
      conversations.set(room, []);
    }

    const history = conversations.get(room);

    history.push(message);

    if (history.length > MAX_HISTORY) {
      history.splice(0, history.length - MAX_HISTORY);
    }

    conversations.set(room, history);

    /* Automatically make both people contacts after messaging. */
    addContact(username, target);
    addContact(target, username);

    emitToUser(username, "message:new", message);
    emitToUser(target, "message:new", message);

    sendContacts(username);
    sendContacts(target);
  });

  /* =========================
     TYPING
  ========================= */

  socket.on("typing", data => {
    if (!username) return;

    const target = normalizeUsername(data?.to);

    if (!target) return;

    emitToUser(target, "typing", {
      from: username,
      typing: !!data?.typing
    });
  });

  /* =========================
     CALL START
  ========================= */

  socket.on("call:start", data => {
    if (!username) return;

    const target = normalizeUsername(data?.to);
    const type = data?.type === "video" ? "video" : "audio";

    if (!target || target === username || !users.has(target)) {
      socket.emit("call:error", {
        message: "Moun ou vle rele a pa disponib."
      });
      return;
    }

    const targetSocket = getSocketForUser(target);

    if (!targetSocket) {
      socket.emit("call:error", {
        message: "Moun sa pa online kounye a."
      });
      return;
    }

    const callId = crypto.randomUUID();

    const call = {
      id: callId,
      caller: username,
      callee: target,
      type,
      state: "ringing",
      createdAt: Date.now()
    };

    calls.set(callId, call);

    targetSocket.emit("call:incoming", {
      callId,
      from: username,
      type
    });

    socket.emit("call:started", {
      callId,
      to: target,
      type
    });
  });

  /* =========================
     CALL ACCEPT
  ========================= */

  socket.on("call:accept", data => {
    if (!username) return;

    const callId = String(data?.callId || "");
    const call = calls.get(callId);

    if (!call || call.callee !== username) return;

    call.state = "accepted";
    calls.set(callId, call);

    emitToUser(call.caller, "call:accepted", {
      callId,
      by: username,
      type: call.type
    });
  });

  /* =========================
     CALL REJECT
  ========================= */

  socket.on("call:reject", data => {
    if (!username) return;

    const callId = String(data?.callId || "");
    const call = calls.get(callId);

    if (!isParticipant(call, username)) return;

    const other = otherParticipant(call, username);

    emitToUser(other, "call:rejected", {
      callId,
      by: username
    });

    calls.delete(callId);
  });

  /* =========================
     CALL END
  ========================= */

  socket.on("call:end", data => {
    if (!username) return;

    const callId = String(data?.callId || "");
    const call = calls.get(callId);

    if (!isParticipant(call, username)) return;

    const other = otherParticipant(call, username);

    emitToUser(other, "call:ended", {
      callId,
      by: username
    });

    socket.emit("call:ended", {
      callId,
      by: username
    });

    calls.delete(callId);
  });

  /* =========================
     WEBRTC OFFER
  ========================= */

  socket.on("webrtc:offer", data => {
    if (!username) return;

    const call = calls.get(String(data?.callId || ""));

    if (!isParticipant(call, username)) return;

    const other = otherParticipant(call, username);

    emitToUser(other, "webrtc:offer", {
      callId: call.id,
      offer: data.offer
    });
  });

  /* =========================
     WEBRTC ANSWER
  ========================= */

  socket.on("webrtc:answer", data => {
    if (!username) return;

    const call = calls.get(String(data?.callId || ""));

    if (!isParticipant(call, username)) return;

    const other = otherParticipant(call, username);

    emitToUser(other, "webrtc:answer", {
      callId: call.id,
      answer: data.answer
    });
  });

  /* =========================
     WEBRTC ICE
  ========================= */

  socket.on("webrtc:ice-candidate", data => {
    if (!username) return;

    const call = calls.get(String(data?.callId || ""));

    if (!isParticipant(call, username)) return;

    const other = otherParticipant(call, username);

    emitToUser(other, "webrtc:ice-candidate", {
      callId: call.id,
      candidate: data.candidate
    });
  });

  /* =========================
     DISCONNECT
  ========================= */

  socket.on("disconnect", () => {
    if (!username) return;

    const user = users.get(username);

    if (user && user.socketId === socket.id) {
      user.online = false;
      user.socketId = null;

      users.set(username, user);
    }

    for (const [callId, call] of calls.entries()) {
      if (!isParticipant(call, username)) continue;

      const other = otherParticipant(call, username);

      emitToUser(other, "call:ended", {
        callId,
        by: username
      });

      calls.delete(callId);
    }

    io.emit("users:update", {
      users: allUsers()
    });
  });
});

/* =========================
   START
========================= */

server.listen(PORT, () => {
  console.log(`ZIQVONA running on port ${PORT}`);
  console.log(
    `TURN configured: ${
      !!(process.env.TURN_USERNAME && process.env.TURN_PASSWORD)
    }`
  );
});
