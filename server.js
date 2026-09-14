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
  }
});

const PORT = process.env.PORT || 10000;

/* =========================
   EXPRESS
========================= */

app.use(express.json());

app.use(
  express.static(path.join(__dirname, "public"))
);

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    version: "2.1.0",
    technology: "Node.js + Express + Socket.IO"
  });
});

app.get("*", (req, res) => {
  res.sendFile(
    path.join(__dirname, "public", "index.html")
  );
});

/* =========================
   ONLINE USERS
========================= */

const users = new Map();

function cleanText(value, max = 500) {
  return String(value || "")
    .trim()
    .slice(0, max);
}

function publicUser(user) {
  return {
    id: user.id,
    name: user.name,
    avatar: user.avatar || "",
    status: user.status || "Online",
    online: true,
    joinedAt: user.joinedAt
  };
}

function getUsers() {
  return Array.from(users.values())
    .map(publicUser)
    .sort((a, b) =>
      a.name.localeCompare(b.name)
    );
}

function sendOnlineUsers() {
  io.emit("online-users", {
    users: getUsers()
  });
}

/* =========================
   SOCKET.IO
========================= */

io.on("connection", (socket) => {
  console.log("Socket connected:", socket.id);

  /* =========================
     REGISTER USER
  ========================= */

  socket.on("register-user", (data = {}) => {
    const name =
      cleanText(data.name, 30) ||
      "ZIQVONA User";

    const avatar =
      cleanText(data.avatar, 500);

    const status =
      cleanText(data.status, 80) ||
      "Online";

    const user = {
      id: socket.id,
      name,
      avatar,
      status,
      online: true,
      joinedAt: Date.now()
    };

    users.set(socket.id, user);

    socket.data.name = name;

    socket.emit("registered", {
      user: publicUser(user)
    });

    sendOnlineUsers();

    socket.broadcast.emit("user-online", {
      user: publicUser(user)
    });

    console.log(`${name} is ONLINE`);
  });

  /* =========================
     UPDATE PROFILE
  ========================= */

  socket.on("profile-update", (data = {}) => {
    const user = users.get(socket.id);

    if (!user) return;

    if (data.name !== undefined) {
      const newName =
        cleanText(data.name, 30);

      if (newName) {
        user.name = newName;
      }
    }

    if (data.avatar !== undefined) {
      user.avatar =
        cleanText(data.avatar, 500);
    }

    if (data.status !== undefined) {
      user.status =
        cleanText(data.status, 80) ||
        "Online";
    }

    socket.data.name = user.name;

    users.set(socket.id, user);

    io.emit("user-updated", {
      user: publicUser(user)
    });

    sendOnlineUsers();
  });

  /* =========================
     GLOBAL CHAT
  ========================= */

  socket.on("global-message", (data = {}) => {
    const sender = users.get(socket.id);

    if (!sender) return;

    const text =
      cleanText(data.text, 2000);

    if (!text) return;

    const message = {
      id:
        Date.now() +
        "-" +
        Math.random()
          .toString(36)
          .slice(2),

      senderId: socket.id,

      senderName: sender.name,

      senderAvatar: sender.avatar || "",

      text,

      time: new Date().toISOString()
    };

    io.emit("global-message", message);
  });

  /* =========================
     PRIVATE CHAT
  ========================= */

  socket.on("private-message", (data = {}) => {
    const sender = users.get(socket.id);

    if (!sender) return;

    const targetId =
      cleanText(data.targetId, 100);

    const text =
      cleanText(data.text, 2000);

    if (!targetId || !text) return;

    if (!users.has(targetId)) {
      socket.emit("message-error", {
        message:
          "Itilizatè sa a pa online kounye a."
      });

      return;
    }

    const message = {
      id:
        Date.now() +
        "-" +
        Math.random()
          .toString(36)
          .slice(2),

      senderId: socket.id,

      senderName: sender.name,

      senderAvatar: sender.avatar || "",

      targetId,

      text,

      time: new Date().toISOString()
    };

    io.to(targetId).emit(
      "private-message",
      message
    );

    socket.emit(
      "private-message",
      message
    );
  });

  /* =========================
     TYPING
  ========================= */

  socket.on(
    "private-typing",
    (data = {}) => {
      const targetId =
        cleanText(data.targetId, 100);

      if (!targetId) return;

      io.to(targetId).emit(
        "private-typing",
        {
          userId: socket.id,
          name:
            socket.data.name ||
            "ZIQVONA User"
        }
      );
    }
  );

  socket.on(
    "private-stop-typing",
    (data = {}) => {
      const targetId =
        cleanText(data.targetId, 100);

      if (!targetId) return;

      io.to(targetId).emit(
        "private-stop-typing",
        {
          userId: socket.id
        }
      );
    }
  );

  /* =========================
     WEBRTC CALL
  ========================= */

  socket.on("call-user", (data = {}) => {
    const targetId =
      cleanText(data.targetId, 100);

    if (!targetId) return;

    if (!users.has(targetId)) {
      socket.emit("call-error", {
        message:
          "Itilizatè a pa online."
      });

      return;
    }

    io.to(targetId).emit(
      "incoming-call",
      {
        from: socket.id,

        fromName:
          socket.data.name ||
          "ZIQVONA User",

        callType:
          data.callType === "voice"
            ? "voice"
            : "video",

        offer:
          data.offer || null
      }
    );
  });

  socket.on("accept-call", (data = {}) => {
    const targetId =
      cleanText(data.targetId, 100);

    if (!targetId) return;

    io.to(targetId).emit(
      "call-accepted",
      {
        from: socket.id,
        answer: data.answer || null
      }
    );
  });

  socket.on("reject-call", (data = {}) => {
    const targetId =
      cleanText(data.targetId, 100);

    if (!targetId) return;

    io.to(targetId).emit(
      "call-rejected",
      {
        from: socket.id
      }
    );
  });

  socket.on("ice-candidate", (data = {}) => {
    const targetId =
      cleanText(data.targetId, 100);

    if (!targetId) return;

    io.to(targetId).emit(
      "ice-candidate",
      {
        from: socket.id,
        candidate:
          data.candidate || null
      }
    );
  });

  socket.on("end-call", (data = {}) => {
    const targetId =
      cleanText(data.targetId, 100);

    if (!targetId) return;

    io.to(targetId).emit(
      "call-ended",
      {
        from: socket.id
      }
    );
  });

  /* =========================
     DISCONNECT
  ========================= */

  socket.on("disconnect", () => {
    const user =
      users.get(socket.id);

    if (!user) return;

    users.delete(socket.id);

    io.emit("user-offline", {
      id: socket.id,
      name: user.name
    });

    sendOnlineUsers();

    console.log(
      `${user.name} is OFFLINE`
    );
  });
});

/* =========================
   START SERVER
========================= */

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `ZIQVONA 2.1.0 running on port ${PORT}`
    );
  }
);
