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

// ===============================
// ZIQVONA SERVER
// ===============================

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Health check
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    message: "ZIQVONA server is running",
    time: new Date().toISOString()
  });
});

// Main page
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ===============================
// ROOMS
// ===============================

const rooms = new Map();

function getRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      users: new Map(),
      createdAt: Date.now()
    });
  }

  return rooms.get(roomId);
}

function getUsers(room) {
  return Array.from(room.users.values()).map((user) => ({
    id: user.id,
    name: user.name,
    avatar: user.avatar || null,
    status: user.status || "Online"
  }));
}

// ===============================
// SOCKET.IO
// ===============================

io.on("connection", (socket) => {
  console.log("🔵 Connected:", socket.id);

  // -------------------------------
  // JOIN ROOM
  // -------------------------------

  socket.on("join-room", (data = {}) => {
    const roomId = String(data.roomId || "ziqvona-room").trim();
    const name = String(data.name || "ZIQVONA User").trim();
    const avatar = data.avatar || null;
    const status = data.status || "Online";

    const room = getRoom(roomId);

    socket.join(roomId);

    socket.data.roomId = roomId;
    socket.data.name = name;

    room.users.set(socket.id, {
      id: socket.id,
      name,
      avatar,
      status
    });

    // Send current users to the new user
    socket.emit("room-users", {
      roomId,
      users: getUsers(room)
    });

    // Tell everyone else a new user joined
    socket.to(roomId).emit("user-joined", {
      id: socket.id,
      name,
      avatar,
      status
    });

    // Update complete user list
    io.to(roomId).emit("users-updated", {
      users: getUsers(room)
    });

    console.log(`👤 ${name} joined room ${roomId}`);
  });

  // -------------------------------
  // CHAT MESSAGE
  // -------------------------------

  socket.on("chat-message", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    const message = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      senderId: socket.id,
      senderName: data.senderName || socket.data.name || "ZIQVONA User",
      text: String(data.text || "").trim(),
      type: data.type || "text",
      time: Date.now()
    };

    if (!message.text && message.type === "text") {
      return;
    }

    io.to(roomId).emit("chat-message", message);
  });

  // -------------------------------
  // PRIVATE MESSAGE
  // -------------------------------

  socket.on("private-message", (data = {}) => {
    const targetId = data.targetId;

    if (!targetId) return;

    const message = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      senderId: socket.id,
      senderName: data.senderName || socket.data.name || "ZIQVONA User",
      text: String(data.text || "").trim(),
      type: data.type || "text",
      time: Date.now()
    };

    io.to(targetId).emit("private-message", message);
  });

  // ===============================
  // WEBRTC SIGNALING
  // ===============================

  // User wants to call another user
  socket.on("call-user", (data = {}) => {
    const targetId = data.targetId;

    if (!targetId) return;

    io.to(targetId).emit("incoming-call", {
      from: socket.id,
      fromName: data.fromName || socket.data.name || "ZIQVONA User",
      callType: data.callType || "video",
      offer: data.offer || null
    });
  });

  // Receiver accepts call
  socket.on("accept-call", (data = {}) => {
    const targetId = data.targetId;

    if (!targetId) return;

    io.to(targetId).emit("call-accepted", {
      from: socket.id,
      answer: data.answer || null
    });
  });

  // ICE candidate
  socket.on("ice-candidate", (data = {}) => {
    const targetId = data.targetId;

    if (!targetId) return;

    io.to(targetId).emit("ice-candidate", {
      from: socket.id,
      candidate: data.candidate || null
    });
  });

  // End call
  socket.on("end-call", (data = {}) => {
    const targetId = data.targetId;

    if (!targetId) return;

    io.to(targetId).emit("call-ended", {
      from: socket.id
    });
  });

  // ===============================
  // GROUP CALL SIGNALING
  // ===============================

  socket.on("group-call-start", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    socket.to(roomId).emit("group-call-started", {
      from: socket.id,
      fromName: socket.data.name,
      callType: data.callType || "video"
    });
  });

  socket.on("group-call-offer", (data = {}) => {
    const targetId = data.targetId;

    if (!targetId) return;

    io.to(targetId).emit("group-call-offer", {
      from: socket.id,
      offer: data.offer || null
    });
  });

  socket.on("group-call-answer", (data = {}) => {
    const targetId = data.targetId;

    if (!targetId) return;

    io.to(targetId).emit("group-call-answer", {
      from: socket.id,
      answer: data.answer || null
    });
  });

  // ===============================
  // TYPING
  // ===============================

  socket.on("typing", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    socket.to(roomId).emit("typing", {
      userId: socket.id,
      name: data.name || socket.data.name
    });
  });

  socket.on("stop-typing", () => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    socket.to(roomId).emit("stop-typing", {
      userId: socket.id
    });
  });

  // ===============================
  // PROFILE UPDATE
  // ===============================

  socket.on("profile-update", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    const room = rooms.get(roomId);

    if (!room || !room.users.has(socket.id)) return;

    const user = room.users.get(socket.id);

    user.name = data.name || user.name;
    user.avatar = data.avatar || user.avatar;
    user.status = data.status || user.status;

    socket.data.name = user.name;

    io.to(roomId).emit("users-updated", {
      users: getUsers(room)
    });
  });

  // ===============================
  // EMOJI / REACTION
  // ===============================

  socket.on("reaction", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    io.to(roomId).emit("reaction", {
      userId: socket.id,
      name: socket.data.name,
      emoji: data.emoji || "❤️",
      messageId: data.messageId || null
    });
  });

  // ===============================
  // DISCONNECT
  // ===============================

  socket.on("disconnect", () => {
    const roomId = socket.data.roomId;

    console.log("🔴 Disconnected:", socket.id);

    if (!roomId) return;

    const room = rooms.get(roomId);

    if (!room) return;

    const user = room.users.get(socket.id);

    room.users.delete(socket.id);

    socket.to(roomId).emit("user-left", {
      id: socket.id,
      name: user ? user.name : "ZIQVONA User"
    });

    io.to(roomId).emit("users-updated", {
      users: getUsers(room)
    });

    // Delete empty rooms
    if (room.users.size === 0) {
      rooms.delete(roomId);
    }
  });
});

// ===============================
// START SERVER
// ===============================

server.listen(PORT, "0.0.0.0", () => {
  console.log("=================================");
  console.log("🚀 ZIQVONA SERVER STARTED");
  console.log(`🌐 Port: ${PORT}`);
  console.log("💬 Chat: READY");
  console.log("👥 Multi-user rooms: READY");
  console.log("📞 Voice signaling: READY");
  console.log("📹 Video signaling: READY");
  console.log("👨‍👩‍👧‍👦 Group calls: READY");
  console.log("=================================");
});
