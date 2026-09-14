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

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    version: "1.0",
    time: new Date().toISOString()
  });
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ============================
// ROOMS
// ============================

const rooms = new Map();

function createRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Map());
  }

  return rooms.get(roomId);
}

function usersInRoom(roomId) {
  const room = rooms.get(roomId);

  if (!room) return [];

  return Array.from(room.values()).map((user) => ({
    id: user.id,
    name: user.name,
    avatar: user.avatar,
    status: user.status
  }));
}

// ============================
// SOCKET CONNECTION
// ============================

io.on("connection", (socket) => {
  console.log("CONNECTED:", socket.id);

  // --------------------------
  // JOIN ROOM
  // --------------------------

  socket.on("join-room", (data = {}) => {
    const roomId = String(data.roomId || "ziqvona-main");
    const name = String(data.name || "ZIQVONA User");
    const avatar = data.avatar || "";
    const status = data.status || "Online";

    const room = createRoom(roomId);

    socket.join(roomId);

    socket.data.roomId = roomId;
    socket.data.name = name;

    room.set(socket.id, {
      id: socket.id,
      name,
      avatar,
      status
    });

    // Give the new user the current list
    socket.emit("room-users", {
      roomId,
      users: usersInRoom(roomId)
    });

    // Tell existing users about the new user
    socket.to(roomId).emit("user-joined", {
      id: socket.id,
      name,
      avatar,
      status
    });

    // Refresh everyone
    io.to(roomId).emit("users-updated", {
      users: usersInRoom(roomId)
    });

    console.log(`${name} joined ${roomId}`);
  });

  // --------------------------
  // CHAT
  // --------------------------

  socket.on("chat-message", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    const text = String(data.text || "").trim();

    if (!text) return;

    io.to(roomId).emit("chat-message", {
      id: Date.now() + "-" + Math.random(),
      senderId: socket.id,
      senderName: data.senderName || socket.data.name || "User",
      text,
      time: new Date().toISOString()
    });
  });

  // --------------------------
  // PRIVATE MESSAGE
  // --------------------------

  socket.on("private-message", (data = {}) => {
    if (!data.targetId) return;

    const text = String(data.text || "").trim();

    if (!text) return;

    io.to(data.targetId).emit("private-message", {
      id: Date.now() + "-" + Math.random(),
      senderId: socket.id,
      senderName: socket.data.name || "User",
      text,
      time: new Date().toISOString()
    });
  });

  // --------------------------
  // TYPING
  // --------------------------

  socket.on("typing", () => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    socket.to(roomId).emit("typing", {
      userId: socket.id,
      name: socket.data.name || "User"
    });
  });

  socket.on("stop-typing", () => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    socket.to(roomId).emit("stop-typing", {
      userId: socket.id
    });
  });

  // --------------------------
  // PROFILE
  // --------------------------

  socket.on("profile-update", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    const room = rooms.get(roomId);

    if (!room || !room.has(socket.id)) return;

    const user = room.get(socket.id);

    user.name = String(data.name || user.name);
    user.avatar = data.avatar || user.avatar;
    user.status = data.status || user.status;

    socket.data.name = user.name;

    io.to(roomId).emit("users-updated", {
      users: usersInRoom(roomId)
    });
  });

  // ==========================
  // WEBRTC
  // ==========================

  socket.on("call-user", (data = {}) => {
    if (!data.targetId) return;

    io.to(data.targetId).emit("incoming-call", {
      from: socket.id,
      fromName: socket.data.name || "ZIQVONA User",
      callType: data.callType || "video",
      offer: data.offer || null
    });
  });

  socket.on("accept-call", (data = {}) => {
    if (!data.targetId) return;

    io.to(data.targetId).emit("call-accepted", {
      from: socket.id,
      answer: data.answer || null
    });
  });

  socket.on("ice-candidate", (data = {}) => {
    if (!data.targetId) return;

    io.to(data.targetId).emit("ice-candidate", {
      from: socket.id,
      candidate: data.candidate || null
    });
  });

  socket.on("end-call", (data = {}) => {
    if (!data.targetId) return;

    io.to(data.targetId).emit("call-ended", {
      from: socket.id
    });
  });

  // ==========================
  // GROUP CALL
  // ==========================

  socket.on("group-call-start", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    socket.to(roomId).emit("group-call-started", {
      from: socket.id,
      fromName: socket.data.name || "User",
      callType: data.callType || "video"
    });
  });

  socket.on("group-call-offer", (data = {}) => {
    if (!data.targetId) return;

    io.to(data.targetId).emit("group-call-offer", {
      from: socket.id,
      offer: data.offer
    });
  });

  socket.on("group-call-answer", (data = {}) => {
    if (!data.targetId) return;

    io.to(data.targetId).emit("group-call-answer", {
      from: socket.id,
      answer: data.answer
    });
  });

  // --------------------------
  // EMOJI / REACTION
  // --------------------------

  socket.on("reaction", (data = {}) => {
    const roomId = socket.data.roomId;

    if (!roomId) return;

    io.to(roomId).emit("reaction", {
      userId: socket.id,
      name: socket.data.name || "User",
      emoji: data.emoji || "❤️"
    });
  });

  // --------------------------
  // DISCONNECT
  // --------------------------

  socket.on("disconnect", () => {
    console.log("DISCONNECTED:", socket.id);

    const roomId = socket.data.roomId;

    if (!roomId) return;

    const room = rooms.get(roomId);

    if (!room) return;

    const user = room.get(socket.id);

    room.delete(socket.id);

    socket.to(roomId).emit("user-left", {
      id: socket.id,
      name: user ? user.name : "User"
    });

    io.to(roomId).emit("users-updated", {
      users: usersInRoom(roomId)
    });

    if (room.size === 0) {
      rooms.delete(roomId);
    }
  });
});

// ============================
// START
// ============================

server.listen(PORT, "0.0.0.0", () => {
  console.log("--------------------------------");
  console.log("ZIQVONA SERVER");
  console.log("PORT:", PORT);
  console.log("CHAT: READY");
  console.log("ROOMS: READY");
  console.log("MULTI USER: READY");
  console.log("VOICE SIGNALING: READY");
  console.log("VIDEO SIGNALING: READY");
  console.log("GROUP CALL SIGNALING: READY");
  console.log("--------------------------------");
});
