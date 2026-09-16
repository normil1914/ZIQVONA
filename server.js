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
  maxHttpBufferSize: 20 * 1024 * 1024
});

const PORT = process.env.PORT || 10000;

const users = new Map();
const privateMessages = new Map();
const pendingIce = new Map();

const MAX_NAME = 40;
const MAX_STATUS = 120;
const MAX_TEXT = 4000;
const MAX_AVATAR = 2 * 1024 * 1024;

app.use(express.json({ limit: "20mb" }));
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    version: "3.0.0",
    technology: "Node.js + Express + Socket.IO",
    users: users.size
  });
});

app.get("/config", (req, res) => {
  const iceServers = [
    {
      urls: [
        "stun:stun.l.google.com:19302",
        "stun:stun1.l.google.com:19302"
      ]
    }
  ];

  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL.split(",").map(v => v.trim()),
      username: process.env.TURN_USERNAME || "",
      credential: process.env.TURN_CREDENTIAL || ""
    });
  }

  res.json({ iceServers });
});

app.use((req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

function clean(value, max = 1000) {
  return String(value ?? "")
    .trim()
    .slice(0, max);
}

function makeId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function userView(user) {
  return {
    id: user.id,
    name: user.name,
    status: user.status || "Online",
    avatar: user.avatar || "",
    online: true
  };
}

function broadcastContacts() {
  const contacts = [...users.values()]
    .map(userView)
    .sort((a, b) => a.name.localeCompare(b.name));

  io.emit("contacts", contacts);
}

function savePrivateMessage(targetId, message) {
  const key = [message.senderId, targetId].sort().join(":");

  if (!privateMessages.has(key)) {
    privateMessages.set(key, []);
  }

  const list = privateMessages.get(key);

  list.push(message);

  if (list.length > 500) {
    list.splice(0, list.length - 500);
  }
}

function getPrivateHistory(a, b) {
  const key = [a, b].sort().join(":");
  return privateMessages.get(key) || [];
}

function queueIce(targetId, packet) {
  if (!pendingIce.has(targetId)) {
    pendingIce.set(targetId, []);
  }

  const list = pendingIce.get(targetId);
  list.push(packet);

  if (list.length > 50) {
    list.splice(0, list.length - 50);
  }
}

function flushIce(targetId) {
  const list = pendingIce.get(targetId);

  if (!list) return;

  for (const packet of list) {
    io.to(targetId).emit("ice-candidate", packet);
  }

  pendingIce.delete(targetId);
}

io.on("connection", socket => {
  console.log("CONNECTED:", socket.id);

  socket.on("register", data => {
    const old = users.get(socket.id);

    const name =
      clean(data?.name, MAX_NAME) ||
      old?.name ||
      `ZIQVONA User ${socket.id.slice(-4)}`;

    const status =
      clean(data?.status, MAX_STATUS) ||
      old?.status ||
      "Online";

    let avatar = "";

    if (data?.avatar) {
      avatar = clean(data.avatar, MAX_AVATAR);
    } else if (old?.avatar) {
      avatar = old.avatar;
    }

    const user = {
      id: socket.id,
      name,
      status,
      avatar
    };

    users.set(socket.id, user);

    socket.data.name = user.name;

    socket.emit("me", userView(user));

    broadcastContacts();

    console.log("REGISTER:", user.name, socket.id);
  });

  socket.on("update-profile", data => {
    const user = users.get(socket.id);

    if (!user) return;

    if (data?.name !== undefined) {
      user.name = clean(data.name, MAX_NAME) || user.name;
    }

    if (data?.status !== undefined) {
      user.status =
        clean(data.status, MAX_STATUS) ||
        "Online";
    }

    if (data?.avatar !== undefined) {
      user.avatar = clean(data.avatar, MAX_AVATAR);
    }

    socket.data.name = user.name;

    socket.emit("me", userView(user));

    broadcastContacts();
  });

  socket.on("get-history", data => {
    const targetId = clean(data?.targetId, 100);

    if (!targetId) return;

    const history = getPrivateHistory(socket.id, targetId);

    socket.emit("private-history", {
      targetId,
      messages: history
    });
  });

  socket.on("private-message", data => {
    const user = users.get(socket.id);

    if (!user) return;

    const targetId = clean(data?.targetId, 100);
    const text = clean(data?.text, MAX_TEXT);

    if (!targetId || !text) return;

    if (!users.has(targetId)) {
      socket.emit("message-error", {
        message: "Kontak sa a pa online kounye a."
      });
      return;
    }

    const message = {
      id: makeId(),
      senderId: socket.id,
      senderName: user.name,
      senderAvatar: user.avatar,
      targetId,
      text,
      time: new Date().toISOString()
    };

    savePrivateMessage(targetId, message);

    io.to(targetId).emit("private-message", message);
    socket.emit("private-message", message);
  });

  socket.on("typing", data => {
    const targetId = clean(data?.targetId, 100);
    const user = users.get(socket.id);

    if (!user || !targetId || !users.has(targetId)) return;

    io.to(targetId).emit("typing", {
      from: socket.id,
      name: user.name
    });
  });

  socket.on("stop-typing", data => {
    const targetId = clean(data?.targetId, 100);

    if (!targetId) return;

    io.to(targetId).emit("stop-typing", {
      from: socket.id
    });
  });

  /*
  =========================================================
  WEBRTC ONE-TO-ONE
  =========================================================
  */

  socket.on("call-user", data => {
    const targetId = clean(data?.targetId, 100);
    const user = users.get(socket.id);

    if (!user || !targetId || !users.has(targetId)) {
      socket.emit("call-error", {
        message: "Kontak la pa online."
      });
      return;
    }

    io.to(targetId).emit("incoming-call", {
      from: socket.id,
      fromName: user.name,
      fromAvatar: user.avatar,
      callType:
        data?.callType === "voice"
          ? "voice"
          : "video",
      offer: data?.offer || null
    });

    console.log(
      `CALL ${user.name} -> ${users.get(targetId)?.name}`
    );
  });

  socket.on("accept-call", data => {
    const targetId = clean(data?.targetId, 100);

    if (!targetId || !users.has(targetId)) return;

    io.to(targetId).emit("call-accepted", {
      from: socket.id,
      answer: data?.answer || null
    });

    flushIce(socket.id);
  });

  socket.on("reject-call", data => {
    const targetId = clean(data?.targetId, 100);

    if (!targetId) return;

    io.to(targetId).emit("call-rejected", {
      from: socket.id
    });
  });

  socket.on("ice-candidate", data => {
    const targetId = clean(data?.targetId, 100);

    if (!targetId || !data?.candidate) return;

    const packet = {
      from: socket.id,
      candidate: data.candidate
    };

    if (!users.has(targetId)) {
      return;
    }

    /*
      Forward immediately.

      The browser also queues candidates if its PeerConnection
      is not ready yet.
    */
    io.to(targetId).emit("ice-candidate", packet);
  });

  socket.on("end-call", data => {
    const targetId = clean(data?.targetId, 100);

    if (!targetId) return;

    io.to(targetId).emit("call-ended", {
      from: socket.id
    });
  });

  /*
  =========================================================
  GROUP CALL
  =========================================================
  */

  socket.on("group-invite", data => {
    const targetIds = Array.isArray(data?.targetIds)
      ? data.targetIds
      : [];

    const user = users.get(socket.id);

    if (!user) return;

    const cleanTargets = targetIds
      .map(id => clean(id, 100))
      .filter(id => id && id !== socket.id && users.has(id));

    const roomId =
      clean(data?.roomId, 80) ||
      `group-${makeId()}`;

    for (const targetId of cleanTargets) {
      io.to(targetId).emit("group-invite", {
        roomId,
        from: socket.id,
        fromName: user.name,
        fromAvatar: user.avatar
      });
    }

    socket.emit("group-invite-sent", {
      roomId,
      count: cleanTargets.length
    });
  });

  socket.on("group-join", data => {
    const roomId = clean(data?.roomId, 80);

    if (!roomId) return;

    const roomName = `group:${roomId}`;

    socket.join(roomName);

    const user = users.get(socket.id);

    io.to(roomName).emit("group-member-joined", {
      id: socket.id,
      name: user?.name || "ZIQVONA User",
      avatar: user?.avatar || ""
    });

    socket.emit("group-joined", {
      roomId
    });
  });

  socket.on("group-signal", data => {
    const targetId = clean(data?.targetId, 100);

    if (!targetId || !users.has(targetId)) return;

    io.to(targetId).emit("group-signal", {
      from: socket.id,
      type: data?.type,
      description: data?.description || null,
      candidate: data?.candidate || null
    });
  });

  socket.on("group-leave", data => {
    const roomId = clean(data?.roomId, 80);

    if (!roomId) return;

    const roomName = `group:${roomId}`;

    socket.leave(roomName);

    io.to(roomName).emit("group-member-left", {
      id: socket.id
    });
  });

  socket.on("disconnect", reason => {
    const user = users.get(socket.id);

    users.delete(socket.id);

    pendingIce.delete(socket.id);

    broadcastContacts();

    if (user) {
      io.emit("user-disconnected", {
        id: socket.id
      });

      console.log(
        "DISCONNECTED:",
        user.name,
        reason
      );
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(
    `ZIQVONA 3.0.0 running on port ${PORT}`
  );
});
