const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const PORT = process.env.PORT || 10000;
const users = new Map();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    version: "2.3.0",
    technology: "Node.js + Express + Socket.IO"
  });
});

app.use((req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

function clean(v, max = 1000) {
  return String(v ?? "").trim().slice(0, max);
}

function userView(u) {
  return {
    id: u.id,
    name: u.name,
    status: u.status || "Online",
    avatar: u.avatar || "",
    online: true
  };
}

function broadcastContacts() {
  io.emit(
    "contacts",
    [...users.values()]
      .map(userView)
      .sort((a, b) => a.name.localeCompare(b.name))
  );
}

io.on("connection", socket => {
  console.log("Connected:", socket.id);

  socket.on("register", data => {
    const old = users.get(socket.id);

    const user = {
      id: socket.id,
      name: clean(data?.name, 40) || old?.name || "ZIQVONA User",
      status: clean(data?.status, 120) || old?.status || "Online",
      avatar: clean(data?.avatar, 200000) || old?.avatar || ""
    };

    users.set(socket.id, user);
    socket.data.name = user.name;

    socket.emit("me", userView(user));
    broadcastContacts();
  });

  socket.on("update-profile", data => {
    const user = users.get(socket.id);
    if (!user) return;

    if (data.name !== undefined) {
      user.name = clean(data.name, 40) || user.name;
    }

    if (data.status !== undefined) {
      user.status = clean(data.status, 120) || "Online";
    }

    if (data.avatar !== undefined) {
      user.avatar = clean(data.avatar, 200000);
    }

    socket.data.name = user.name;

    socket.emit("me", userView(user));
    broadcastContacts();
  });

  socket.on("global-message", data => {
    const user = users.get(socket.id);
    const text = clean(data?.text, 4000);

    if (!user || !text) return;

    io.emit("global-message", {
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      senderId: user.id,
      senderName: user.name,
      senderAvatar: user.avatar,
      text,
      time: new Date().toISOString()
    });
  });

  socket.on("private-message", data => {
    const user = users.get(socket.id);
    const targetId = clean(data?.targetId, 100);
    const text = clean(data?.text, 4000);

    if (!user || !targetId || !text) return;

    if (!users.has(targetId)) {
      socket.emit("message-error", {
        message: "Kontak sa a pa online kounye a."
      });
      return;
    }

    const message = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      senderId: user.id,
      senderName: user.name,
      senderAvatar: user.avatar,
      targetId,
      text,
      time: new Date().toISOString()
    };

    io.to(targetId).emit("private-message", message);
    socket.emit("private-message", message);
  });

  socket.on("typing", data => {
    const targetId = clean(data?.targetId, 100);
    const user = users.get(socket.id);

    if (targetId && user) {
      io.to(targetId).emit("typing", {
        from: socket.id,
        name: user.name
      });
    }
  });

  socket.on("stop-typing", data => {
    const targetId = clean(data?.targetId, 100);

    if (targetId) {
      io.to(targetId).emit("stop-typing", {
        from: socket.id
      });
    }
  });

  // =========================
  // WEBRTC CALL SIGNALING
  // =========================

  socket.on("call-user", data => {
    const targetId = clean(data?.targetId, 100);
    const user = users.get(socket.id);

    if (!targetId || !user || !users.has(targetId)) {
      socket.emit("call-error", {
        message: "Kontak la pa online."
      });
      return;
    }

    io.to(targetId).emit("incoming-call", {
      from: socket.id,
      fromName: user.name,
      callType: data.callType === "voice" ? "voice" : "video",
      offer: data.offer || null
    });
  });

  socket.on("accept-call", data => {
    const targetId = clean(data?.targetId, 100);

    if (targetId) {
      io.to(targetId).emit("call-accepted", {
        from: socket.id,
        answer: data.answer || null
      });
    }
  });

  socket.on("reject-call", data => {
    const targetId = clean(data?.targetId, 100);

    if (targetId) {
      io.to(targetId).emit("call-rejected", {
        from: socket.id
      });
    }
  });

  socket.on("ice-candidate", data => {
    const targetId = clean(data?.targetId, 100);

    if (targetId && data.candidate) {
      io.to(targetId).emit("ice-candidate", {
        from: socket.id,
        candidate: data.candidate
      });
    }
  });

  socket.on("end-call", data => {
    const targetId = clean(data?.targetId, 100);

    if (targetId) {
      io.to(targetId).emit("call-ended", {
        from: socket.id
      });
    }
  });

  socket.on("disconnect", () => {
    const user = users.get(socket.id);

    users.delete(socket.id);
    broadcastContacts();

    if (user) {
      console.log("Disconnected:", user.name);
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ZIQVONA 2.3.0 running on port ${PORT}`);
});
