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
    version: "Global Online v2"
  });
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

/*
==================================================
GLOBAL ONLINE USERS
==================================================
*/

const onlineUsers = new Map();

function publicUser(user) {
  return {
    id: user.id,
    name: user.name,
    avatar: user.avatar || "",
    status: user.status || "Online",
    online: true
  };
}

function getOnlineUsers() {
  return Array.from(onlineUsers.values())
    .map(publicUser)
    .sort((a, b) =>
      a.name.localeCompare(b.name)
    );
}

function broadcastOnlineUsers() {
  io.emit("online-users", {
    users: getOnlineUsers()
  });
}

/*
==================================================
CONNECTION
==================================================
*/

io.on("connection", (socket) => {

  console.log("CONNECTED:", socket.id);

  /*
  ================================================
  REGISTER USER
  ================================================
  */

  socket.on("register-user", (data = {}) => {

    const name =
      String(data.name || "ZIQVONA User")
        .trim()
        .slice(0, 30);

    const avatar =
      String(data.avatar || "");

    const status =
      String(data.status || "Online");

    const user = {
      id: socket.id,
      name,
      avatar,
      status,
      online: true,
      connectedAt: Date.now()
    };

    onlineUsers.set(socket.id, user);

    socket.data.name = name;

    socket.emit("registered", {
      user: publicUser(user)
    });

    broadcastOnlineUsers();

    io.emit("user-online", {
      user: publicUser(user)
    });

    console.log(
      `${name} is ONLINE`
    );
  });


  /*
  ================================================
  UPDATE PROFILE
  ================================================
  */

  socket.on("profile-update", (data = {}) => {

    const user =
      onlineUsers.get(socket.id);

    if (!user) return;

    if (data.name) {
      user.name =
        String(data.name)
          .trim()
          .slice(0, 30);
    }

    if (data.avatar !== undefined) {
      user.avatar =
        String(data.avatar);
    }

    if (data.status !== undefined) {
      user.status =
        String(data.status)
          .trim()
          .slice(0, 80);
    }

    socket.data.name = user.name;

    onlineUsers.set(
      socket.id,
      user
    );

    broadcastOnlineUsers();

    io.emit("user-updated", {
      user: publicUser(user)
    });
  });


  /*
  ================================================
  GLOBAL CHAT
  ================================================
  */

  socket.on("global-message", (data = {}) => {

    const user =
      onlineUsers.get(socket.id);

    if (!user) return;

    const text =
      String(data.text || "").trim();

    if (!text) return;

    io.emit("global-message", {
      id:
        Date.now() +
        "-" +
        Math.random()
          .toString(36)
          .slice(2),

      senderId:
        socket.id,

      senderName:
        user.name,

      text,

      time:
        new Date().toISOString()
    });
  });


  /*
  ================================================
  PRIVATE MESSAGE
  ================================================
  */

  socket.on("private-message", (data = {}) => {

    const sender =
      onlineUsers.get(socket.id);

    if (!sender) return;

    const targetId =
      String(data.targetId || "");

    const text =
      String(data.text || "").trim();

    if (!targetId || !text) return;

    const message = {
      id:
        Date.now() +
        "-" +
        Math.random()
          .toString(36)
          .slice(2),

      senderId:
        socket.id,

      senderName:
        sender.name,

      targetId,

      text,

      time:
        new Date().toISOString()
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


  /*
  ================================================
  TYPING
  ================================================
  */

  socket.on("private-typing", (data = {}) => {

    if (!data.targetId) return;

    io.to(data.targetId).emit(
      "private-typing",
      {
        userId: socket.id,
        name:
          socket.data.name ||
          "ZIQVONA User"
      }
    );
  });


  socket.on("private-stop-typing", (data = {}) => {

    if (!data.targetId) return;

    io.to(data.targetId).emit(
      "private-stop-typing",
      {
        userId: socket.id
      }
    );
  });


  /*
  ================================================
  VOICE / VIDEO CALL
  ================================================
  */

  socket.on("call-user", (data = {}) => {

    if (!data.targetId) return;

    io.to(data.targetId).emit(
      "incoming-call",
      {
        from:
          socket.id,

        fromName:
          socket.data.name ||
          "ZIQVONA User",

        callType:
          data.callType ||
          "video",

        offer:
          data.offer || null
      }
    );
  });


  socket.on("accept-call", (data = {}) => {

    if (!data.targetId) return;

    io.to(data.targetId).emit(
      "call-accepted",
      {
        from:
          socket.id,

        answer:
          data.answer || null
      }
    );
  });


  socket.on("ice-candidate", (data = {}) => {

    if (!data.targetId) return;

    io.to(data.targetId).emit(
      "ice-candidate",
      {
        from:
          socket.id,

        candidate:
          data.candidate || null
      }
    );
  });


  socket.on("end-call", (data = {}) => {

    if (!data.targetId) return;

    io.to(data.targetId).emit(
      "call-ended",
      {
        from:
          socket.id
      }
    );
  });


  /*
  ================================================
  DISCONNECT
  ================================================
  */

  socket.on("disconnect", () => {

    const user =
      onlineUsers.get(socket.id);

    if (user) {

      onlineUsers.delete(
        socket.id
      );

      io.emit("user-offline", {
        id: socket.id,
        name: user.name
      });

      broadcastOnlineUsers();

      console.log(
        `${user.name} is OFFLINE`
      );
    }

  });

});


/*
==================================================
START SERVER
==================================================
*/

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      "================================"
    );

    console.log(
      "ZIQVONA GLOBAL ONLINE v2"
    );

    console.log(
      "PORT:",
      PORT
    );

    console.log(
      "GLOBAL USERS: READY"
    );

    console.log(
      "PRIVATE CHAT: READY"
    );

    console.log(
      "VOICE CALL SIGNALING: READY"
    );

    console.log(
      "VIDEO CALL SIGNALING: READY"
    );

    console.log(
      "================================"
    );
  }
);
