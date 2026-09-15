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

const users = new Map();

app.use(express.json());

app.use(
  express.static(
    path.join(__dirname, "public")
  )
);

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    version: "2.2.1",
    technology: "Node.js + Express + Socket.IO"
  });
});

app.use((req, res) => {
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});

function clean(value, max = 500) {
  return String(value ?? "")
    .trim()
    .slice(0, max);
}

function publicUser(user) {
  return {
    id: user.id,
    name: user.name,
    avatar: user.avatar || "",
    status: user.status || "Online",
    online: true
  };
}

function sendContacts() {
  const contacts = Array.from(users.values())
    .map(publicUser)
    .sort((a, b) =>
      a.name.localeCompare(b.name)
    );

  io.emit("contacts", contacts);
}

io.on("connection", (socket) => {
  console.log(
    "ZIQVONA connection:",
    socket.id
  );

  socket.on("register", (data = {}) => {
    const user = {
      id: socket.id,

      name:
        clean(data.name, 40) ||
        "ZIQVONA User",

      avatar:
        clean(data.avatar, 2000),

      status:
        clean(data.status, 100) ||
        "Online"
    };

    users.set(
      socket.id,
      user
    );

    socket.emit("me", {
      ...publicUser(user)
    });

    sendContacts();

    console.log(
      user.name,
      "is ONLINE"
    );
  });

  socket.on(
    "update-profile",
    (data = {}) => {
      const user =
        users.get(socket.id);

      if (!user) {
        return;
      }

      if (
        data.name !== undefined
      ) {
        user.name =
          clean(data.name, 40) ||
          user.name;
      }

      if (
        data.avatar !== undefined
      ) {
        user.avatar =
          clean(
            data.avatar,
            2000
          );
      }

      if (
        data.status !== undefined
      ) {
        user.status =
          clean(
            data.status,
            100
          ) ||
          "Online";
      }

      users.set(
        socket.id,
        user
      );

      socket.emit("me", {
        ...publicUser(user)
      });

      sendContacts();
    }
  );

  socket.on(
    "global-message",
    (data = {}) => {
      const user =
        users.get(socket.id);

      if (!user) {
        return;
      }

      const text =
        clean(data.text, 2000);

      if (!text) {
        return;
      }

      const message = {
        id:
          Date.now() +
          "-" +
          Math.random()
            .toString(36)
            .slice(2),

        senderId:
          user.id,

        senderName:
          user.name,

        senderAvatar:
          user.avatar || "",

        text,

        time:
          new Date().toISOString()
      };

      io.emit(
        "global-message",
        message
      );
    }
  );

  socket.on(
    "private-message",
    (data = {}) => {
      const user =
        users.get(socket.id);

      if (!user) {
        return;
      }

      const targetId =
        clean(
          data.targetId,
          100
        );

      const text =
        clean(
          data.text,
          2000
        );

      if (
        !targetId ||
        !text
      ) {
        return;
      }

      if (
        !users.has(targetId)
      ) {
        socket.emit(
          "message-error",
          {
            message:
              "Kontak sa a pa online kounye a."
          }
        );

        return;
      }

      const message = {
        id:
          Date.now() +
          "-" +
          Math.random()
            .toString(36)
            .slice(2),

        senderId:
          user.id,

        senderName:
          user.name,

        senderAvatar:
          user.avatar || "",

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
    }
  );

  socket.on(
    "typing",
    (data = {}) => {
      const targetId =
        clean(
          data.targetId,
          100
        );

      if (!targetId) {
        return;
      }

      const user =
        users.get(socket.id);

      io.to(targetId).emit(
        "typing",
        {
          from:
            socket.id,

          name:
            user?.name ||
            "ZIQVONA User"
        }
      );
    }
  );

  socket.on(
    "stop-typing",
    (data = {}) => {
      const targetId =
        clean(
          data.targetId,
          100
        );

      if (!targetId) {
        return;
      }

      io.to(targetId).emit(
        "stop-typing",
        {
          from:
            socket.id
        }
      );
    }
  );

  socket.on(
    "disconnect",
    () => {
      const user =
        users.get(socket.id);

      users.delete(
        socket.id
      );

      sendContacts();

      if (user) {
        console.log(
          user.name,
          "is OFFLINE"
        );
      }
    }
  );
});

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `ZIQVONA 2.2.1 running on port ${PORT}`
    );
  }
);
