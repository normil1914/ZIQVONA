
"use strict";

const express = require("express");
const http = require("http");
const path = require("path");
const { randomUUID } = require("crypto");
const { Server } = require("socket.io");
const { createClient } = require("@supabase/supabase-js");

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 10000;

const SUPABASE_URL = (
  process.env.SUPABASE_URL ||
  "https://hkirnyqousvmphpwqfga.supabase.co"
).trim();

const SUPABASE_KEY = (
  process.env.SUPABASE_ANON_KEY ||
  process.env.SUPABASE_PUBLISHABLE_KEY ||
  process.env.SUPABASE_KEY ||
  ""
).trim();

if (!SUPABASE_KEY) {
  throw new Error(
    "Mete SUPABASE_ANON_KEY oswa SUPABASE_PUBLISHABLE_KEY nan Render."
  );
}

const authClient = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false
  }
});

const SERVICE_KEY = (
  process.env.SUPABASE_SERVICE_ROLE_KEY || ""
).trim();

const adminClient = SERVICE_KEY
  ? createClient(SUPABASE_URL, SERVICE_KEY, {
      auth: {
        persistSession: false,
        autoRefreshToken: false
      }
    })
  : null;

const io = new Server(server, {
  cors: {
    origin: true,
    methods: ["GET", "POST"]
  },
  maxHttpBufferSize: 12 * 1024 * 1024,
  pingInterval: 25000,
  pingTimeout: 20000
});

const MAX_USERS = 10000;
const MAX_TEXT = 4000;
const MAX_MEDIA = 8 * 1024 * 1024;
const MAX_AVATAR = 2 * 1024 * 1024;
const MAX_STATUS = 120;
const MAX_EVENTS_PER_MINUTE = 120;
const MAX_HISTORY = 5000;

const users = new Map();
const byName = new Map();
const profiles = new Map();
const messages = [];
const limits = new Map();

app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.set("Cache-Control", "no-store");
  res.json({
    ok: true,
    app: "ZIQVONA",
    onlineUsers: users.size,
    maxUsers: MAX_USERS,
    uptime: Math.round(process.uptime())
  });
});

app.get("/config", (req, res) => {
  const iceServers = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" }
  ];

  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL
        .split(",")
        .map(value => value.trim())
        .filter(Boolean),
      username: process.env.TURN_USERNAME || "",
      credential: process.env.TURN_CREDENTIAL || ""
    });
  }

  res.set("Cache-Control", "no-store");
  res.json({ iceServers });
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

function clean(value, max = 1000) {
  return String(value ?? "").trim().slice(0, max);
}

function publicProfile(username) {
  const profile = profiles.get(username) || {};

  return {
    username,
    displayName: profile.displayName || username,
    status: profile.status || "Disponib",
    avatar: profile.avatar || "",
    online: byName.has(username)
  };
}

function allProfiles() {
  const result = {};

  for (const username of profiles.keys()) {
    result[username] = publicProfile(username);
  }

  return result;
}

function sendToUser(username, event, data) {
  const socketId = byName.get(username);

  if (!socketId) return false;

  io.to(socketId).emit(event, data);
  return true;
}

function rateLimited(socket) {
  const now = Date.now();
  let entry = limits.get(socket.id);

  if (!entry || now - entry.start >= 60000) {
    entry = { start: now, count: 0 };
  }

  entry.count += 1;
  limits.set(socket.id, entry);

  return entry.count > MAX_EVENTS_PER_MINUTE;
}

function reportError(socket, event, message) {
  socket.emit(event, { message });
}

function privateHistory(requester, other) {
  // Istwa sa a pa retounen konvèsasyon ant de lòt moun.
  return messages
    .filter(message =>
      (message.from === requester && message.to === other) ||
      (message.from === other && message.to === requester)
    )
    .slice(-200);
}

function presence() {
  io.emit("presence", {
    online: [...byName.keys()]
  });
}

function validUsername(value) {
  return /^[a-zA-Z0-9_.@+-]{1,80}$/.test(value);
}

function profileFromRow(row, metadata, username) {
  return {
    displayName: clean(
      row?.display_name ||
      metadata?.display_name ||
      metadata?.full_name ||
      username,
      50
    ),
    status: clean(
      row?.status ||
      metadata?.status ||
      "Disponib",
      MAX_STATUS
    ),
    avatar: clean(row?.avatar || "", MAX_AVATAR)
  };
}

// Verify Supabase access token before accepting a socket.
io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth?.accessToken;

    if (typeof token !== "string" || !token) {
      return next(
        new Error("Konekte ak Supabase anvan ou antre.")
      );
    }

    const { data, error } = await authClient.auth.getUser(token);

    if (error || !data?.user?.id) {
      return next(
        new Error("Sesyon an pa valab. Rekonekte.")
      );
    }

    socket.data.authUserId = data.user.id;
    socket.data.authUser = data.user;
    next();
  } catch (error) {
    console.error("Socket authentication error:", error.message);
    next(new Error("Nou pa kapab verifye kont lan kounye a."));
  }
});

io.on("connection", async socket => {
  const authUser = socket.data.authUser;
  const authUserId = socket.data.authUserId;
  const metadata = authUser.user_metadata || {};

  try {
    // Use the database profile when the server-only key is configured.
    let row = null;

    if (adminClient) {
      const result = await adminClient
        .from("profiles")
        .select("id,username,display_name,status,avatar")
        .eq("id", authUserId)
        .maybeSingle();

      if (result.error) {
        console.error("Profile lookup failed:", result.error.message);
      } else {
        row = result.data;
      }
    }

    const fallback = clean(
      metadata.username ||
      metadata.user_name ||
      authUser.email ||
      authUser.phone ||
      authUser.id,
      80
    );

    const username = clean(row?.username || fallback, 80);

    if (!username || !validUsername(username)) {
      socket.emit("error-message", {
        message: "Non itilizatè a pa valab. Mete yon username ki kòrèk."
      });
      socket.disconnect(true);
      return;
    }

    // Do not allow two different authenticated accounts to share a username.
    const existingSocketId = byName.get(username);

    if (existingSocketId) {
      const existingUser = users.get(existingSocketId);

      if (existingUser && existingUser.id !== authUserId) {
        socket.emit("error-message", {
          message: "Username sa deja itilize. Chwazi yon username diferan."
        });
        socket.disconnect(true);
        return;
      }

      if (existingSocketId !== socket.id) {
        const previousSocket = io.sockets.sockets.get(existingSocketId);

        if (previousSocket) {
          previousSocket.emit("force-disconnect", {
            message: "Kont sa a konekte sou yon lòt aparèy."
          });
          previousSocket.disconnect(true);
        }
      }
    }

    if (users.size >= MAX_USERS && !users.has(socket.id)) {
      socket.emit("error-message", {
        message: "ZIQVONA beta a plen pou kounye a."
      });
      socket.disconnect(true);
      return;
    }

    socket.data.username = username;

    profiles.set(
      username,
      profileFromRow(row, metadata, username)
    );

    users.set(socket.id, {
      id: authUserId,
      username
    });

    byName.set(username, socket.id);

    socket.emit("registered", {
      me: publicProfile(username),
      profiles: allProfiles(),
      online: [...byName.keys()]
    });

    presence();

    socket.on("register", () => {
      socket.emit("registered", {
        me: publicProfile(username),
        profiles: allProfiles(),
        online: [...byName.keys()]
      });
    });

    socket.on("update-profile", async raw => {
      if (rateLimited(socket)) {
        return reportError(
          socket,
          "error-message",
          "Twòp demann. Tann yon ti moman."
        );
      }

      const old = profiles.get(username) || {};

      const nextProfile = {
        displayName: clean(
          raw?.displayName ?? old.displayName,
          50
        ),
        status: clean(
          raw?.status ?? old.status,
          MAX_STATUS
        ),
        avatar: clean(
          raw?.avatar ?? old.avatar,
          MAX_AVATAR
        )
      };

      if (Buffer.byteLength(nextProfile.avatar, "utf8") > MAX_AVATAR) {
        return reportError(
          socket,
          "error-message",
          "Foto pwofil la twò gwo."
        );
      }

      if (adminClient) {
        const { error } = await adminClient
          .from("profiles")
          .update({
            display_name: nextProfile.displayName,
            status: nextProfile.status,
            avatar: nextProfile.avatar
          })
          .eq("id", authUserId);

        if (error) {
          console.error("Profile update failed:", error.message);
          return reportError(
            socket,
            "error-message",
            "Nou pa kapab sove pwofil la."
          );
        }
      }

      profiles.set(username, nextProfile);
      socket.emit("profile-updated", publicProfile(username));
      presence();
    });

    socket.on("search-users", raw => {
      if (rateLimited(socket)) return;

      const query = clean(raw?.query, 50).toLowerCase();

      const results = [...profiles.keys()]
        .filter(name => name !== username)
        .map(publicProfile)
        .filter(profile =>
          !query ||
          profile.username.toLowerCase().includes(query) ||
          profile.displayName.toLowerCase().includes(query)
        )
        .slice(0, 100);

      socket.emit("search-results", results);
    });

    socket.on("get-history", raw => {
      if (rateLimited(socket)) return;

      const other = clean(raw?.with, 80);

      if (!other || other === username) return;

      socket.emit("history", {
        with: other,
        messages: privateHistory(username, other)
      });
    });

    socket.on("private-message", raw => {
      if (rateLimited(socket)) {
        return reportError(
          socket,
          "message-error",
          "Twòp mesaj. Tann yon ti moman."
        );
      }

      const to = clean(raw?.to, 80);
      const text = clean(raw?.text, MAX_TEXT);

      if (!to || to === username || !text) return;

      if (!byName.has(to)) {
        return reportError(
          socket,
          "message-error",
          "Kontak la pa online."
        );
      }

      const message = {
        id: randomUUID(),
        kind: "text",
        type: "text",
        from: username,
        to,
        text,
        time: new Date().toISOString()
      };

      messages.push(message);

      if (messages.length > MAX_HISTORY) {
        messages.shift();
      }

      socket.emit("private-message", message);
      sendToUser(to, "private-message", message);
    });

    socket.on("media-message", raw => {
      if (rateLimited(socket)) {
        return reportError(
          socket,
          "message-error",
          "Twòp demann medya."
        );
      }

      const to = clean(raw?.to, 80);

      // Support both old and new client property names.
      const requestedKind = raw?.kind || raw?.type;
      const kind = ["image", "video", "voice"].includes(requestedKind)
        ? requestedKind
        : null;

      const data = typeof raw?.data === "string"
        ? raw.data
        : "";

      if (!to || to === username || !kind || !data) {
        return reportError(
          socket,
          "message-error",
          "Medya a pa valab."
        );
      }

      if (!byName.has(to)) {
        return reportError(
          socket,
          "message-error",
          "Kontak la pa online."
        );
      }

      if (Buffer.byteLength(data, "utf8") > MAX_MEDIA) {
        return reportError(
          socket,
          "message-error",
          "Fichye a twò gwo. Eseye yon fichye ki pi piti."
        );
      }

      const message = {
        id: randomUUID(),
        kind,
        type: kind,
        from: username,
        to,
        data,
        duration: Math.max(0, Number(raw?.duration) || 0),
        time: new Date().toISOString()
      };

      messages.push(message);

      if (messages.length > MAX_HISTORY) {
        messages.shift();
      }

      socket.emit("private-message", message);
      sendToUser(to, "private-message", message);
    });

    for (const event of ["typing", "stop-typing"]) {
      socket.on(event, raw => {
        if (rateLimited(socket)) return;

        const to = clean(raw?.to, 80);

        if (!to || to === username) return;

        sendToUser(to, event, { from: username });
      });
    }

    for (const event of [
      "call-offer",
      "call-answer",
      "ice-candidate",
      "call-reject",
      "call-end"
    ]) {
      socket.on(event, raw => {
        if (rateLimited(socket)) return;

        const to = clean(raw?.to, 80);

        if (!to || to === username || !byName.has(to)) {
          if (event === "call-offer") {
            reportError(
              socket,
              "call-error",
              "Kontak la pa online."
            );
          }
          return;
        }

        // Never trust the sender identity supplied by the browser.
        sendToUser(to, event, {
          ...raw,
          from: username,
          to
        });
      });
    }

    socket.on("disconnect", () => {
      limits.delete(socket.id);

      if (byName.get(username) === socket.id) {
        byName.delete(username);
      }

      users.delete(socket.id);
      presence();
    });
  } catch (error) {
    console.error("Socket setup error:", error.message);
    socket.disconnect(true);
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ZIQVONA running on port ${PORT}`);
});
