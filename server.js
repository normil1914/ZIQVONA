
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

const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
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

const users = new Map();
const byName = new Map();
const profiles = new Map();
const messages = [];
const limits = new Map();
const calls = new Map();

app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
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
      urls: process.env.TURN_URL.split(",").map(x => x.trim()),
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
  const p = profiles.get(username) || {};
  return {
    username,
    displayName: p.displayName || username,
    status: p.status || "Disponib",
    avatar: p.avatar || "",
    online: byName.has(username)
  };
}

function allProfiles() {
  const result = {};
  for (const name of profiles.keys()) {
    result[name] = publicProfile(name);
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

  entry.count++;
  limits.set(socket.id, entry);
  return entry.count > 120;
}

function reportError(socket, event, message) {
  socket.emit(event, { message });
}

function privateHistory(a, b) {
  return messages.filter(m =>
    (m.from === a && m.to === b) ||
    (m.from === b && m.to === a)
  ).slice(-200);
}

function presence() {
  io.emit("presence", { online: [...byName.keys()] });
}

io.use(async (socket, next) => {
  try {
    if (users.size >= MAX_USERS) {
      return next(new Error("ZIQVONA beta a plen pou kounye a."));
    }

    const token = socket.handshake.auth?.accessToken;
    if (typeof token !== "string" || !token) {
      return next(new Error("Konekte ak Supabase anvan ou antre."));
    }

    const { data, error } = await authClient.auth.getUser(token);

    if (error || !data?.user?.id) {
      return next(new Error("Sesyon an pa valab. Rekonekte."));
    }

    const user = data.user;
    let profile = null;

    if (adminClient) {
      const result = await adminClient
        .from("profiles")
        .select("id,username,display_name,status,avatar")
        .eq("id", user.id)
        .maybeSingle();

      if (!result.error) profile = result.data;
    }

    const meta = user.user_metadata || {};
    const username = clean(
      profile?.username ||
      meta.username ||
      meta.user_name ||
      user.email ||
      user.phone ||
      user.id,
      80
    );

    if (!username) {
      return next(new Error("Kont sa a pa gen idantite valab."));
    }

    socket.data.authUserId = user.id;
    socket.data.username = username;
    socket.data.profileRow = profile;
    socket.data.metadata = meta;

    next();
  } catch (error) {
    console.error("Authentication error:", error.message);
    next(new Error("Nou pa kapab verifye kont lan kounye a."));
  }
});

io.on("connection", socket => {
  const username = socket.data.username;
  const authUserId = socket.data.authUserId;
  const oldSocketId = byName.get(username);

  if (oldSocketId && oldSocketId !== socket.id) {
    const oldSocket = io.sockets.sockets.get(oldSocketId);
    if (oldSocket) {
      oldSocket.emit("force-disconnect", {
        message: "Kont sa a konekte sou yon lòt aparèy."
      });
      oldSocket.disconnect(true);
    }
  }

  const row = socket.data.profileRow || {};
  const meta = socket.data.metadata || {};

  profiles.set(username, {
    displayName: clean(
      row.display_name || meta.display_name ||
      meta.full_name || username, 50
    ),
    status: clean(row.status || meta.status || "Disponib", 120),
    avatar: clean(row.avatar || "", 3000000)
  });

  users.set(socket.id, { id: authUserId, username });
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
      return reportError(socket, "error-message", "Twòp demann.");
    }

    const old = profiles.get(username) || {};
    const next = {
      displayName: clean(raw?.displayName ?? old.displayName, 50),
      status: clean(raw?.status ?? old.status, 120),
      avatar: clean(raw?.avatar ?? old.avatar, 3000000)
    };

    if (adminClient) {
      const { error } = await adminClient
        .from("profiles")
        .update({
          display_name: next.displayName,
          status: next.status,
          avatar: next.avatar
        })
        .eq("id", authUserId);

      if (error) {
        return reportError(
          socket, "error-message", "Nou pa kapab sove pwofil la."
        );
      }
    }

    profiles.set(username, next);
    socket.emit("profile-updated", publicProfile(username));
    presence();
  });

  socket.on("search-users", raw => {
    if (rateLimited(socket)) return;

    const query = clean(raw?.query, 50).toLowerCase();
    const results = [...profiles.keys()]
      .filter(name => name !== username)
      .map(publicProfile)
      .filter(p =>
        !query ||
        p.username.toLowerCase().includes(query) ||
        p.displayName.toLowerCase().includes(query)
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
      return reportError(socket, "message-error", "Twòp mesaj. Tann yon ti moman.");
    }

    const to = clean(raw?.to, 80);
    const text = clean(raw?.text, MAX_TEXT);

    if (!to || to === username || !text) return;

    if (!byName.has(to)) {
      return reportError(socket, "message-error", "Kontak la pa online.");
    }

    const message = {
      id: randomUUID(),
      kind: "text",
      from: username,
      to,
      text,
      time: new Date().toISOString()
    };

    messages.push(message);
    if (messages.length > 5000) messages.shift();

    socket.emit("private-message", message);
    sendToUser(to, "private-message", message);
  });

  socket.on("media-message", raw => {
    if (rateLimited(socket)) {
      return reportError(socket, "message-error", "Twòp demann medya.");
    }

    const to = clean(raw?.to, 80);
    const kind = ["image", "video", "voice"].includes(raw?.kind)
      ? raw.kind
      : null;
    const data = typeof raw?.data === "string" ? raw.data : "";

    if (!to || to === username || !kind || !data) {
      return reportError(socket, "message-error", "Medya a pa valab.");
    }

    if (!byName.has(to)) {
      return reportError(socket, "message-error", "Kontak la pa online.");
    }

    if (Buffer.byteLength(data, "utf8") > MAX_MEDIA) {
      return reportError(socket, "message-error", "Fichye a twò gwo.");
    }

    const message = {
      id: randomUUID(),
      kind,
      from: username,
      to,
      data,
      duration: Math.max(0, Number(raw?.duration) || 0),
      time: new Date().toISOString()
    };

    messages.push(message);
    if (messages.length > 5000) messages.shift();

    socket.emit("private-message", message);
    sendToUser(to, "private-message", message);
  });

  for (const event of ["typing", "stop-typing"]) {
    socket.on(event, raw => {
      if (rateLimited(socket)) return;
      const to = clean(raw?.to, 80);
      if (to && to !== username) {
        sendToUser(to, event, { from: username });
      }
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
          reportError(socket, "call-error", "Kontak la pa online.");
        }
        return;
      }

      sendToUser(to, event, { ...raw, from: username, to });
    });
  }

  socket.on("disconnect", () => {
    limits.delete(socket.id);

    for (const [roomId, call] of calls) {
      call.sockets.delete(socket.id);
      if (!call.sockets.size) calls.delete(roomId);
    }

    if (byName.get(username) === socket.id) {
      byName.delete(username);
    }

    users.delete(socket.id);
    presence();
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ZIQVONA running on port ${PORT}`);
});
