
const express = require("express");
const http = require("http");
const path = require("path");
const { randomUUID } = require("crypto");
const { Server } = require("socket.io");
const { createClient } = require("@supabase/supabase-js");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 10000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  throw new Error(
    "ZIQVONA bezwen SUPABASE_URL ak SUPABASE_ANON_KEY nan anviwònman sèvè a."
  );
}

const authClient = createClient(
  SUPABASE_URL,
  SUPABASE_ANON_KEY,
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false
    }
  }
);

// Sèvi ak kle sa a sèlman sou sèvè a.
// Pa janm mete SUPABASE_SERVICE_ROLE_KEY nan index.html.
const adminClient = SUPABASE_SERVICE_ROLE_KEY
  ? createClient(
      SUPABASE_URL,
      SUPABASE_SERVICE_ROLE_KEY,
      {
        auth: {
          persistSession: false,
          autoRefreshToken: false
        }
      }
    )
  : null;

const io = new Server(server, {
  cors: {
    origin: true,
    methods: ["GET", "POST"]
  },
  maxHttpBufferSize: 15 * 1024 * 1024,
  pingInterval: 25000,
  pingTimeout: 20000,
  transports: ["websocket", "polling"]
});

const MAX_USERS = 10000;
const MAX_TEXT = 4000;
const MAX_MEDIA = 10 * 1024 * 1024;
const MAX_STATUS = 120;
const MAX_AVATAR = 2 * 1024 * 1024;
const MAX_EVENTS_PER_MINUTE = 120;

const users = new Map();       // socket ID -> authenticated user
const byName = new Map();     // username -> socket ID
const profiles = new Map();   // username -> public profile
const messages = [];          // temporary memory only
const eventWindows = new Map();
const ipConnections = new Map();
const groupRooms = new Map();

app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    onlineUsers: users.size,
    maxUsers: MAX_USERS,
    uptime: Math.round(process.uptime()),
    timestamp: new Date().toISOString()
  });
});

app.get("/config", (req, res) => {
  const iceServers = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" }
  ];

  const turnUrls = process.env.TURN_URL ||
    process.env.TURN_HOST || "";

  if (turnUrls) {
    iceServers.push({
      urls: turnUrls.split(",").map(v => v.trim()).filter(Boolean),
      username: process.env.TURN_USERNAME || "",
      credential:
        process.env.TURN_CREDENTIAL ||
        process.env.TURN_PASSWORD ||
        ""
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
  let window = eventWindows.get(socket.id);

  if (!window || now - window.start >= 60000) {
    window = { start: now, count: 0 };
  }

  window.count++;
  eventWindows.set(socket.id, window);
  return window.count > MAX_EVENTS_PER_MINUTE;
}

function errorTo(socket, event, message) {
  socket.emit(event, { message });
}

function privateHistory(a, b) {
  return messages
    .filter(m =>
      (m.from === a && m.to === b) ||
      (m.from === b && m.to === a)
    )
    .slice(-200);
}

function broadcastPresence() {
  io.emit("presence", { online: [...byName.keys()] });
}

// Verify the Supabase access token before allowing a socket connection.
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
      return next(new Error("Koneksyon an pa valab. Rekonekte."));
    }

    const authUser = data.user;
    let profile = null;

    // If available, read the user's profile using the server-only key.
    if (adminClient) {
      const { data: row, error: profileError } =
        await adminClient
          .from("profiles")
          .select("id,username,display_name,status,avatar")
          .eq("id", authUser.id)
          .maybeSingle();

      if (!profileError && row) profile = row;
    }

    const metadata = authUser.user_metadata || {};
    const username = clean(
      profile?.username ||
      metadata.username ||
      metadata.user_name ||
      authUser.email ||
      authUser.phone ||
      authUser.id,
      80
    );

    if (!username) {
      return next(new Error("Kont sa a pa gen idantite valab."));
    }

    socket.data.authUserId = authUser.id;
    socket.data.username = username;
    socket.data.profileRow = profile;
    socket.data.metadata = metadata;

    next();
  } catch (err) {
    console.error("Socket authentication failed:", err.message);
    next(new Error("Nou pa ka verifye kont lan kounye a."));
  }
});

io.on("connection", socket => {
  const username = socket.data.username;
  const authUserId = socket.data.authUserId;

  // Do not allow a second active socket to impersonate this username.
  const previousSocketId = byName.get(username);
  if (previousSocketId && previousSocketId !== socket.id) {
    const previousSocket = io.sockets.sockets.get(previousSocketId);
    if (previousSocket) {
      previousSocket.emit("force-disconnect", {
        message: "Kont sa a konekte sou yon lòt aparèy."
      });
      previousSocket.disconnect(true);
    }
  }

  const row = socket.data.profileRow || {};
  const metadata = socket.data.metadata || {};

  const profile = {
    displayName: clean(
      row.display_name ||
      metadata.display_name ||
      metadata.full_name ||
      username,
      50
    ),
    status: clean(row.status || "Disponib", MAX_STATUS),
    avatar: clean(row.avatar || "", MAX_AVATAR)
  };

  profiles.set(username, profile);
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

  broadcastPresence();

  socket.on("register", () => {
    // Identity is determined by the verified token, never by client input.
    socket.emit("registered", {
      me: publicProfile(username),
      profiles: allProfiles(),
      online: [...byName.keys()]
    });
  });

  socket.on("update-profile", async raw => {
    if (rateLimited(socket)) {
      return errorTo(socket, "error-message", "Twòp demann. Tann yon ti moman.");
    }

    const old = profiles.get(username) || {};
    const next = {
      displayName: clean(raw?.displayName ?? old.displayName, 50),
      status: clean(raw?.status ?? old.status, MAX_STATUS),
      avatar: clean(raw?.avatar ?? old.avatar, MAX_AVATAR)
    };

    // Persist the profile when the server-only key is configured.
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
        console.error("Profile update failed:", error.message);
        return errorTo(socket, "error-message", "Nou pa kapab sove pwofil la kounye a.");
      }
    }

    profiles.set(username, next);
    socket.emit("profile-updated", publicProfile(username));
    broadcastPresence();
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
      return errorTo(socket, "message-error", "Twòp mesaj. Tann yon ti moman.");
    }

    const to = clean(raw?.to, 80);
    const text = clean(raw?.text, MAX_TEXT);

    if (!to || to === username || !text) return;

    if (!byName.has(to)) {
      return errorTo(socket, "message-error", "Kontak la pa online kounye a.");
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
      return errorTo(socket, "message-error", "Twòp medya. Tann yon ti moman.");
    }

    const to = clean(raw?.to, 80);
    const kind = ["image", "video", "voice"].includes(raw?.kind)
      ? raw.kind
      : null;
    const data = typeof raw?.data === "string" ? raw.data : "";

    if (!to || to === username || !kind || !data) {
      return errorTo(socket, "message-error", "Medya a pa valab.");
    }

    if (!byName.has(to)) {
      return errorTo(socket, "message-error", "Kontak la pa online.");
    }

    if (Buffer.byteLength(data, "utf8") > MAX_MEDIA) {
      return errorTo(socket, "message-error", "Fichye a twò gwo.");
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
      if (!to || to === username) return;

      sendToUser(to, event, { from: username });
    });
  }

  // Relay WebRTC signals only to the intended online recipient.
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
          errorTo(socket, "call-error", "Kontak la pa online kounye a.");
        }
        return;
      }

      sendToUser(to, event, {
        ...raw,
        from: username,
        to
      });
    });
  }

  socket.on("group-call-create", raw => {
    if (rateLimited(socket)) return;

    const requested = Array.isArray(raw?.members)
      ? raw.members.map(v => clean(v, 80)).filter(Boolean)
      : [];

    const members = [...new Set([username, ...requested])].slice(0, 20);
    const roomId = randomUUID();

    groupRooms.set(roomId, {
      owner: username,
      members: new Set(members),
      sockets: new Set([socket.id])
    });

    socket.join(`group:${roomId}`);

    for (const member of members) {
      if (member !== username && byName.has(member)) {
        sendToUser(member, "group-call-invite", {
          roomId,
          from: username,
          members
        });
      }
    }

    socket.emit("group-call-created", { roomId, members });
  });

  socket.on("group-call-join", raw => {
    if (rateLimited(socket)) return;

    const roomId = clean(raw?.roomId, 100);
    const room = groupRooms.get(roomId);

    if (!room || !room.members.has(username)) {
      return errorTo(socket, "call-error", "Ou pa gen otorizasyon pou antre nan apèl sa a.");
    }

    room.sockets.add(socket.id);
    socket.join(`group:${roomId}`);

    socket.to(`group:${roomId}`).emit("group-peer-joined", {
      roomId,
      username
    });

    const peers = [];
    for (const id of room.sockets) {
      if (id === socket.id) continue;
      const peer = users.get(id);
      if (peer) peers.push(peer.username);
    }

    socket.emit("group-peers", { roomId, peers });
  });

  socket.on("group-signal", raw => {
    if (rateLimited(socket)) return;

    const roomId = clean(raw?.roomId, 100);
    const to = clean(raw?.to, 80);
    const room = groupRooms.get(roomId);
    const targetSocket = byName.get(to);

    if (
      !room ||
      !room.members.has(username) ||
      !room.members.has(to) ||
      !room.sockets.has(socket.id) ||
      !targetSocket ||
      !room.sockets.has(targetSocket)
    ) {
      return;
    }

    sendToUser(to, "group-signal", {
      ...raw,
      from: username,
      to,
      roomId
    });
  });

  socket.on("group-call-leave", raw => {
    const roomId = clean(raw?.roomId, 100);
    const room = groupRooms.get(roomId);
    if (!room) return;

    room.sockets.delete(socket.id);
    socket.leave(`group:${roomId}`);

    socket.to(`group:${roomId}`).emit("group-peer-left", {
      roomId,
      username
    });

    if (room.sockets.size === 0) {
      groupRooms.delete(roomId);
    }
  });

  socket.on("disconnect", () => {
    eventWindows.delete(socket.id);

    const ip = socket.data.ip;
    if (ip) {
      const count = Math.max(0, (ipConnections.get(ip) || 1) - 1);
      if (count) ipConnections.set(ip, count);
      else ipConnections.delete(ip);
    }

    for (const [roomId, room] of groupRooms) {
      if (room.sockets.has(socket.id)) {
        room.sockets.delete(socket.id);
        io.to(`group:${roomId}`).emit("group-peer-left", {
          roomId,
          username
        });
        if (room.sockets.size === 0) groupRooms.delete(roomId);
      }
    }

    if (byName.get(username) === socket.id) {
      byName.delete(username);
    }

    users.delete(socket.id);
    broadcastPresence();
  });
});

io.use((socket, next) => {
  const ip = socket.handshake.address || "unknown";
  const count = ipConnections.get(ip) || 0;

  if (count >= 40) {
    return next(new Error("Twòp koneksyon soti nan menm rezo a."));
  }

  ipConnections.set(ip, count + 1);
  socket.data.ip = ip;
  next();
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ZIQVONA running on port ${PORT}`);
});
