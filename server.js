
"use strict";

const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { Server } = require("socket.io");
const { createClient } = require("@supabase/supabase-js");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 10000);
const MAX_USERS = 10000;
const MAX_TEXT = 4000;
const MAX_MEDIA_BYTES = 8 * 1024 * 1024;
const MAX_EVENTS_PER_MINUTE = 120;
const MAX_HISTORY = 200;
const MAX_STATUS = 120;
const MAX_AVATAR = 2 * 1024 * 1024;

const SUPABASE_URL =
  process.env.SUPABASE_URL ||
  "https://hkirnyqousvmphpwqfga.supabase.co";

const SUPABASE_PUBLIC_KEY =
  process.env.SUPABASE_ANON_KEY ||
  process.env.SUPABASE_PUBLISHABLE_KEY ||
  process.env.SUPABASE_KEY;

const SUPABASE_SERVER_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SECRET_KEY;

if (!SUPABASE_URL || !SUPABASE_PUBLIC_KEY) {
  throw new Error("Render bezwen SUPABASE_URL ak SUPABASE_PUBLISHABLE_KEY.");
}

if (!SUPABASE_SERVER_KEY) {
  throw new Error(
    "Render bezwen SUPABASE_SERVICE_ROLE_KEY oswa SUPABASE_SECRET_KEY. " +
    "Pa janm mete kle sa a nan public/index.html."
  );
}

const authClient = createClient(SUPABASE_URL, SUPABASE_PUBLIC_KEY, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
});

const db = createClient(SUPABASE_URL, SUPABASE_SERVER_KEY, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
});

const io = new Server(server, {
  cors: {
    origin: true,
    credentials: false
  },
  maxHttpBufferSize: MAX_MEDIA_BYTES + 1024 * 1024,
  pingInterval: 25000,
  pingTimeout: 20000
});

app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

const onlineUsers = new Map();
const eventCounters = new Map();

function cleanString(value, max = 200) {
  return typeof value === "string"
    ? value.trim().slice(0, max)
    : "";
}

function safeError(error) {
  console.error("[ZIQVONA]", error?.message || error);
  return "Operasyon an pa t reyisi. Tanpri eseye ankò.";
}

function emitError(socket, message) {
  socket.emit("error-message", { message });
}

function profileUsername(profile, authUser) {
  return cleanString(
    profile?.username ||
    authUser?.user_metadata?.username ||
    authUser?.user_metadata?.user_name ||
    authUser?.phone ||
    authUser?.email ||
    authUser?.id,
    160
  );
}

function publicProfile(profile, authUser) {
  const username = profileUsername(profile, authUser);
  return {
    id: profile?.id || authUser?.id || null,
    username,
    display_name: cleanString(
      profile?.display_name ||
      authUser?.user_metadata?.display_name ||
      authUser?.user_metadata?.full_name ||
      username,
      80
    ),
    status: cleanString(
      profile?.status || authUser?.user_metadata?.status || "",
      MAX_STATUS
    ),
    avatar: profile?.avatar_url ||
      profile?.avatar ||
      authUser?.user_metadata?.avatar ||
      authUser?.user_metadata?.avatar_url ||
      ""
  };
}

async function getProfileById(userId) {
  const { data, error } = await db
    .from("profiles")
    .select("id,username,display_name,status,avatar_url")
    .eq("id", userId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function getProfileByUsername(username) {
  const { data, error } = await db
    .from("profiles")
    .select("id,username,display_name,status,avatar_url")
    .eq("username", username)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function getUserProfile(authUser) {
  const profile = await getProfileById(authUser.id);
  return publicProfile(profile, authUser);
}

function isOnline(username) {
  return onlineUsers.has(username);
}

function currentOnlineList() {
  return Array.from(onlineUsers.keys());
}

function allOnlineProfiles() {
  const result = {};
  for (const [username, entry] of onlineUsers.entries()) {
    result[username] = entry.profile;
  }
  return result;
}

function rateLimit(socket) {
  const now = Date.now();
  let item = eventCounters.get(socket.id);

  if (!item || now - item.startedAt >= 60000) {
    item = { startedAt: now, count: 0 };
    eventCounters.set(socket.id, item);
  }

  item.count += 1;
  return item.count <= MAX_EVENTS_PER_MINUTE;
}

function decodeMediaData(value) {
  if (typeof value !== "string") return null;

  const match = value.match(
    /^data:(image\/[a-zA-Z0-9.+-]+|video\/[a-zA-Z0-9.+-]+|audio\/[a-zA-Z0-9.+-]+);base64,([A-Za-z0-9+/=]+)$/
  );

  if (!match) return null;

  const mime = match[1].toLowerCase();
  const buffer = Buffer.from(match[2], "base64");

  if (!buffer.length || buffer.length > MAX_MEDIA_BYTES) return null;

  if (!["image", "video", "audio"].some(prefix => mime.startsWith(prefix))) {
    return null;
  }

  return { mime, buffer };
}

function mediaKindAllowed(kind, mime) {
  if (kind === "image") return mime.startsWith("image/");
  if (kind === "video") return mime.startsWith("video/");
  if (kind === "voice") return mime.startsWith("audio/");
  return false;
}

function parseMessageBody(body) {
  if (typeof body !== "string") return { kind: "text", text: "" };

  try {
    const parsed = JSON.parse(body);
    if (parsed && typeof parsed === "object") {
      return {
        kind: cleanString(parsed.kind || parsed.type || "text", 20),
        text: typeof parsed.text === "string" ? parsed.text : ""
      };
    }
  } catch (_) {
    // Ansyen mesaj yo ka genyen tèks senp.
  }

  return { kind: "text", text: body };
}

function toClientMessage(row, senderProfile, recipientProfile) {
  const body = parseMessageBody(row.body);
  const senderId = row.sender_id;

  const from = senderProfile?.username || senderId;
  const to = recipientProfile?.username || "";

  return {
    id: row.id,
    conversation_id: row.conversation_id,
    from,
    to,
    text: body.text,
    kind: body.kind,
    type: body.kind,
    timestamp: row.created_at || new Date().toISOString()
  };
}

/*
 * Sekirite DM:
 * - Konvèsasyon an dwe gen egzakteman de manm pou fonksyon DM sa a.
 * - Se manm konvèsasyon yo sèlman ki ka li istwa a.
 * - Pou kòmanse yon nouvo DM, toude itilizatè yo dwe nan contacts
 *   youn lòt nan baz done a.
 *
 * Sa pa montre konvèsasyon yon moun bay zanmi li yo.
 */

async function areMutualContacts(userA, userB) {
  const { data: a, error: errorA } = await db
    .from("contacts")
    .select("contact_id")
    .eq("owner_id", userA)
    .eq("contact_id", userB)
    .limit(1);

  if (errorA) throw errorA;

  const { data: b, error: errorB } = await db
    .from("contacts")
    .select("contact_id")
    .eq("owner_id", userB)
    .eq("contact_id", userA)
    .limit(1);

  if (errorB) throw errorB;

  return Boolean(a?.length && b?.length);
}

async function getDirectConversation(userA, userB) {
  const { data: membershipsA, error: errorA } = await db
    .from("conversation_members")
    .select("conversation_id")
    .eq("user_id", userA);

  if (errorA) throw errorA;

  const ids = (membershipsA || []).map(row => row.conversation_id);
  if (!ids.length) return null;

  const { data: membershipsB, error: errorB } = await db
    .from("conversation_members")
    .select("conversation_id")
    .eq("user_id", userB)
    .in("conversation_id", ids);

  if (errorB) throw errorB;

  const common = (membershipsB || []).map(row => row.conversation_id);
  if (!common.length) return null;

  const { data: conversations, error: convError } = await db
    .from("conversations")
    .select("id,type")
    .in("id", common);

  if (convError) throw convError;

  for (const conversation of conversations || []) {
    if (conversation.type && conversation.type !== "direct") continue;

    const { data: members, error: memberError } = await db
      .from("conversation_members")
      .select("user_id")
      .eq("conversation_id", conversation.id);

    if (memberError) throw memberError;

    const memberIds = new Set((members || []).map(row => row.user_id));
    if (
      memberIds.size === 2 &&
      memberIds.has(userA) &&
      memberIds.has(userB)
    ) {
      return conversation;
    }
  }

  return null;
}

async function getOrCreateDirectConversation(userA, userB) {
  const existing = await getDirectConversation(userA, userB);
  if (existing) return existing;

  if (!(await areMutualContacts(userA, userB))) {
    const error = new Error(
      "Nouvo konvèsasyon pa otorize: nou bezwen relasyon kontak mityèl."
    );
    error.code = "CONTACT_REQUIRED";
    throw error;
  }

  const { data: conversation, error: createError } = await db
    .from("conversations")
    .insert({ type: "direct" })
    .select("id,type")
    .single();

  if (createError) throw createError;

  const { error: memberError } = await db
    .from("conversation_members")
    .insert([
      { conversation_id: conversation.id, user_id: userA },
      { conversation_id: conversation.id, user_id: userB }
    ]);

  if (memberError) {
    // Pa kite yon konvèsasyon vid si enskripsyon manm yo echwe.
    await db.from("conversations").delete().eq("id", conversation.id);
    throw memberError;
  }

  return conversation;
}

async function getConversationMembers(conversationId) {
  const { data, error } = await db
    .from("conversation_members")
    .select("user_id")
    .eq("conversation_id", conversationId);

  if (error) throw error;
  return (data || []).map(row => row.user_id);
}

async function ensureConversationMember(conversationId, userId) {
  const members = await getConversationMembers(conversationId);
  return members.includes(userId);
}

async function loadConversationHistory(conversation, userA, userB) {
  const allowed = await ensureConversationMember(conversation.id, userA);
  if (!allowed) {
    const error = new Error("Ou pa manm konvèsasyon sa a.");
    error.code = "NOT_MEMBER";
    throw error;
  }

  const allowedB = await ensureConversationMember(conversation.id, userB);
  if (!allowedB) {
    const error = new Error("Manm konvèsasyon an pa valab.");
    error.code = "NOT_MEMBER";
    throw error;
  }

  const { data, error } = await db
    .from("messages")
    .select("id,conversation_id,sender_id,body,created_at")
    .eq("conversation_id", String(conversation.id))
    .order("created_at", { ascending: false })
    .limit(MAX_HISTORY);

  if (error) throw error;

  const [profileA, profileB] = await Promise.all([
    getProfileById(userA),
    getProfileById(userB)
  ]);

  const authA = onlineUsers.get(
    publicProfile(profileA, { id: userA }).username
  )?.authUser;
  const authB = onlineUsers.get(
    publicProfile(profileB, { id: userB }).username
  )?.authUser;

  const publicA = publicProfile(profileA, authA || { id: userA });
  const publicB = publicProfile(profileB, authB || { id: userB });

  return (data || []).reverse().map(row => {
    const senderIsA = row.sender_id === userA;
    return toClientMessage(
      row,
      senderIsA ? publicA : publicB,
      senderIsA ? publicB : publicA
    );
  });
}

async function sendPrivateMessage(socket, raw, kind = "text") {
  const toUsername = cleanString(raw?.to, 160);
  if (!toUsername) {
    emitError(socket, "Chwazi moun k ap resevwa mesaj la.");
    return;
  }

  if (toUsername === socket.userProfile.username) {
    emitError(socket, "Ou pa ka voye yon mesaj bay tèt ou.");
    return;
  }

  let text = typeof raw?.text === "string" ? raw.text : "";

  if (kind === "text") {
    text = text.trim();
    if (!text || text.length > MAX_TEXT) {
      emitError(socket, "Mesaj la vid oswa li depase limit 4000 karaktè.");
      return;
    }
  } else {
    const media = decodeMediaData(text);
    if (!media || !mediaKindAllowed(kind, media.mime)) {
      emitError(socket, "Fichye a pa valab oswa li twò gwo.");
      return;
    }
  }

  const recipient = await getProfileByUsername(toUsername);
  if (!recipient?.id) {
    emitError(socket, "Nou pa jwenn itilizatè sa a nan pwofil yo.");
    return;
  }

  if (recipient.id === socket.user.id) {
    emitError(socket, "Ou pa ka voye yon mesaj bay tèt ou.");
    return;
  }

  const conversation = await getOrCreateDirectConversation(
    socket.user.id,
    recipient.id
  );

  // Verifikasyon an fèt sou sèvè a anvan chak ekriti.
  const senderIsMember = await ensureConversationMember(
    conversation.id,
    socket.user.id
  );
  const recipientIsMember = await ensureConversationMember(
    conversation.id,
    recipient.id
  );

  if (!senderIsMember || !recipientIsMember) {
    emitError(socket, "Ou pa otorize voye mesaj nan konvèsasyon sa a.");
    return;
  }

  const messageId = crypto.randomUUID();
  const body = JSON.stringify({ kind, text });

  const { data: row, error } = await db
    .from("messages")
    .insert({
      id: messageId,
      conversation_id: String(conversation.id),
      sender_id: socket.user.id,
      body
    })
    .select("id,conversation_id,sender_id,body,created_at")
    .single();

  if (error) throw error;

  const recipientProfile = publicProfile(recipient);
  const message = toClientMessage(row, socket.userProfile, recipientProfile);

  // Tout aparèy ki konekte sou kont sa yo resevwa sèlman DM pa yo.
  io.to(`user:${socket.user.id}`).emit("private-message", message);
  io.to(`user:${recipient.id}`).emit(
    kind === "text" ? "private-message" : "media-message",
    message
  );
}

async function sendHistory(socket, raw) {
  const otherUsername = cleanString(raw?.with, 160);
  if (!otherUsername || otherUsername === socket.userProfile.username) {
    emitError(socket, "Chwazi yon lòt itilizatè pou istwa konvèsasyon an.");
    return;
  }

  const other = await getProfileByUsername(otherUsername);
  if (!other?.id) {
    emitError(socket, "Nou pa jwenn pwofil sa a.");
    return;
  }

  const conversation = await getDirectConversation(socket.user.id, other.id);
  if (!conversation) {
    // Pa kreye yon konvèsasyon jis paske yon moun mande istwa a.
    socket.emit("history", { with: otherUsername, messages: [] });
    return;
  }

  const messages = await loadConversationHistory(
    conversation,
    socket.user.id,
    other.id
  );

  socket.emit("history", {
    with: otherUsername,
    messages
  });
}

async function searchUsers(socket, raw) {
  const query = cleanString(raw?.query || raw?.q, 80);

  if (!query) {
    socket.emit("search-results", []);
    return;
  }

  const { data, error } = await db
    .from("profiles")
    .select("id,username,display_name,status,avatar_url")
    .or(`username.ilike.%${query.replace(/[%_,().]/g, "")}%,display_name.ilike.%${query.replace(/[%_,().]/g, "")}%`)
    .neq("id", socket.user.id)
    .limit(30);

  if (error) throw error;

  socket.emit(
    "search-results",
    (data || []).map(profile => publicProfile(profile))
  );
}

async function updateProfile(socket, raw) {
  const display_name = cleanString(raw?.display_name, 80);
  const status = cleanString(raw?.status, MAX_STATUS);
  const avatar = typeof raw?.avatar === "string" ? raw.avatar : "";

  if (!display_name) {
    emitError(socket, "Non pwofil la pa dwe vid.");
    return;
  }

  if (avatar) {
    if (avatar.length > MAX_AVATAR * 1.4) {
      emitError(socket, "Foto pwofil la twò gwo.");
      return;
    }
    if (!/^data:image\/[a-zA-Z0-9.+-]+;base64,/.test(avatar)) {
      emitError(socket, "Fòma foto pwofil la pa valab.");
      return;
    }
  }

  const updates = {
    display_name,
    status,
    avatar_url: avatar || null
  };

  const { data, error } = await db
    .from("profiles")
    .update(updates)
    .eq("id", socket.user.id)
    .select("id,username,display_name,status,avatar_url")
    .maybeSingle();

  if (error) throw error;

  if (!data) {
    emitError(
      socket,
      "Pwofil ou pa egziste nan tablo profiles la. Kreye pwofil la anvan."
    );
    return;
  }

  socket.userProfile = publicProfile(data, socket.user);
  const entry = onlineUsers.get(socket.userProfile.username);

  if (entry) entry.profile = socket.userProfile;

  io.emit("profile-updated", socket.userProfile);
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    timestamp: new Date().toISOString(),
    online: onlineUsers.size
  });
});

app.get("/config", (_req, res) => {
  res.json({
    app: "ZIQVONA",
    auth: "supabase",
    realtime: true
  });
});

app.use(express.static(path.join(__dirname, "public")));

app.get("*", (req, res, next) => {
  if (req.path.startsWith("/socket.io/")) return next();
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth?.accessToken;

    if (!token || typeof token !== "string") {
      return next(new Error("Token koneksyon an manke."));
    }

    const { data, error } = await authClient.auth.getUser(token);
    if (error || !data?.user) {
      return next(new Error("Sesyon an pa valab. Konekte ankò."));
    }

    socket.user = data.user;
    socket.userProfile = await getUserProfile(data.user);

    if (!socket.userProfile.username) {
      return next(new Error("Kont lan pa gen username valab."));
    }

    next();
  } catch (error) {
    console.error("Socket authentication failed:", error?.message || error);
    next(new Error("Pa t kapab verifye kont lan."));
  }
});

io.on("connection", socket => {
  const userId = socket.user.id;
  const username = socket.userProfile.username;

  if (onlineUsers.size >= MAX_USERS && !onlineUsers.has(username)) {
    socket.emit("force-disconnect", { message: "Sèvè a rive nan kapasite li." });
    socket.disconnect(true);
    return;
  }

  socket.join(`user:${userId}`);

  const entry = onlineUsers.get(username) || {
    userId,
    profile: socket.userProfile,
    authUser: socket.user,
    sockets: new Set()
  };

  entry.sockets.add(socket.id);
  entry.profile = socket.userProfile;
  entry.authUser = socket.user;
  onlineUsers.set(username, entry);

  socket.emit("registered", {
    me: socket.userProfile,
    profiles: allOnlineProfiles(),
    online: currentOnlineList()
  });

  io.emit("presence", { online: currentOnlineList() });

  socket.on("register", () => {
    socket.emit("registered", {
      me: socket.userProfile,
      profiles: allOnlineProfiles(),
      online: currentOnlineList()
    });
  });

  socket.on("search-users", raw => {
    if (!rateLimit(socket)) {
      emitError(socket, "Ou ap voye twòp demann. Tann yon ti moman.");
      return;
    }

    searchUsers(socket, raw).catch(error => {
      safeError(error);
      emitError(socket, "Rechèch la pa t reyisi.");
    });
  });

  socket.on("get-history", raw => {
    if (!rateLimit(socket)) {
      emitError(socket, "Ou ap voye twòp demann.");
      return;
    }

    sendHistory(socket, raw).catch(error => {
      safeError(error);
      emitError(
        socket,
        error.code === "CONTACT_REQUIRED"
          ? "Ou pa gen pèmisyon pou konvèsasyon sa a."
          : "Nou pa t kapab chaje istwa mesaj yo."
      );
    });
  });

  socket.on("private-message", raw => {
    if (!rateLimit(socket)) {
      emitError(socket, "Ou ap voye twòp mesaj. Tann yon ti moman.");
      return;
    }

    sendPrivateMessage(socket, raw, "text").catch(error => {
      safeError(error);
      emitError(
        socket,
        error.code === "CONTACT_REQUIRED"
          ? "Pou kòmanse nouvo konvèsasyon, toude moun yo dwe nan kontak youn lòt."
          : "Mesaj la pa t kapab voye."
      );
    });
  });

  socket.on("media-message", raw => {
    if (!rateLimit(socket)) {
      emitError(socket, "Ou ap voye twòp fichye.");
      return;
    }

    const kind = cleanString(raw?.kind || raw?.type, 20);
    if (!["image", "video", "voice"].includes(kind)) {
      emitError(socket, "Kalite medya sa a pa sipòte.");
      return;
    }

    sendPrivateMessage(socket, raw, kind).catch(error => {
      safeError(error);
      emitError(
        socket,
        error.code === "CONTACT_REQUIRED"
          ? "Ou pa gen pèmisyon pou voye medya nan konvèsasyon sa a."
          : "Medya a pa t kapab voye."
      );
    });
  });

  socket.on("update-profile", raw => {
    if (!rateLimit(socket)) {
      emitError(socket, "Ou ap voye twòp demann.");
      return;
    }

    updateProfile(socket, raw).catch(error => {
      safeError(error);
      emitError(socket, "Pwofil la pa t kapab sove.");
    });
  });

  socket.on("typing", raw => {
    if (!rateLimit(socket)) return;

    const toUsername = cleanString(raw?.to, 160);
    const recipient = onlineUsers.get(toUsername);
    if (!recipient || recipient.userId === userId) return;

    // Pa relè typing si pa gen DM ki otorize deja.
    getDirectConversation(userId, recipient.userId)
      .then(conversation => {
        if (!conversation) return;

        io.to(`user:${recipient.userId}`).emit("typing", {
          from: username
        });
      })
      .catch(error => safeError(error));
  });

  /*
   * Relè siyal WebRTC yo sèlman lè gen yon konvèsasyon DM otorize.
   * Tout siyal yo pase nan menm verifikasyon manm yo.
   */
  const callEvents = [
    "call-offer",
    "call-answer",
    "call-ice-candidate",
    "call-end"
  ];

  for (const eventName of callEvents) {
    socket.on(eventName, raw => {
      if (!rateLimit(socket)) {
        emitError(socket, "Twòp aktivite apèl.");
        return;
      }

      const toUsername = cleanString(raw?.to, 160);
      const recipient = onlineUsers.get(toUsername);

      if (!recipient || recipient.userId === userId) {
        if (eventName === "call-offer") {
          socket.emit("call-error", { message: "Moun nan pa konekte." });
        }
        return;
      }

      getDirectConversation(userId, recipient.userId)
        .then(conversation => {
          if (!conversation) {
            socket.emit("call-error", {
              message: "Pa gen konvèsasyon otorize ant kont sa yo."
            });
            return;
          }

          const payload = {
            ...raw,
            from: username,
            to: toUsername
          };

          io.to(`user:${recipient.userId}`).emit(eventName, payload);
        })
        .catch(error => {
          safeError(error);
          socket.emit("call-error", { message: "Apèl la pa t kapab konekte." });
        });
    });
  }

  socket.on("disconnect", () => {
    eventCounters.delete(socket.id);

    const current = onlineUsers.get(username);
    if (current) {
      current.sockets.delete(socket.id);

      if (current.sockets.size === 0) {
        onlineUsers.delete(username);
      }
    }

    io.emit("presence", { online: currentOnlineList() });
  });
});

server.listen(PORT, () => {
  console.log(`ZIQVONA ap koute sou pò ${PORT}`);
});
