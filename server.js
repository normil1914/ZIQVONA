const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { Server } = require("socket.io");
const { createClient } = require("@supabase/supabase-js");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 10000;

/* =========================================================
   ENVIRONMENT
========================================================= */

const SUPABASE_URL =
  process.env.SUPABASE_URL ||
  "https://hkirnyqousvmphpwqfga.supabase.co";

const SUPABASE_PUBLISHABLE_KEY =
  process.env.SUPABASE_PUBLISHABLE_KEY ||
  "";

const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  "";

/*
  The publishable key is safe for public applications.

  The service-role key MUST ONLY exist in Render Environment
  Variables. Never put it in public/index.html or GitHub.
*/

if (!SUPABASE_PUBLISHABLE_KEY) {
  console.warn("WARNING: SUPABASE_PUBLISHABLE_KEY is not configured.");
}

if (!SUPABASE_SERVICE_ROLE_KEY) {
  console.warn(
    "WARNING: SUPABASE_SERVICE_ROLE_KEY is not configured. " +
    "Server will use the authenticated user's token for database operations."
  );
}

/* =========================================================
   SUPABASE CLIENTS
========================================================= */

const supabaseAdmin = SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: {
        autoRefreshToken: false,
        persistSession: false
      }
    })
  : null;

const supabasePublic = SUPABASE_PUBLISHABLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY, {
      auth: {
        autoRefreshToken: false,
        persistSession: false
      }
    })
  : null;

/* =========================================================
   SOCKET.IO
========================================================= */

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"],
  maxHttpBufferSize: 20 * 1024 * 1024
});

app.use(express.json({ limit: "1mb" }));

/* =========================================================
   LIMITS
========================================================= */

const MAX_USERS = 1200;
const MAX_USERNAME = 32;
const MAX_TEXT = 4000;
const MAX_HISTORY = 5000;
const MAX_STATUS = 120;
const MAX_IMAGE_BYTES = 6 * 1024 * 1024;
const MAX_AVATAR_BYTES = 3 * 1024 * 1024;

/* =========================================================
   RUNTIME STATE
========================================================= */

/*
  These Maps are ONLY for live Socket.IO/WebRTC state.

  Permanent users, profiles, contacts, conversations and
  messages are stored in Supabase PostgreSQL.
*/

const liveUsers = new Map();
/*
  userId -> {
    userId,
    username,
    socketId,
    online,
    connectedAt
  }
*/

const liveCalls = new Map();

/* =========================================================
   HELPERS
========================================================= */

function clean(value, max = 255) {
  return String(value || "")
    .trim()
    .slice(0, max);
}

function normalizeUsername(value) {
  return clean(value, MAX_USERNAME)
    .toLowerCase()
    .replace(/\s+/g, "");
}

function cleanDisplayName(value) {
  return clean(value, MAX_USERNAME)
    .replace(/\s+/g, " ");
}

function validUsername(username) {
  return /^[a-zA-Z0-9_.-]{2,32}$/.test(username);
}

function validUUID(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(value || "")
  );
}

function getLiveUser(userId) {
  return liveUsers.get(String(userId)) || null;
}

function getSocketForUser(userId) {
  const live = getLiveUser(userId);

  if (!live || !live.socketId) {
    return null;
  }

  return io.sockets.sockets.get(live.socketId) || null;
}

function emitToUser(userId, event, payload) {
  const socket = getSocketForUser(userId);

  if (socket) {
    socket.emit(event, payload);
  }
}

function publicProfile(profile, online = false) {
  if (!profile) return null;

  return {
    id: profile.id,
    username: profile.username,
    displayName:
      profile.display_name ||
      profile.username ||
      "ZIQVONA User",
    status: profile.status || "",
    avatar: profile.avatar_url || "",
    online: !!online
  };
}

function publicUserFromProfile(profile) {
  const live = getLiveUser(profile.id);

  return publicProfile(profile, !!live?.online);
}

function conversationRoom(conversationId) {
  return `conversation:${conversationId}`;
}

function callParticipants(call) {
  if (!call) return [];

  return [call.callerId, call.calleeId].filter(Boolean);
}

function otherCallParticipant(call, userId) {
  if (!call) return null;

  return call.callerId === userId
    ? call.calleeId
    : call.callerId;
}

/* =========================================================
   SUPABASE DATABASE HELPERS
========================================================= */

function getDbClient(accessToken = null) {
  if (supabaseAdmin) {
    return supabaseAdmin;
  }

  if (!supabasePublic) {
    throw new Error("Supabase client is not configured.");
  }

  if (!accessToken) {
    return supabasePublic;
  }

  return createClient(
    SUPABASE_URL,
    SUPABASE_PUBLISHABLE_KEY,
    {
      global: {
        headers: {
          Authorization: `Bearer ${accessToken}`
        }
      },
      auth: {
        autoRefreshToken: false,
        persistSession: false
      }
    }
  );
}

async function getProfileById(userId, accessToken = null) {
  const db = getDbClient(accessToken);

  const { data, error } = await db
    .from("profiles")
    .select(
      "id, username, display_name, status, avatar_url, created_at, updated_at"
    )
    .eq("id", userId)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

async function getProfileByUsername(username, accessToken = null) {
  const db = getDbClient(accessToken);

  const { data, error } = await db
    .from("profiles")
    .select(
      "id, username, display_name, status, avatar_url, created_at, updated_at"
    )
    .eq("username", normalizeUsername(username))
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

async function createProfileIfMissing(
  userId,
  username,
  displayName,
  accessToken = null
) {
  let profile = await getProfileById(userId, accessToken);

  if (profile) {
    return profile;
  }

  const db = getDbClient(accessToken);

  const normalized = normalizeUsername(username);

  if (!validUsername(normalized)) {
    throw new Error(
      "Non itilizatè a dwe gen 2-32 karaktè: lèt, chif, _, -, oswa ."
    );
  }

  const { data, error } = await db
    .from("profiles")
    .insert({
      id: userId,
      username: normalized,
      display_name:
        cleanDisplayName(displayName) ||
        normalized,
      status: "",
      avatar_url: ""
    })
    .select(
      "id, username, display_name, status, avatar_url, created_at, updated_at"
    )
    .single();

  if (error) {
    /*
      If username already exists, return the existing profile.
    */
    if (error.code === "23505") {
      const existing = await getProfileByUsername(
        normalized,
        accessToken
      );

      if (existing && existing.id !== userId) {
        throw new Error(
          "Non itilizatè sa deja egziste."
        );
      }

      return existing;
    }

    throw error;
  }

  return data;
}

async function listProfiles(accessToken = null) {
  const db = getDbClient(accessToken);

  const { data, error } = await db
    .from("profiles")
    .select(
      "id, username, display_name, status, avatar_url, created_at, updated_at"
    )
    .order("display_name", {
      ascending: true
    })
    .limit(MAX_USERS);

  if (error) {
    throw error;
  }

  return (data || []).map(publicUserFromProfile);
}

async function listContacts(userId, accessToken = null) {
  const db = getDbClient(accessToken);

  const { data, error } = await db
    .from("contacts")
    .select(
      "owner_id, contact_id, created_at"
    )
    .eq("owner_id", userId);

  if (error) {
    throw error;
  }

  const ids = (data || []).map(row => row.contact_id);

  if (!ids.length) {
    return [];
  }

  const { data: profiles, error: profileError } = await db
    .from("profiles")
    .select(
      "id, username, display_name, status, avatar_url, created_at, updated_at"
    )
    .in("id", ids);

  if (profileError) {
    throw profileError;
  }

  return (profiles || [])
    .map(publicUserFromProfile)
    .sort((a, b) => {
      if (a.online !== b.online) {
        return a.online ? -1 : 1;
      }

      return a.displayName.localeCompare(
        b.displayName
      );
    });
}

async function addContact(
  ownerId,
  contactId,
  accessToken = null
) {
  if (!ownerId || !contactId || ownerId === contactId) {
    return;
  }

  const db = getDbClient(accessToken);

  const { error } = await db
    .from("contacts")
    .upsert(
      {
        owner_id: ownerId,
        contact_id: contactId
      },
      {
        onConflict: "owner_id,contact_id",
        ignoreDuplicates: true
      }
    );

  if (error) {
    throw error;
  }
}

async function removeContact(
  ownerId,
  contactId,
  accessToken = null
) {
  const db = getDbClient(accessToken);

  const { error } = await db
    .from("contacts")
    .delete()
    .eq("owner_id", ownerId)
    .eq("contact_id", contactId);

  if (error) {
    throw error;
  }
}

async function getConversationBetween(
  userA,
  userB,
  accessToken = null
) {
  const db = getDbClient(accessToken);

  const { data: memberships, error: memberError } = await db
    .from("conversation_members")
    .select("conversation_id, user_id")
    .in("user_id", [userA, userB]);

  if (memberError) {
    throw memberError;
  }

  if (!memberships || !memberships.length) {
    return null;
  }

  const counts = new Map();

  for (const row of memberships) {
    const id = String(row.conversation_id);

    if (!counts.has(id)) {
      counts.set(id, new Set());
    }

    counts.get(id).add(String(row.user_id));
  }

  for (const [conversationId, memberSet] of counts.entries()) {
    if (
      memberSet.has(String(userA)) &&
      memberSet.has(String(userB))
    ) {
      return conversationId;
    }
  }

  return null;
}

async function createDirectConversation(
  userA,
  userB,
  accessToken = null
) {
  const existing = await getConversationBetween(
    userA,
    userB,
    accessToken
  );

  if (existing) {
    return existing;
  }

  const db = getDbClient(accessToken);

  const { data: conversation, error: conversationError } =
    await db
      .from("conversations")
      .insert({
        type: "direct"
      })
      .select("id, type, created_at")
      .single();

  if (conversationError) {
    throw conversationError;
  }

  const conversationId = conversation.id;

  const { error: membersError } = await db
    .from("conversation_members")
    .insert([
      {
        conversation_id: conversationId,
        user_id: userA
      },
      {
        conversation_id: conversationId,
        user_id: userB
      }
    ]);

  if (membersError) {
    /*
      If the members cannot be inserted, remove the conversation
      only when service role is available.
    */
    if (supabaseAdmin) {
      await supabaseAdmin
        .from("conversations")
        .delete()
        .eq("id", conversationId);
    }

    throw membersError;
  }

  return conversationId;
}

async function getMessages(
  conversationId,
  accessToken = null
) {
  const db = getDbClient(accessToken);

  const { data, error } = await db
    .from("messages")
    .select(
      "id, conversation_id, sender_id, body, created_at"
    )
    .eq(
      "conversation_id",
      String(conversationId)
    )
    .order("created_at", {
      ascending: true
    })
    .limit(MAX_HISTORY);

  if (error) {
    throw error;
  }

  return (data || []).map(row => ({
    id: row.id,
    conversationId: row.conversation_id,
    senderId: row.sender_id,
    body: row.body || "",
    text: row.body || "",
    type: "text",
    createdAt: row.created_at
  }));
}

async function insertMessage(
  conversationId,
  senderId,
  body,
  accessToken = null
) {
  const db = getDbClient(accessToken);

  const { data, error } = await db
    .from("messages")
    .insert({
      conversation_id: String(conversationId),
      sender_id: senderId,
      body
    })
    .select(
      "id, conversation_id, sender_id, body, created_at"
    )
    .single();

  if (error) {
    throw error;
  }

  return {
    id: data.id,
    conversationId: data.conversation_id,
    senderId: data.sender_id,
    body: data.body || "",
    text: data.body || "",
    type: "text",
    createdAt: data.created_at
  };
}

/* =========================================================
   AUTH
========================================================= */

async function verifyAccessToken(accessToken) {
  if (!accessToken) {
    throw new Error("Missing Supabase access token.");
  }

  /*
    getUser(token) validates the token against Supabase Auth.
  */
  if (!supabasePublic) {
    throw new Error("Supabase publishable key is missing.");
  }

  const { data, error } =
    await supabasePublic.auth.getUser(
      accessToken
    );

  if (error || !data?.user) {
    throw new Error(
      "Sesyon Supabase la pa valid oswa li ekspire."
    );
  }

  return data.user;
}

async function authenticateSocket(socket) {
  const accessToken =
    socket.handshake.auth?.accessToken ||
    socket.handshake.auth?.token ||
    null;

  if (!accessToken) {
    throw new Error(
      "ZIQVONA bezwen yon sesyon Supabase pou konekte."
    );
  }

  const authUser = await verifyAccessToken(
    accessToken
  );

  return {
    authUser,
    accessToken
  };
}

/* =========================================================
   ICE / TURN
========================================================= */

function buildIceServers() {
  const servers = [
    {
      urls: [
        "stun:stun.l.google.com:19302",
        "stun:stun1.l.google.com:19302"
      ]
    }
  ];

  const turnUsername =
    process.env.TURN_USERNAME;

  const turnPassword =
    process.env.TURN_PASSWORD;

  const turnHost =
    process.env.TURN_HOST ||
    "global.relay.metered.ca";

  if (turnUsername && turnPassword) {
    servers.push(
      {
        urls: `turn:${turnHost}:80`,
        username: turnUsername,
        credential: turnPassword
      },
      {
        urls: `turn:${turnHost}:80?transport=tcp`,
        username: turnUsername,
        credential: turnPassword
      },
      {
        urls: `turn:${turnHost}:443`,
        username: turnUsername,
        credential: turnPassword
      },
      {
        urls: `turns:${turnHost}:443?transport=tcp`,
        username: turnUsername,
        credential: turnPassword
      }
    );
  }

  return servers;
}

/* =========================================================
   HTTP
========================================================= */

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    version: "3.0.0",
    database: !!supabasePublic,
    serviceRoleConfigured: !!supabaseAdmin,
    online: Array.from(liveUsers.values())
      .filter(user => user.online)
      .length,
    turnConfigured: !!(
      process.env.TURN_USERNAME &&
      process.env.TURN_PASSWORD
    )
  });
});

app.get("/api/turn", (req, res) => {
  res.json({
    iceServers: buildIceServers(),
    turnConfigured: !!(
      process.env.TURN_USERNAME &&
      process.env.TURN_PASSWORD
    )
  });
});

app.get("/api/config", (req, res) => {
  res.json({
    app: "ZIQVONA",
    supabaseUrl: SUPABASE_URL,
    supabaseConfigured: !!(
      SUPABASE_URL &&
      SUPABASE_PUBLISHABLE_KEY
    )
  });
});

app.use(
  express.static(
    path.join(__dirname, "public")
  )
);

app.get("*", (req, res) => {
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});

/* =========================================================
   SOCKET.IO
========================================================= */

io.on("connection", socket => {
  let userId = null;
  let accessToken = null;

  /*
    Authentication is done explicitly after connection so
    the existing frontend can transition without breaking
    the current Socket.IO/WebRTC architecture.
  */

  socket.on("auth:login", async payload => {
    try {
      if (userId) {
        socket.emit("auth:error", {
          message: "Sesyon sa deja konekte."
        });
        return;
      }

      const token =
        payload?.accessToken ||
        payload?.token ||
        null;

      if (!token) {
        socket.emit("auth:error", {
          message:
            "Token Supabase la manke."
        });
        return;
      }

      const authUser =
        await verifyAccessToken(token);

      userId = authUser.id;
      accessToken = token;

      let profile =
        await getProfileById(
          userId,
          accessToken
        );

      /*
        New Auth account may not have a profile yet.
        The frontend can send username/displayName
        immediately after registration.
      */
      if (!profile && payload?.username) {
        profile =
          await createProfileIfMissing(
            userId,
            payload.username,
            payload.displayName ||
              payload.username,
            accessToken
          );
      }

      if (!profile) {
        socket.emit("auth:profile-required", {
          userId
        });
        return;
      }

      const oldLive =
        liveUsers.get(userId);

      if (
        oldLive?.socketId &&
        oldLive.socketId !== socket.id
      ) {
        const oldSocket =
          io.sockets.sockets.get(
            oldLive.socketId
          );

        if (oldSocket) {
          oldSocket.emit(
            "session:replaced"
          );

          oldSocket.disconnect(true);
        }
      }

      liveUsers.set(userId, {
        userId,
        username: profile.username,
        socketId: socket.id,
        online: true,
        connectedAt: Date.now()
      });

      socket.data.userId = userId;
      socket.data.username =
        profile.username;

      const users = await listProfiles(
        accessToken
      );

      const userContacts =
        await listContacts(
          userId,
          accessToken
        );

      socket.emit("login:success", {
        user:
          publicUserFromProfile(
            profile
          ),
        profile,
        users,
        contacts: userContacts,
        auth: {
          userId,
          email:
            authUser.email || null,
          phone:
            authUser.phone || null
        }
      });

      io.emit("users:update", {
        users
      });
    } catch (error) {
      console.error(
        "auth:login error:",
        error
      );

      socket.emit("auth:error", {
        message:
          error?.message ||
          "Koneksyon Supabase la echwe."
      });
    }
  });

  /* =======================================================
     CREATE PROFILE
  ======================================================= */

  socket.on(
    "profile:create",
    async data => {
      try {
        if (!userId || !accessToken) {
          socket.emit("profile:error", {
            message:
              "Ou poko konekte."
          });
          return;
        }

        const username =
          normalizeUsername(
            data?.username
          );

        const displayName =
          cleanDisplayName(
            data?.displayName
          ) || username;

        if (!validUsername(username)) {
          socket.emit("profile:error", {
            message:
              "Username la dwe gen 2-32 karaktè: lèt, chif, _, -, oswa ."
          });
          return;
        }

        const existing =
          await getProfileByUsername(
            username,
            accessToken
          );

        if (
          existing &&
          existing.id !== userId
        ) {
          socket.emit("profile:error", {
            message:
              "Username sa deja itilize."
          });
          return;
        }

        const db =
          getDbClient(
            accessToken
          );

        const { data: profile, error } =
          await db
            .from("profiles")
            .upsert(
              {
                id: userId,
                username,
                display_name:
                  displayName,
                status: clean(
                  data?.status,
                  MAX_STATUS
                ),
                avatar_url:
                  typeof data?.avatar ===
                  "string"
                    ? data.avatar
                    : ""
              },
              {
                onConflict: "id"
              }
            )
            .select(
              "id, username, display_name, status, avatar_url, created_at, updated_at"
            )
            .single();

        if (error) {
          throw error;
        }

        const live =
          liveUsers.get(userId);

        if (live) {
          live.username =
            profile.username;

          liveUsers.set(
            userId,
            live
          );
        }

        socket.emit(
          "profile:updated",
          {
            profile
          }
        );

        const users =
          await listProfiles(
            accessToken
          );

        io.emit(
          "users:update",
          {
            users
          }
        );
      } catch (error) {
        console.error(
          "profile:create error:",
          error
        );

        socket.emit(
          "profile:error",
          {
            message:
              error?.message ||
              "Profile la pa ka kreye."
          }
        );
      }
    }
  );

  /* =======================================================
     PROFILE UPDATE
  ======================================================= */

  socket.on(
    "profile:update",
    async data => {
      try {
        if (!userId || !accessToken) {
          return;
        }

        const db =
          getDbClient(
            accessToken
          );

        const current =
          await getProfileById(
            userId,
            accessToken
          );

        if (!current) {
          socket.emit(
            "profile:error",
            {
              message:
                "Profile la poko egziste."
            }
          );
          return;
        }

        const displayName =
          cleanDisplayName(
            data?.displayName
          ) ||
          current.display_name ||
          current.username;

        const status =
          clean(
            data?.status,
            MAX_STATUS
          );

        let avatar =
          current.avatar_url ||
          "";

        if (
          typeof data?.avatar ===
          "string"
        ) {
          if (
            data.avatar === "" ||
            /^data:image\/(png|jpeg|jpg|webp|gif);base64,/i.test(
              data.avatar
            )
          ) {
            if (
              Buffer.byteLength(
                data.avatar,
                "utf8"
              ) <=
              MAX_AVATAR_BYTES
            ) {
              avatar =
                data.avatar;
            }
          }
        }

        const { data: profile, error } =
          await db
            .from("profiles")
            .update({
              display_name:
                displayName,
              status,
              avatar_url:
                avatar,
              updated_at:
                new Date().toISOString()
            })
            .eq("id", userId)
            .select(
              "id, username, display_name, status, avatar_url, created_at, updated_at"
            )
            .single();

        if (error) {
          throw error;
        }

        socket.emit(
          "profile:updated",
          {
            profile
          }
        );

        const users =
          await listProfiles(
            accessToken
          );

        io.emit(
          "users:update",
          {
            users
          }
        );
      } catch (error) {
        console.error(
          "profile:update error:",
          error
        );

        socket.emit(
          "profile:error",
          {
            message:
              error?.message ||
              "Profile la pa ka sove."
          }
        );
      }
    }
  );

  /* =======================================================
     CONTACT ADD
  ======================================================= */

  socket.on(
    "contact:add",
    async data => {
      try {
        if (!userId || !accessToken) {
          return;
        }

        const targetUsername =
          normalizeUsername(
            data?.username
          );

        if (!targetUsername) {
          return;
        }

        const target =
          await getProfileByUsername(
            targetUsername,
            accessToken
          );

        if (
          !target ||
          target.id === userId
        ) {
          socket.emit(
            "contact:error",
            {
              message:
                "Moun sa pa disponib."
            }
          );
          return;
        }

        await addContact(
          userId,
          target.id,
          accessToken
        );

        const userContacts =
          await listContacts(
            userId,
            accessToken
          );

        socket.emit(
          "contacts:update",
          {
            contacts:
              userContacts
          }
        );

        socket.emit(
          "contact:added",
          {
            username:
              target.username,
            user:
              publicUserFromProfile(
                target
              )
          }
        );
      } catch (error) {
        console.error(
          "contact:add error:",
          error
        );

        socket.emit(
          "contact:error",
          {
            message:
              error?.message ||
              "Kontak la pa ka ajoute."
          }
        );
      }
    }
  );

  /* =======================================================
     CONTACT REMOVE
  ======================================================= */

  socket.on(
    "contact:remove",
    async data => {
      try {
        if (!userId || !accessToken) {
          return;
        }

        const targetUsername =
          normalizeUsername(
            data?.username
          );

        const target =
          await getProfileByUsername(
            targetUsername,
            accessToken
          );

        if (!target) {
          return;
        }

        await removeContact(
          userId,
          target.id,
          accessToken
        );

        const userContacts =
          await listContacts(
            userId,
            accessToken
          );

        socket.emit(
          "contacts:update",
          {
            contacts:
              userContacts
          }
        );

        socket.emit(
          "contact:removed",
          {
            username:
              target.username
          }
        );
      } catch (error) {
        console.error(
          "contact:remove error:",
          error
        );

        socket.emit(
          "contact:error",
          {
            message:
              error?.message ||
              "Kontak la pa ka retire."
          }
        );
      }
    }
  );

  /* =======================================================
     CHAT JOIN
  ======================================================= */

  socket.on(
    "chat:join",
    async data => {
      try {
        if (!userId || !accessToken) {
          socket.emit(
            "chat:error",
            {
              message:
                "Ou poko konekte."
            }
          );
          return;
        }

        const targetUsername =
          normalizeUsername(
            data?.username
          );

        const target =
          await getProfileByUsername(
            targetUsername,
            accessToken
          );

        if (
          !target ||
          target.id === userId
        ) {
          socket.emit(
            "chat:error",
            {
              message:
                "Moun sa pa egziste."
            }
          );
          return;
        }

        const conversationId =
          await createDirectConversation(
            userId,
            target.id,
            accessToken
          );

        socket.join(
          conversationRoom(
            conversationId
          )
        );

        /*
          Add the other person to contacts for
          the current user.
        */
        await addContact(
          userId,
          target.id,
          accessToken
        );

        const messages =
          await getMessages(
            conversationId,
            accessToken
          );

        socket.emit(
          "chat:history",
          {
            with:
              target.username,
            withUser:
              publicUserFromProfile(
                target
              ),
            conversationId,
            messages
          }
        );

        const userContacts =
          await listContacts(
            userId,
            accessToken
          );

        socket.emit(
          "contacts:update",
          {
            contacts:
              userContacts
          }
        );
      } catch (error) {
        console.error(
          "chat:join error:",
          error
        );

        socket.emit(
          "chat:error",
          {
            message:
              error?.message ||
              "Chat la pa ka louvri."
          }
        );
      }
    }
  );

  /* =======================================================
     MESSAGE SEND
  ======================================================= */

  socket.on(
    "message:send",
    async data => {
      try {
        if (!userId || !accessToken) {
          return;
        }

        const targetUsername =
          normalizeUsername(
            data?.to
          );

        const target =
          await getProfileByUsername(
            targetUsername,
            accessToken
          );

        if (
          !target ||
          target.id === userId
        ) {
          socket.emit(
            "message:error",
            {
              message:
                "Destinatè a pa disponib."
            }
          );
          return;
        }

        /*
          The database structure has:
          messages.body
          messages.message_type

          Text is stored in body.
        */

        const type =
          data?.type === "image"
            ? "image"
            : "text";

        if (type === "image") {
          /*
            The current database has no image_data
            column. The future Storage implementation
            should upload the image and put its URL
            into media_url.

            Therefore this server returns a clear
            message instead of silently losing the photo.
          */

          socket.emit(
            "message:error",
            {
              message:
                "Photo chat la bezwen Supabase Storage. Nou pral branche Storage apre Auth."
            }
          );

          return;
        }

        const text =
          clean(
            data?.text ||
              data?.body,
            MAX_TEXT
          );

        if (!text) {
          return;
        }

        const conversationId =
          data?.conversationId ||
          (await getConversationBetween(
            userId,
            target.id,
            accessToken
          ));

        const finalConversationId =
          conversationId ||
          (await createDirectConversation(
            userId,
            target.id,
            accessToken
          ));

        const message =
          await insertMessage(
            finalConversationId,
            userId,
            text,
            accessToken
          );

        socket.join(
          conversationRoom(
            finalConversationId
          )
        );

        const targetSocket =
          getSocketForUser(
            target.id
          );

        if (targetSocket) {
          targetSocket.join(
            conversationRoom(
              finalConversationId
            )
          );
        }

        /*
          Send to sender.
        */
        socket.emit(
          "message:new",
          {
            ...message,
            from:
              socket.data.username,
            to:
              target.username
          }
        );

        /*
          Send to recipient.
        */
        emitToUser(
          target.id,
          "message:new",
          {
            ...message,
            from:
              socket.data.username,
            to:
              target.username
          }
        );

        /*
          Add sender -> recipient as contact.
        */
        await addContact(
          userId,
          target.id,
          accessToken
        ).catch(() => {});

        /*
          If admin client exists, also add recipient -> sender.
          This is server-side and bypasses user RLS.
        */
        if (supabaseAdmin) {
          await addContact(
            target.id,
            userId,
            null
          ).catch(() => {});
        }

        const userContacts =
          await listContacts(
            userId,
            accessToken
          );

        socket.emit(
          "contacts:update",
          {
            contacts:
              userContacts
          }
        );

        if (
          targetSocket
        ) {
          const targetContacts =
            await listContacts(
              target.id,
              null
            ).catch(
              () => []
            );

          targetSocket.emit(
            "contacts:update",
            {
              contacts:
                targetContacts
            }
          );
        }
      } catch (error) {
        console.error(
          "message:send error:",
          error
        );

        socket.emit(
          "message:error",
          {
            message:
              error?.message ||
              "Mesaj la pa ka voye."
          }
        );
      }
    }
  );

  /* =======================================================
     TYPING
  ======================================================= */

  socket.on(
    "typing",
    async data => {
      try {
        if (!userId || !accessToken) {
          return;
        }

        const targetUsername =
          normalizeUsername(
            data?.to
          );

        const target =
          await getProfileByUsername(
            targetUsername,
            accessToken
          );

        if (!target) {
          return;
        }

        emitToUser(
          target.id,
          "typing",
          {
            from:
              socket.data.username,
            typing:
              !!data?.typing
          }
        );
      } catch (error) {
        console.error(
          "typing error:",
          error
        );
      }
    }
  );

  /* =======================================================
     CALL START
  ======================================================= */

  socket.on(
    "call:start",
    async data => {
      try {
        if (!userId || !accessToken) {
          return;
        }

        const targetUsername =
          normalizeUsername(
            data?.to
          );

        const type =
          data?.type === "video"
            ? "video"
            : "audio";

        const target =
          await getProfileByUsername(
            targetUsername,
            accessToken
          );

        if (
          !target ||
          target.id === userId
        ) {
          socket.emit(
            "call:error",
            {
              message:
                "Moun ou vle rele a pa disponib."
            }
          );
          return;
        }

        const targetSocket =
          getSocketForUser(
            target.id
          );

        if (!targetSocket) {
          socket.emit(
            "call:error",
            {
              message:
                "Moun sa pa online kounye a."
            }
          );
          return;
        }

        const callId =
          crypto.randomUUID();

        const call = {
          id: callId,
          callerId: userId,
          calleeId: target.id,
          callerUsername:
            socket.data.username,
          calleeUsername:
            target.username,
          type,
          state: "ringing",
          createdAt:
            Date.now()
        };

        liveCalls.set(
          callId,
          call
        );

        /*
          Persist call record.
        */
        if (supabaseAdmin) {
          await supabaseAdmin
            .from("calls")
            .insert({
              id: callId,
              caller_id: userId,
              call_type: type,
              status: "ringing"
            })
            .catch(error => {
              console.error(
                "calls insert:",
                error
              );
            });
        }

        targetSocket.emit(
          "call:incoming",
          {
            callId,
            from:
              socket.data.username,
            fromUser:
              publicUserFromProfile(
                await getProfileById(
                  userId,
                  accessToken
                )
              ),
            type
          }
        );

        socket.emit(
          "call:started",
          {
            callId,
            to:
              target.username,
            type
          }
        );
      } catch (error) {
        console.error(
          "call:start error:",
          error
        );

        socket.emit(
          "call:error",
          {
            message:
              error?.message ||
              "Apèl la pa ka kòmanse."
          }
        );
      }
    }
  );

  /* =======================================================
     CALL ACCEPT
  ======================================================= */

  socket.on(
    "call:accept",
    async data => {
      try {
        if (!userId) return;

        const callId =
          String(
            data?.callId || ""
          );

        const call =
          liveCalls.get(
            callId
          );

        if (
          !call ||
          call.calleeId !== userId
        ) {
          return;
        }

        call.state =
          "accepted";

        liveCalls.set(
          callId,
          call
        );

        if (supabaseAdmin) {
          await supabaseAdmin
            .from("calls")
            .update({
              status:
                "accepted"
            })
            .eq(
              "id",
              callId
            )
            .catch(() => {});
        }

        emitToUser(
          call.callerId,
          "call:accepted",
          {
            callId,
            by:
              socket.data.username,
            type:
              call.type
          }
        );
      } catch (error) {
        console.error(
          "call:accept error:",
          error
        );
      }
    }
  );

  /* =======================================================
     CALL REJECT
  ======================================================= */

  socket.on(
    "call:reject",
    async data => {
      try {
        if (!userId) return;

        const callId =
          String(
            data?.callId || ""
          );

        const call =
          liveCalls.get(
            callId
          );

        if (
          !call ||
          !callParticipants(
            call
          ).includes(userId)
        ) {
          return;
        }

        const other =
          otherCallParticipant(
            call,
            userId
          );

        emitToUser(
          other,
          "call:rejected",
          {
            callId,
            by:
              socket.data.username
          }
        );

        if (supabaseAdmin) {
          await supabaseAdmin
            .from("calls")
            .update({
              status:
                "rejected",
              ended_at:
                new Date().toISOString()
            })
            .eq(
              "id",
              callId
            )
            .catch(() => {});
        }

        liveCalls.delete(
          callId
        );
      } catch (error) {
        console.error(
          "call:reject error:",
          error
        );
      }
    }
  );

  /* =======================================================
     CALL END
  ======================================================= */

  socket.on(
    "call:end",
    async data => {
      try {
        if (!userId) return;

        const callId =
          String(
            data?.callId || ""
          );

        const call =
          liveCalls.get(
            callId
          );

        if (
          !call ||
          !callParticipants(
            call
          ).includes(userId)
        ) {
          return;
        }

        const other =
          otherCallParticipant(
            call,
            userId
          );

        emitToUser(
          other,
          "call:ended",
          {
            callId,
            by:
              socket.data.username
          }
        );

        socket.emit(
          "call:ended",
          {
            callId,
            by:
              socket.data.username
          }
        );

        if (supabaseAdmin) {
          await supabaseAdmin
            .from("calls")
            .update({
              status:
                "ended",
              ended_at:
                new Date().toISOString()
            })
            .eq(
              "id",
              callId
            )
            .catch(() => {});
        }

        liveCalls.delete(
          callId
        );
      } catch (error) {
        console.error(
          "call:end error:",
          error
        );
      }
    }
  );

  /* =======================================================
     WEBRTC OFFER
  ======================================================= */

  socket.on(
    "webrtc:offer",
    data => {
      if (!userId) return;

      const call =
        liveCalls.get(
          String(
            data?.callId || ""
          )
        );

      if (
        !call ||
        !callParticipants(
          call
        ).includes(userId)
      ) {
        return;
      }

      const other =
        otherCallParticipant(
          call,
          userId
        );

      emitToUser(
        other,
        "webrtc:offer",
        {
          callId:
            call.id,
          offer:
            data.offer
        }
      );
    }
  );

  /* =======================================================
     WEBRTC ANSWER
  ======================================================= */

  socket.on(
    "webrtc:answer",
    data => {
      if (!userId) return;

      const call =
        liveCalls.get(
          String(
            data?.callId || ""
          )
        );

      if (
        !call ||
        !callParticipants(
          call
        ).includes(userId)
      ) {
        return;
      }

      const other =
        otherCallParticipant(
          call,
          userId
        );

      emitToUser(
        other,
        "webrtc:answer",
        {
          callId:
            call.id,
          answer:
            data.answer
        }
      );
    }
  );

  /* =======================================================
     WEBRTC ICE
  ======================================================= */

  socket.on(
    "webrtc:ice-candidate",
    data => {
      if (!userId) return;

      const call =
        liveCalls.get(
          String(
            data?.callId || ""
          )
        );

      if (
        !call ||
        !callParticipants(
          call
        ).includes(userId)
      ) {
        return;
      }

      const other =
        otherCallParticipant(
          call,
          userId
        );

      emitToUser(
        other,
        "webrtc:ice-candidate",
        {
          callId:
            call.id,
          candidate:
            data.candidate
        }
      );
    }
  );

  /* =======================================================
     DISCONNECT
  ======================================================= */

  socket.on(
    "disconnect",
    async () => {
      if (!userId) {
        return;
      }

      const live =
        liveUsers.get(
          userId
        );

      if (
        live &&
        live.socketId ===
          socket.id
      ) {
        live.online = false;
        live.socketId = null;

        liveUsers.set(
          userId,
          live
        );
      }

      for (
        const [callId, call]
        of liveCalls.entries()
      ) {
        if (
          !callParticipants(
            call
          ).includes(userId)
        ) {
          continue;
        }

        const other =
          otherCallParticipant(
            call,
            userId
          );

        emitToUser(
          other,
          "call:ended",
          {
            callId,
            by:
              socket.data.username ||
              userId
          }
        );

        if (supabaseAdmin) {
          await supabaseAdmin
            .from("calls")
            .update({
              status:
                "ended",
              ended_at:
                new Date().toISOString()
            })
            .eq(
              "id",
              callId
            )
            .catch(() => {});
        }

        liveCalls.delete(
          callId
        );
      }

      /*
        Refresh online users.
      */
      try {
        const users =
          await listProfiles(
            accessToken
          );

        io.emit(
          "users:update",
          {
            users
          }
        );
      } catch (error) {
        console.error(
          "disconnect refresh:",
          error
        );
      }
    }
  );
});

/* =========================================================
   START
========================================================= */

server.listen(
  PORT,
  () => {
    console.log(
      `ZIQVONA v3.0.0 running on port ${PORT}`
    );

    console.log(
      `Supabase configured: ${
        !!supabasePublic
      }`
    );

    console.log(
      `Service role configured: ${
        !!supabaseAdmin
      }`
    );

    console.log(
      `TURN configured: ${
        !!(
          process.env.TURN_USERNAME &&
          process.env.TURN_PASSWORD
        )
      }`
    );
  }
);
