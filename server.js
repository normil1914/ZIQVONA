const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");
const { createClient } = require("@supabase/supabase-js");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 10000;

const SUPABASE_URL =
  process.env.SUPABASE_URL ||
  "https://hkirnyqousvmphpwqfga.supabase.co";

const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "";

const TURN_HOST =
  process.env.TURN_HOST || "global.relay.metered.ca";

const TURN_USERNAME =
  process.env.TURN_USERNAME || "";

const TURN_PASSWORD =
  process.env.TURN_PASSWORD || "";

const admin = SUPABASE_SERVICE_ROLE_KEY
  ? createClient(
      SUPABASE_URL,
      SUPABASE_SERVICE_ROLE_KEY,
      {
        auth: {
          autoRefreshToken: false,
          persistSession: false
        }
      }
    )
  : null;

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"]
});

app.use(express.json({ limit: "8mb" }));

app.use(
  express.static(path.join(__dirname, "public"))
);

/* =========================
   HEALTH
========================= */

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    app: "ZIQVONA",
    supabase: !!SUPABASE_SERVICE_ROLE_KEY,
    turn: !!(
      TURN_HOST &&
      TURN_USERNAME &&
      TURN_PASSWORD
    ),
    time: new Date().toISOString()
  });
});

/* =========================
   TURN
========================= */

app.get("/api/turn", (_req, res) => {
  const iceServers = [
    {
      urls: "stun:stun.l.google.com:19302"
    },
    {
      urls: "stun:stun1.l.google.com:19302"
    }
  ];

  if (
    TURN_HOST &&
    TURN_USERNAME &&
    TURN_PASSWORD
  ) {
    iceServers.push(
      {
        urls: `turn:${TURN_HOST}:80`,
        username: TURN_USERNAME,
        credential: TURN_PASSWORD
      },
      {
        urls: `turn:${TURN_HOST}:80?transport=tcp`,
        username: TURN_USERNAME,
        credential: TURN_PASSWORD
      },
      {
        urls: `turn:${TURN_HOST}:443`,
        username: TURN_USERNAME,
        credential: TURN_PASSWORD
      },
      {
        urls: `turn:${TURN_HOST}:443?transport=tcp`,
        username: TURN_USERNAME,
        credential: TURN_PASSWORD
      }
    );
  }

  res.json({
    iceServers,
    turnConfigured: !!(
      TURN_HOST &&
      TURN_USERNAME &&
      TURN_PASSWORD
    )
  });
});

/* =========================
   SPA
========================= */

app.get("*", (req, res, next) => {
  if (req.path.startsWith("/api/")) {
    return next();
  }

  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});

/* =========================
   ONLINE USERS
========================= */

const online = new Map();

/* =========================
   SOCKET AUTH
========================= */

io.use(async (socket, next) => {
  try {
    if (!admin) {
      return next(
        new Error(
          "SUPABASE_SERVICE_ROLE_KEY missing on server."
        )
      );
    }

    const token =
      socket.handshake.auth?.accessToken;

    if (!token) {
      return next(
        new Error(
          "Authentication required."
        )
      );
    }

    const {
      data,
      error
    } = await admin.auth.getUser(token);

    if (
      error ||
      !data ||
      !data.user
    ) {
      return next(
        new Error(
          "Invalid or expired Supabase session."
        )
      );
    }

    socket.user = data.user;

    next();

  } catch (error) {

    console.error(
      "Socket authentication error:",
      error
    );

    next(
      new Error(
        "Authentication failed."
      )
    );
  }
});

/* =========================
   HELPERS
========================= */

function cleanText(
  value,
  max = 4000
) {
  return String(
    value ?? ""
  )
    .trim()
    .slice(0, max);
}

function profileObject(row) {
  if (!row) return null;

  return {
    id: row.id,
    username: row.username || "",
    displayName:
      row.display_name ||
      row.username ||
      "ZIQVONA User",
    status: row.status || "",
    avatar:
      row.avatar_url || ""
  };
}

function sendToUser(
  userId,
  event,
  data
) {
  const sockets =
    online.get(userId);

  if (!sockets) return;

  for (
    const socketId of sockets
  ) {
    io
      .to(socketId)
      .emit(event, data);
  }
}

/* =========================
   PROFILE
========================= */

async function getProfile(
  userId
) {
  const {
    data,
    error
  } = await admin
    .from("profiles")
    .select("*")
    .eq("id", userId)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data || null;
}

async function ensureProfile(
  user,
  requested = {}
) {
  const existing =
    await getProfile(user.id);

  if (existing) {
    return existing;
  }

  let username =
    cleanText(
      requested.username ||
        user.user_metadata
          ?.username ||
        "",
      32
    )
      .toLowerCase()
      .replace(
        /[^a-z0-9_.-]/g,
        ""
      );

  if (!username) {
    username =
      "user_" +
      user.id.slice(0, 8);
  }

  const {
    data: taken
  } = await admin
    .from("profiles")
    .select("id")
    .eq(
      "username",
      username
    )
    .neq(
      "id",
      user.id
    )
    .limit(1);

  if (
    taken &&
    taken.length
  ) {
    username =
      username +
      "_" +
      user.id.slice(0, 5);
  }

  const displayName =
    cleanText(
      requested.displayName ||
        user.user_metadata
          ?.display_name ||
        user.user_metadata
          ?.full_name ||
        username,
      80
    ) || username;

  const {
    data,
    error
  } = await admin
    .from("profiles")
    .insert({
      id: user.id,
      username,
      display_name:
        displayName,
      status: "",
      avatar_url: ""
    })
    .select("*")
    .single();

  if (!error) {
    return data;
  }

  const retry =
    await getProfile(user.id);

  if (retry) {
    return retry;
  }

  throw error;
}

/* =========================
   PEOPLE
========================= */

async function listPeople(
  currentId
) {
  const {
    data,
    error
  } = await admin
    .from("profiles")
    .select(
      "id,username,display_name,status,avatar_url,updated_at"
    )
    .neq(
      "id",
      currentId
    )
    .order(
      "display_name",
      {
        ascending: true
      }
    )
    .limit(1200);

  if (error) {
    throw error;
  }

  return (
    data || []
  ).map((p) => ({
    ...profileObject(p),
    online:
      online.has(p.id) &&
      online
        .get(p.id)
        .size > 0
  }));
}

/* =========================
   CONTACTS
========================= */

async function listContacts(
  userId
) {
  const {
    data,
    error
  } = await admin
    .from("contacts")
    .select("contact_id")
    .eq(
      "owner_id",
      userId
    );

  if (error) {
    throw error;
  }

  const ids =
    (data || [])
      .map(
        x => x.contact_id
      );

  if (!ids.length) {
    return [];
  }

  const {
    data: profiles,
    error: profileError
  } = await admin
    .from("profiles")
    .select(
      "id,username,display_name,status,avatar_url"
    )
    .in(
      "id",
      ids
    );

  if (profileError) {
    throw profileError;
  }

  return (
    profiles || []
  ).map((p) => ({
    ...profileObject(p),
    online:
      online.has(p.id) &&
      online
        .get(p.id)
        .size > 0
  }));
}

/* =========================
   DIRECT CONVERSATION
========================= */

async function getDirectConversation(
  userA,
  userB
) {
  const {
    data: mine,
    error: mineError
  } = await admin
    .from(
      "conversation_members"
    )
    .select(
      "conversation_id"
    )
    .eq(
      "user_id",
      userA
    );

  if (mineError) {
    throw mineError;
  }

  const ids =
    (mine || [])
      .map(
        x => x.conversation_id
      );

  if (!ids.length) {
    return null;
  }

  const {
    data: shared,
    error: sharedError
  } = await admin
    .from(
      "conversation_members"
    )
    .select(
      "conversation_id"
    )
    .eq(
      "user_id",
      userB
    )
    .in(
      "conversation_id",
      ids
    );

  if (sharedError) {
    throw sharedError;
  }

  if (
    !shared ||
    !shared.length
  ) {
    return null;
  }

  const sharedIds =
    shared.map(
      x => x.conversation_id
    );

  const {
    data: conversations,
    error
  } = await admin
    .from("conversations")
    .select(
      "id,type"
    )
    .in(
      "id",
      sharedIds
    )
    .eq(
      "type",
      "direct"
    );

  if (error) {
    throw error;
  }

  return (
    conversations &&
    conversations[0]
  ) || null;
}

async function ensureDirectConversation(
  userA,
  userB
) {
  const existing =
    await getDirectConversation(
      userA,
      userB
    );

  if (existing) {
    return existing;
  }

  const {
    data: conversation,
    error
  } = await admin
    .from("conversations")
    .insert({
      type: "direct"
    })
    .select(
      "id,type"
    )
    .single();

  if (error) {
    throw error;
  }

  const {
    error: memberError
  } = await admin
    .from(
      "conversation_members"
    )
    .insert([
      {
        conversation_id:
          conversation.id,
        user_id: userA
      },
      {
        conversation_id:
          conversation.id,
        user_id: userB
      }
    ]);

  if (memberError) {

    const fallback =
      await getDirectConversation(
        userA,
        userB
      );

    if (fallback) {
      return fallback;
    }

    throw memberError;
  }

  return conversation;
}

/* =========================
   CONNECTION
========================= */

io.on(
  "connection",
  async socket => {

    const userId =
      socket.user.id;

    try {

      const requested =
        socket.handshake
          .auth?.profile ||
        {};

      const profile =
        await ensureProfile(
          socket.user,
          requested
        );

      if (
        !online.has(userId)
      ) {
        online.set(
          userId,
          new Set()
        );
      }

      online
        .get(userId)
        .add(socket.id);

      socket.join(
        "user:" + userId
      );

      const people =
        await listPeople(
          userId
        );

      const contacts =
        await listContacts(
          userId
        );

      socket.emit(
        "login:success",
        {
          user: {
            id: userId,
            email:
              socket.user
                .email || "",
            phone:
              socket.user
                .phone || "",
            username:
              profile.username,
            displayName:
              profile.display_name ||
              profile.username
          },

          profile:
            profileObject(
              profile
            ),

          users:
            people,

          contacts:
            contacts
        }
      );

      io.emit(
        "presence:update",
        {
          userId,
          online: true
        }
      );

      /* =====================
         PEOPLE
      ===================== */

      socket.on(
        "people:list",
        async () => {
          try {

            socket.emit(
              "people:list",
              await listPeople(
                userId
              )
            );

          } catch (error) {

            socket.emit(
              "app:error",
              {
                message:
                  error.message
              }
            );
          }
        }
      );

      /* =====================
         PROFILE UPDATE
      ===================== */

      socket.on(
        "profile:update",
        async payload => {

          try {

            const username =
              cleanText(
                payload.username,
                32
              )
                .toLowerCase()
                .replace(
                  /[^a-z0-9_.-]/g,
                  ""
                );

            const displayName =
              cleanText(
                payload.displayName,
                80
              );

            const status =
              cleanText(
                payload.status,
                120
              );

            const avatarUrl =
              cleanText(
                payload.avatarUrl,
                1000000
              );

            const update = {
              username,
              display_name:
                displayName ||
                username,
              status,
              avatar_url:
                avatarUrl
            };

            const {
              data,
              error
            } = await admin
              .from("profiles")
              .update(update)
              .eq(
                "id",
                userId
              )
              .select("*")
              .single();

            if (error) {
              throw error;
            }

            socket.emit(
              "profile:updated",
              profileObject(
                data
              )
            );

            io.emit(
              "profile:changed",
              profileObject(
                data
              )
            );

          } catch (error) {

            socket.emit(
              "app:error",
              {
                message:
                  error.message ||
                  "Profile update failed."
              }
            );
          }
        }
      );

      /* =====================
         CONTACTS
      ===================== */

      socket.on(
        "contacts:list",
        async () => {

          try {

            socket.emit(
              "contacts:list",
              await listContacts(
                userId
              )
            );

          } catch (error) {

            socket.emit(
              "app:error",
              {
                message:
                  error.message
              }
            );
          }
        }
      );

      socket.on(
        "contact:add",
        async payload => {

          try {

            const contactId =
              payload.userId;

            if (
              !contactId ||
              contactId === userId
            ) {
              return;
            }

            const {
              error
            } = await admin
              .from("contacts")
              .upsert(
                {
                  owner_id:
                    userId,
                  contact_id:
                    contactId
                },
                {
                  onConflict:
                    "owner_id,contact_id"
                }
              );

            if (error) {
              throw error;
            }

            socket.emit(
              "contacts:list",
              await listContacts(
                userId
              )
            );

          } catch (error) {

            socket.emit(
              "app:error",
              {
                message:
                  error.message
              }
            );
          }
        }
      );

      socket.on(
        "contact:remove",
        async payload => {

          try {

            const contactId =
              payload.userId;

            const {
              error
            } = await admin
              .from("contacts")
              .delete()
              .eq(
                "owner_id",
                userId
              )
              .eq(
                "contact_id",
                contactId
              );

            if (error) {
              throw error;
            }

            socket.emit(
              "contacts:list",
              await listContacts(
                userId
              )
            );

          } catch (error) {

            socket.emit(
              "app:error",
              {
                message:
                  error.message
              }
            );
          }
        }
      );

      /* =====================
         OPEN CHAT
      ===================== */

      socket.on(
        "chat:open",
        async payload => {

          try {

            const otherUserId =
              payload.userId;

            if (
              !otherUserId ||
              otherUserId === userId
            ) {
              return;
            }

            const conversation =
              await ensureDirectConversation(
                userId,
                otherUserId
              );

            const {
              data: messages,
              error
            } = await admin
              .from("messages")
              .select(
                "id,conversation_id,sender_id,body,created_at"
              )
              .eq(
                "conversation_id",
                String(
                  conversation.id
                )
              )
              .order(
                "created_at",
                {
                  ascending:
                    true
                }
              )
              .limit(500);

            if (error) {
              throw error;
            }

            socket.emit(
              "chat:opened",
              {
                conversation,
                otherUserId,
                messages:
                  messages || []
              }
            );

          } catch (error) {

            socket.emit(
              "app:error",
              {
                message:
                  error.message ||
                  "Could not open chat."
              }
            );
          }
        }
      );

      /* =====================
         SEND MESSAGE
      ===================== */

      socket.on(
        "chat:message",
        async payload => {

          try {

            const conversationId =
              payload.conversationId;

            const body =
              cleanText(
                payload.body,
                4000
              );

            if (
              !conversationId ||
              !body
            ) {
              return;
            }

            const {
              data: member
            } = await admin
              .from(
                "conversation_members"
              )
              .select(
                "user_id"
              )
              .eq(
                "conversation_id",
                conversationId
              )
              .eq(
                "user_id",
                userId
              )
              .maybeSingle();

            if (!member) {
              return socket.emit(
                "app:error",
                {
                  message:
                    "Conversation access denied."
                }
              );
            }

            const {
              data: message,
              error
            } = await admin
              .from("messages")
              .insert({
                conversation_id:
                  String(
                    conversationId
                  ),
                sender_id:
                  userId,
                body
              })
              .select(
                "id,conversation_id,sender_id,body,created_at"
              )
              .single();

            if (error) {
              throw error;
            }

            const {
              data: members
            } = await admin
              .from(
                "conversation_members"
              )
              .select(
                "user_id"
              )
              .eq(
                "conversation_id",
                conversationId
              );

            for (
              const member of
              members || []
            ) {

              sendToUser(
                member.user_id,
                "chat:message",
                message
              );
            }

          } catch (error) {

            socket.emit(
              "app:error",
              {
                message:
                  error.message ||
                  "Message failed."
              }
            );
          }
        }
      );

      /* =====================
         TYPING
      ===================== */

      socket.on(
        "typing",
        payload => {

          if (
            payload.otherUserId
          ) {

            sendToUser(
              payload.otherUserId,
              "typing",
              {
                conversationId:
                  payload.conversationId,
                userId,
                isTyping:
                  !!payload.isTyping
              }
            );
          }
        }
      );

      /* =====================
         CALL START
      ===================== */

      socket.on(
        "call:start",
        async payload => {

          try {

            const targetUserId =
              payload.targetUserId;

            const callType =
              payload.callType ===
              "video"
                ? "video"
                : "voice";

            if (
              !targetUserId ||
              targetUserId === userId
            ) {
              return;
            }

            const conversation =
              await ensureDirectConversation(
                userId,
                targetUserId
              );

            const {
              data: call,
              error
            } = await admin
              .from("calls")
              .insert({
                conversation_id:
                  conversation.id,
                caller_id:
                  userId,
                call_type:
                  callType,
                status:
                  "ringing"
              })
              .select("*")
              .single();

            if (error) {
              throw error;
            }

            const caller =
              await getProfile(
                userId
              );

            sendToUser(
              targetUserId,
              "call:incoming",
              {
                callId:
                  call.id,

                conversationId:
                  conversation.id,

                callType,

                caller:
                  profileObject(
                    caller
                  )
              }
            );

            socket.emit(
              "call:started",
              {
                callId:
                  call.id,

                conversationId:
                  conversation.id,

                targetUserId,

                callType
              }
            );

          } catch (error) {

            socket.emit(
              "call:error",
              {
                message:
                  error.message ||
                  "Could not start call."
              }
            );
          }
        }
      );

      /* =====================
         CALL ACCEPT
      ===================== */

      socket.on(
        "call:accept",
        async payload => {

          try {

            const {
              data: call,
              error
            } = await admin
              .from("calls")
              .update({
                status:
                  "accepted"
              })
              .eq(
                "id",
                payload.callId
              )
              .select("*")
              .single();

            if (error) {
              throw error;
            }

            sendToUser(
              call.caller_id,
              "call:accepted",
              {
                callId:
                  call.id,
                callType:
                  call.call_type
              }
            );

          } catch (error) {

            socket.emit(
              "call:error",
              {
                message:
                  error.message
              }
            );
          }
        }
      );

      /* =====================
         CALL REJECT
      ===================== */

      socket.on(
        "call:reject",
        async payload => {

          try {

            const {
              data: call
            } = await admin
              .from("calls")
              .update({
                status:
                  "rejected",
                ended_at:
                  new Date().toISOString()
              })
              .eq(
                "id",
                payload.callId
              )
              .select("*")
              .single();

            if (call) {

              sendToUser(
                call.caller_id,
                "call:rejected",
                {
                  callId:
                    call.id
                }
              );
            }

          } catch (error) {

            socket.emit(
              "call:error",
              {
                message:
                  error.message
              }
            );
          }
        }
      );

      /* =====================
         CALL END
      ===================== */

      socket.on(
        "call:end",
        async payload => {

          try {

            const {
              data: call
            } = await admin
              .from("calls")
              .update({
                status:
                  "ended",
                ended_at:
                  new Date().toISOString()
              })
              .eq(
                "id",
                payload.callId
              )
              .select("*")
              .single();

            if (!call) {
              return;
            }

            const {
              data: members
            } = await admin
              .from(
                "conversation_members"
              )
              .select(
                "user_id"
              )
              .eq(
                "conversation_id",
                call.conversation_id
              );

            for (
              const member of
              members || []
            ) {

              if (
                member.user_id !==
                userId
              ) {

                sendToUser(
                  member.user_id,
                  "call:ended",
                  {
                    callId:
                      call.id
                  }
                );
              }
            }

          } catch (error) {

            socket.emit(
              "call:error",
              {
                message:
                  error.message
              }
            );
          }
        }
      );

      /* =====================
         WEBRTC OFFER
      ===================== */

      socket.on(
        "webrtc:offer",
        payload => {

          if (
            payload.targetUserId &&
            payload.callId &&
            payload.offer
          ) {

            sendToUser(
              payload.targetUserId,
              "webrtc:offer",
              {
                callId:
                  payload.callId,
                fromUserId:
                  userId,
                offer:
                  payload.offer
              }
            );
          }
        }
      );

      /* =====================
         WEBRTC ANSWER
      ===================== */

      socket.on(
        "webrtc:answer",
        payload => {

          if (
            payload.targetUserId &&
            payload.callId &&
            payload.answer
          ) {

            sendToUser(
              payload.targetUserId,
              "webrtc:answer",
              {
                callId:
                  payload.callId,
                fromUserId:
                  userId,
                answer:
                  payload.answer
              }
            );
          }
        }
      );

      /* =====================
         ICE
      ===================== */

      socket.on(
        "webrtc:ice-candidate",
        payload => {

          if (
            payload.targetUserId &&
            payload.callId &&
            payload.candidate
          ) {

            sendToUser(
              payload.targetUserId,
              "webrtc:ice-candidate",
              {
                callId:
                  payload.callId,
                fromUserId:
                  userId,
                candidate:
                  payload.candidate
              }
            );
          }
        }
      );

      /* =====================
         DISCONNECT
      ===================== */

      socket.on(
        "disconnect",
        () => {

          const set =
            online.get(
              userId
            );

          if (!set) {
            return;
          }

          set.delete(
            socket.id
          );

          if (!set.size) {

            online.delete(
              userId
            );

            io.emit(
              "presence:update",
              {
                userId,
                online: false
              }
            );
          }
        }
      );

    } catch (error) {

      console.error(
        "Connection setup error:",
        error
      );

      socket.emit(
        "app:error",
        {
          message:
            error.message ||
            "Account initialization failed."
        }
      );

      socket.disconnect(
        true
      );
    }
  }
);

/* =========================
   START
========================= */

server.listen(
  PORT,
  () => {

    console.log(
      `ZIQVONA running on port ${PORT}`
    );

    console.log(
      `Supabase configured: ${
        !!SUPABASE_SERVICE_ROLE_KEY
      }`
    );

    console.log(
      `TURN configured: ${
        !!(
          TURN_HOST &&
          TURN_USERNAME &&
          TURN_PASSWORD
        )
      }`
    );
  }
);
