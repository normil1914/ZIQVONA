const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 3000);
const MAX_USERS = 1200;
const MAX_TEXT = 4000;
const MAX_MEDIA = 15 * 1024 * 1024;
const MAX_HISTORY = 5000;
const MAX_PROFILE_STATUS = 120;
const MAX_AVATAR = 2 * 1024 * 1024;
const MAX_MESSAGE_RATE = 60;
const RATE_WINDOW_MS = 60 * 1000;

const users = new Map();       // username -> websocket
const clients = new Map();     // websocket -> username
const profiles = new Map();    // username -> profile
const messages = [];            // temporary in-memory history
const rateLimits = new Map();   // username -> timestamps

const PUBLIC_DIR = path.join(__dirname, "public");

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".mp3": "audio/mpeg",
  ".wav": "audio/wav",
  ".ogg": "audio/ogg",
  ".mp4": "video/mp4",
  ".webm": "video/webm"
};

function send(ws, data) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;

  try {
    ws.send(JSON.stringify(data));
    return true;
  } catch {
    return false;
  }
}

function broadcast(data) {
  wss.clients.forEach((client) => send(client, data));
}

function cleanUsername(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 30);
}

function cleanText(value) {
  return String(value || "")
    .trim()
    .slice(0, MAX_TEXT);
}

function cleanStatus(value) {
  return String(value || "")
    .trim()
    .slice(0, MAX_PROFILE_STATUS);
}

function isValidUsername(username) {
  return (
    username.length >= 2 &&
    username.length <= 30 &&
    /^[\p{L}\p{N} ._-]+$/u.test(username)
  );
}

function getOnlineUsers() {
  return [...users.keys()];
}

function publicProfile(username) {
  const p = profiles.get(username) || {};

  return {
    username,
    displayName: p.displayName || username,
    status: p.status || "Disponib",
    avatar: p.avatar || ""
  };
}

function allProfiles() {
  const result = {};

  profiles.forEach((_, username) => {
    result[username] = publicProfile(username);
  });

  return result;
}

function sendPresence() {
  broadcast({
    type: "presence",
    online: getOnlineUsers(),
    profiles: allProfiles()
  });
}

function makeId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  return (
    Date.now().toString(36) +
    "-" +
    Math.random().toString(36).slice(2)
  );
}

function checkRateLimit(username) {
  if (!username) return true;

  const now = Date.now();
  let list = rateLimits.get(username) || [];

  list = list.filter((time) => now - time < RATE_WINDOW_MS);

  if (list.length >= MAX_MESSAGE_RATE) {
    rateLimits.set(username, list);
    return false;
  }

  list.push(now);
  rateLimits.set(username, list);

  return true;
}

function removeOldMessages() {
  if (messages.length <= MAX_HISTORY) return;

  const extra = messages.length - MAX_HISTORY;
  messages.splice(0, extra);
}

function normalizeMedia(media) {
  if (!media || typeof media !== "object") return null;

  const type = String(media.type || "").toLowerCase();

  const allowedTypes = [
    "image",
    "video",
    "audio",
    "file"
  ];

  if (!allowedTypes.includes(type)) {
    return null;
  }

  const url = String(media.url || "");

  if (!url) {
    return null;
  }

  if (url.length > MAX_MEDIA) {
    return null;
  }

  let name = String(media.name || "media")
    .trim()
    .slice(0, 120);

  let mime = String(media.mime || "")
    .trim()
    .slice(0, 100);

  return {
    type,
    url,
    name,
    mime
  };
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw.toString());
  } catch {
    return null;
  }
}

function getFilePath(requestUrl) {
  let pathname;

  try {
    pathname = decodeURIComponent(
      requestUrl.split("?")[0]
    );
  } catch {
    return null;
  }

  if (pathname === "/") {
    pathname = "/index.html";
  }

  pathname = pathname.replace(/^\/+/, "");

  const resolved = path.resolve(
    PUBLIC_DIR,
    pathname
  );

  const publicRoot = path.resolve(PUBLIC_DIR);

  if (
    resolved !== publicRoot &&
    !resolved.startsWith(publicRoot + path.sep)
  ) {
    return null;
  }

  return resolved;
}

const server = http.createServer((req, res) => {
  const pathname = req.url.split("?")[0];

  // Health check pou Render
  if (pathname === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    });

    return res.end(
      JSON.stringify({
        ok: true,
        app: "ZIQVONA",
        version: "1.0.0",
        onlineUsers: users.size,
        time: new Date().toISOString()
      })
    );
  }

  // Simple API info
  if (pathname === "/api/status") {
    res.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    });

    return res.end(
      JSON.stringify({
        app: "ZIQVONA",
        version: "1.0.0",
        status: "online"
      })
    );
  }

  const filePath = getFilePath(req.url);

  if (!filePath) {
    res.writeHead(400);
    return res.end("Bad request");
  }

  fs.readFile(filePath, (error, data) => {
    if (error) {
      res.writeHead(404, {
        "Content-Type": "text/plain; charset=utf-8"
      });

      return res.end("Not found");
    }

    const ext = path.extname(filePath).toLowerCase();

    res.writeHead(200, {
      "Content-Type":
        MIME_TYPES[ext] ||
        "application/octet-stream",
      "Cache-Control":
        ext === ".html"
          ? "no-cache"
          : "public, max-age=3600"
    });

    res.end(data);
  });
});

const wss = new WebSocket.Server({
  server,
  maxPayload: MAX_MEDIA
});

wss.on("connection", (ws, request) => {
  ws.isAlive = true;
  ws.ip = request.socket.remoteAddress || "";

  send(ws, {
    type: "welcome",
    app: "ZIQVONA",
    version: "1.0.0"
  });

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw) => {
    // Pwoteksyon kont mesaj twò gwo
    if (raw.length > MAX_MEDIA) {
      return send(ws, {
        type: "error",
        message: "Fichye oswa mesaj la twò gwo."
      });
    }

    const m = safeJsonParse(raw);

    if (!m || typeof m !== "object") {
      return send(ws, {
        type: "error",
        message: "Done yo pa valab."
      });
    }

    // LOGIN
    if (m.type === "login") {
      const username = cleanUsername(m.username);

      if (!username) {
        return send(ws, {
          type: "error",
          message: "Mete yon non."
        });
      }

      if (!isValidUsername(username)) {
        return send(ws, {
          type: "error",
          message:
            "Non an dwe gen 2-30 karaktè epi sèvi ak lèt, chif, espas, - oswa _."
        });
      }

      if (users.size >= MAX_USERS && !users.has(username)) {
        return send(ws, {
          type: "error",
          message:
            "ZIQVONA rive nan limit itilizatè aktyèl la. Eseye pita."
        });
      }

      if (
        users.has(username) &&
        users.get(username) !== ws
      ) {
        return send(ws, {
          type: "error",
          message: "Non sa deja konekte."
        });
      }

      // Si websocket sa te deja konekte ak yon lòt non
      const oldUsername = clients.get(ws);

      if (oldUsername && oldUsername !== username) {
        users.delete(oldUsername);
      }

      users.set(username, ws);
      clients.set(ws, username);

      if (!profiles.has(username)) {
        profiles.set(username, {
          displayName: username,
          status: "Disponib",
          avatar: ""
        });
      }

      send(ws, {
        type: "login_ok",
        username,
        online: getOnlineUsers(),
        profiles: allProfiles(),
        profile: publicProfile(username)
      });

      sendPresence();

      return;
    }

    const from = clients.get(ws);

    if (!from) {
      return send(ws, {
        type: "error",
        message: "Tanpri konekte anvan."
      });
    }

    // PROFILE GET
    if (m.type === "profile_get") {
      return send(ws, {
        type: "profile",
        profile: publicProfile(from)
      });
    }

    // PROFILE UPDATE
    if (m.type === "profile_update") {
      const current = profiles.get(from) || {};

      let avatar = String(
        m.avatar ?? current.avatar ?? ""
      );

      if (avatar.length > MAX_AVATAR) {
        avatar = "";
      }

      profiles.set(from, {
        displayName: String(
          m.displayName ??
          current.displayName ??
          from
        )
          .trim()
          .slice(0, 50),

        status: cleanStatus(
          m.status ??
          current.status ??
          "Disponib"
        ),

        avatar
      });

      send(ws, {
        type: "profile",
        profile: publicProfile(from)
      });

      broadcast({
        type: "profiles",
        profiles: allProfiles()
      });

      return;
    }

    // SEARCH USERS
    if (m.type === "search_users") {
      const q = String(m.query || "")
        .trim()
        .toLowerCase()
        .slice(0, 30);

      const results = Object.values(allProfiles())
        .filter((profile) => {
          if (profile.username === from) {
            return false;
          }

          if (!q) return true;

          return (
            profile.username
              .toLowerCase()
              .includes(q) ||
            profile.displayName
              .toLowerCase()
              .includes(q) ||
            profile.status
              .toLowerCase()
              .includes(q)
          );
        })
        .map((profile) => ({
          ...profile,
          online: users.has(profile.username)
        }))
        .slice(0, 50);

      return send(ws, {
        type: "search_results",
        results
      });
    }

    // TEXT MESSAGE
    if (m.type === "message") {
      if (!checkRateLimit(from)) {
        return send(ws, {
          type: "error",
          message:
            "Ou voye twòp mesaj twò vit. Tanpri tann yon ti moman."
        });
      }

      const to = cleanUsername(m.to);
      const text = cleanText(m.text);

      if (!to || !text) {
        return send(ws, {
          type: "error",
          message: "Mesaj la pa valab."
        });
      }

      if (to === from) {
        return send(ws, {
          type: "error",
          message: "Ou pa ka voye mesaj bay tèt ou."
        });
      }

      const packet = {
        type: "message",
        id: makeId(),
        from,
        to,
        text,
        time: new Date().toISOString()
      };

      messages.push(packet);
      removeOldMessages();

      send(ws, packet);

      const recipient = users.get(to);

      if (recipient) {
        send(recipient, packet);
      }

      return;
    }

    // MEDIA MESSAGE
    if (m.type === "media_message") {
      if (!checkRateLimit(from)) {
        return send(ws, {
          type: "error",
          message:
            "Ou voye twòp fichye twò vit. Tanpri tann yon ti moman."
        });
      }

      const to = cleanUsername(m.to);
      const media = normalizeMedia(m.media);
      const caption = cleanText(m.caption);

      if (!to || !media) {
        return send(ws, {
          type: "error",
          message: "Media a pa valab."
        });
      }

      if (to === from) {
        return send(ws, {
          type: "error",
          message: "Ou pa ka voye media bay tèt ou."
        });
      }

      const packet = {
        type: "media_message",
        id: makeId(),
        from,
        to,
        media,
        caption,
        time: new Date().toISOString()
      };

      messages.push(packet);
      removeOldMessages();

      send(ws, packet);

      const recipient = users.get(to);

      if (recipient) {
        send(recipient, packet);
      }

      return;
    }

    // HISTORY
    if (m.type === "history") {
      const withUser = cleanUsername(m.with);

      if (!withUser) {
        return send(ws, {
          type: "history",
          with: "",
          messages: []
        });
      }

      const history = messages
        .filter((message) => {
          return (
            (message.from === from &&
              message.to === withUser) ||
            (message.from === withUser &&
              message.to === from)
          );
        })
        .slice(-200);

      return send(ws, {
        type: "history",
        with: withUser,
        messages: history
      });
    }

    // TYPING
    if (m.type === "typing") {
      const to = cleanUsername(m.to);

      if (to && users.has(to)) {
        send(users.get(to), {
          type: "typing",
          from
        });
      }

      return;
    }

    // READ RECEIPT
    if (m.type === "message_read") {
      const to = cleanUsername(m.to);
      const messageId = String(m.messageId || "")
        .slice(0, 100);

      if (to && users.has(to) && messageId) {
        send(users.get(to), {
          type: "message_read",
          messageId,
          from
        });
      }

      return;
    }

    // DELIVERY RECEIPT
    if (m.type === "message_delivered") {
      const to = cleanUsername(m.to);
      const messageId = String(m.messageId || "")
        .slice(0, 100);

      if (to && users.has(to) && messageId) {
        send(users.get(to), {
          type: "message_delivered",
          messageId,
          from
        });
      }

      return;
    }

    // BLOCK USER
    if (m.type === "block_user") {
      const target = cleanUsername(m.username);

      if (!target || target === from) {
        return;
      }

      // Block persistent la ap vini nan database.
      // Pou V1 backend sa a, nou konfime aksyon an.
      return send(ws, {
        type: "block_ok",
        username: target
      });
    }

    // REPORT USER
    if (m.type === "report_user") {
      const target = cleanUsername(m.username);
      const reason = cleanText(m.reason).slice(0, 500);

      if (!target || target === from) {
        return;
      }

      console.log(
        "[REPORT]",
        JSON.stringify({
          from,
          target,
          reason,
          time: new Date().toISOString()
        })
      );

      return send(ws, {
        type: "report_ok",
        username: target
      });
    }

    // WEBRTC SIGNALING
    if (
      m.type === "call-offer" ||
      m.type === "call-answer" ||
      m.type === "ice" ||
      m.type === "call-end"
    ) {
      const to = cleanUsername(m.to);

      if (!to) return;

      const recipient = users.get(to);

      if (!recipient) {
        if (m.type !== "call-end") {
          send(ws, {
            type: "error",
            message:
              "Itilizatè a pa konekte kounye a."
          });
        }

        return;
      }

      const packet = {
        ...m,
        from
      };

      // Pa kite yon kliyan modifye from
      delete packet.sender;
      packet.from = from;

      send(recipient, packet);

      return;
    }

    // UNKNOWN MESSAGE
    return send(ws, {
      type: "error",
      message: "Aksyon ZIQVONA sa a pa rekonèt."
    });
  });

  ws.on("close", () => {
    const username = clients.get(ws);

    if (!username) return;

    if (users.get(username) === ws) {
      users.delete(username);
    }

    clients.delete(ws);
    rateLimits.delete(username);

    sendPresence();
  });

  ws.on("error", () => {
    // close handler ap netwaye koneksyon an
  });
});

// Ping tout clients pou evite koneksyon ki mouri rete nan memwa.
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      try {
        ws.terminate();
      } catch {}

      return;
    }

    ws.isAlive = false;

    try {
      ws.ping();
    } catch {}
  });
}, 30000);

wss.on("close", () => {
  clearInterval(heartbeat);
});

server.listen(PORT, () => {
  console.log(
    `ZIQVONA V1.0 running on port ${PORT}`
  );
  console.log(
    `PORT: ${PORT}`
  );
});
