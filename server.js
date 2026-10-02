const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  maxHttpBufferSize: 20 * 1024 * 1024
});

const PORT = process.env.PORT || 10000;

const MAX_USERS = 10000;
const MAX_TEXT = 4000;
const MAX_MEDIA = 12 * 1024 * 1024;
const MAX_HISTORY = 5000;
const MAX_STATUS = 120;
const MAX_AVATAR = 2 * 1024 * 1024;
const MAX_CONNECTIONS_PER_IP = 40;
const MAX_EVENTS_PER_MINUTE = 120;

const users = new Map();
const byName = new Map();
const profiles = new Map();
const messages = [];
const eventWindows = new Map();
const ipConnections = new Map();

app.use(express.json({ limit: '20mb' }));

app.use(express.static(path.join(__dirname, 'public')));

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    app: 'ZIQVONA',
    onlineUsers: users.size,
    maxUsers: MAX_USERS,
    uptime: Math.round(process.uptime())
  });
});

app.get('/config', (req, res) => {
  const iceServers = [
    {
      urls: [
        'stun:stun.l.google.com:19302',
        'stun:stun1.l.google.com:19302'
      ]
    }
  ];

  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL
        .split(',')
        .map(x => x.trim())
        .filter(Boolean),
      username: process.env.TURN_USERNAME || '',
      credential: process.env.TURN_CREDENTIAL || ''
    });
  }

  res.json({ iceServers });
});

app.get('*', (req, res) => {
  res.sendFile(
    path.join(__dirname, 'public', 'index.html')
  );
});

function clean(value, max = 1000) {
  return String(value ?? '')
    .trim()
    .slice(0, max);
}

function createId() {
  return `${Date.now()}-${Math.random()
    .toString(36)
    .slice(2)}-${Math.random()
    .toString(36)
    .slice(2)}`;
}

function publicProfile(username) {
  const profile = profiles.get(username) || {};

  return {
    username,
    displayName: profile.displayName || username,
    status: profile.status || 'Disponib',
    avatar: profile.avatar || '',
    online: byName.has(username)
  };
}

function allProfiles() {
  const result = {};

  profiles.forEach((_, username) => {
    result[username] = publicProfile(username);
  });

  return result;
}

function broadcastPresence() {
  io.emit('presence', {
    online: [...byName.keys()]
  });
}

function rateLimited(socket) {
  const now = Date.now();
  const key = socket.id;

  const row =
    eventWindows.get(key) || {
      start: now,
      count: 0
    };

  if (now - row.start >= 60000) {
    row.start = now;
    row.count = 0;
  }

  row.count += 1;

  eventWindows.set(key, row);

  return row.count > MAX_EVENTS_PER_MINUTE;
}

function sendToUser(username, event, data) {
  const socketId = byName.get(username);

  if (socketId) {
    io.to(socketId).emit(event, data);
  }

  return Boolean(socketId);
}

function privateHistory(userA, userB) {
  return messages
    .filter(message =>
      (message.from === userA && message.to === userB) ||
      (message.from === userB && message.to === userA)
    )
    .slice(-200);
}

io.use((socket, next) => {
  if (users.size >= MAX_USERS) {
    return next(
      new Error('ZIQVONA beta a plen pou kounye a.')
    );
  }

  const ip = socket.handshake.address || 'unknown';

  const count = ipConnections.get(ip) || 0;

  if (count >= MAX_CONNECTIONS_PER_IP) {
    return next(
      new Error(
        'Twòp koneksyon soti nan menm rezo a.'
      )
    );
  }

  ipConnections.set(ip, count + 1);

  socket.data.ip = ip;

  next();
});

io.on('connection', socket => {

  /*
   * REGISTER
   */

  socket.on('register', raw => {
    if (rateLimited(socket)) {
      return socket.emit('error-message', {
        message:
          'Twòp demann. Tanpri tann yon ti moman.'
      });
    }

    const username = clean(
      raw?.username || raw?.name,
      40
    );

    if (!username || username.length < 2) {
      return socket.emit('error-message', {
        message:
          'Non an dwe gen omwen 2 karaktè.'
      });
    }

    const oldSocketId = byName.get(username);

    if (
      oldSocketId &&
      oldSocketId !== socket.id
    ) {
      io.to(oldSocketId).emit(
        'force-disconnect',
        {
          message:
            'Kont sa a konekte sou yon lòt aparèy.'
        }
      );

      io.sockets.sockets
        .get(oldSocketId)
        ?.disconnect(true);

      users.delete(oldSocketId);
    }

    const oldProfile =
      profiles.get(username) || {};

    const user = {
      id: socket.id,

      username,

      displayName: clean(
        raw?.displayName ||
        raw?.name ||
        oldProfile.displayName ||
        username,
        50
      ),

      status: clean(
        raw?.status ||
        oldProfile.status ||
        'Disponib',
        MAX_STATUS
      ),

      avatar: clean(
        raw?.avatar ||
        oldProfile.avatar ||
        '',
        MAX_AVATAR
      )
    };

    profiles.set(username, {
      displayName: user.displayName,
      status: user.status,
      avatar: user.avatar
    });

    users.set(socket.id, user);

    byName.set(username, socket.id);

    socket.data.username = username;

    const knownProfiles =
      profiles.size <= 500
        ? allProfiles()
        : {
            [username]:
              publicProfile(username)
          };

    socket.emit('registered', {
      me: publicProfile(username),
      profiles: knownProfiles,
      online: [...byName.keys()]
    });

    broadcastPresence();
  });

  /*
   * UPDATE PROFILE
   */

  socket.on('update-profile', raw => {
    if (rateLimited(socket)) {
      return socket.emit('error-message', {
        message:
          'Twòp demann. Tanpri tann.'
      });
    }

    const username =
      socket.data.username;

    if (!username) return;

    const old =
      profiles.get(username) || {};

    const profile = {
      displayName: clean(
        raw?.displayName ??
          old.displayName ??
          username,
        50
      ),

      status: clean(
        raw?.status ??
          old.status ??
          'Disponib',
        MAX_STATUS
      ),

      avatar: String(
        raw?.avatar ??
          old.avatar ??
          ''
      ).slice(0, MAX_AVATAR)
    };

    profiles.set(username, profile);

    const user =
      users.get(socket.id);

    if (user) {
      Object.assign(user, profile);
    }

    socket.emit(
      'profile-updated',
      publicProfile(username)
    );

    broadcastPresence();
  });

  /*
   * SEARCH USERS
   */

  socket.on('search-users', raw => {
    if (rateLimited(socket)) return;

    const me =
      socket.data.username || '';

    const query = clean(
      raw?.query,
      50
    ).toLowerCase();

    const results = [...profiles.keys()]
      .filter(username =>
        username !== me
      )
      .map(publicProfile)
      .filter(profile =>
        !query ||
        profile.username
          .toLowerCase()
          .includes(query) ||
        profile.displayName
          .toLowerCase()
          .includes(query)
      )
      .slice(0, 100);

    socket.emit(
      'search-results',
      results
    );
  });

  /*
   * PRIVATE CHAT HISTORY
   */

  socket.on('get-history', raw => {
    const me =
      socket.data.username;

    const other = clean(
      raw?.with,
      50
    );

    if (!me || !other) return;

    socket.emit('history', {
      with: other,
      messages:
        privateHistory(me, other)
    });
  });

  /*
   * TEXT MESSAGE
   */

  socket.on('private-message', raw => {
    if (rateLimited(socket)) {
      return socket.emit(
        'message-error',
        {
          message:
            'Twòp mesaj. Tanpri tann yon ti moman.'
        }
      );
    }

    const from =
      socket.data.username;

    const to = clean(
      raw?.to,
      50
    );

    const text = clean(
      raw?.text,
      MAX_TEXT
    );

    if (
      !from ||
      !to ||
      !text ||
      from === to
    ) {
      return;
    }

    if (!byName.has(to)) {
      return socket.emit(
        'message-error',
        {
          message:
            'Kontak la pa online kounye a.'
        }
      );
    }

    const message = {
      id: createId(),
      kind: 'text',
      from,
      to,
      text,
      time:
        new Date().toISOString()
    };

    messages.push(message);

    while (
      messages.length >
      MAX_HISTORY
    ) {
      messages.shift();
    }

    socket.emit(
      'private-message',
      message
    );

    sendToUser(
      to,
      'private-message',
      message
    );
  });

  /*
   * IMAGE / VIDEO / VOICE MESSAGE
   */

  socket.on('media-message', raw => {
    if (rateLimited(socket)) {
      return socket.emit(
        'message-error',
        {
          message:
            'Twòp medya. Tanpri tann yon ti moman.'
        }
      );
    }

    const from =
      socket.data.username;

    const to = clean(
      raw?.to,
      50
    );

    const allowedKinds = [
      'image',
      'video',
      'voice'
    ];

    const kind =
      allowedKinds.includes(
        raw?.kind
      )
        ? raw.kind
        : 'voice';

    const data =
      String(raw?.data || '');

    if (
      !from ||
      !to ||
      !data ||
      !byName.has(to)
    ) {
      return socket.emit(
        'message-error',
        {
          message:
            'Kontak la pa online.'
        }
      );
    }

    if (
      Buffer.byteLength(
        data,
        'utf8'
      ) > MAX_MEDIA
    ) {
      return socket.emit(
        'message-error',
        {
          message:
            'Fichye a twò gwo.'
        }
      );
    }

    const message = {
      id: createId(),
      kind,
      from,
      to,
      data,
      duration:
        Number(
          raw?.duration || 0
        ),
      time:
        new Date().toISOString()
    };

    messages.push(message);

    while (
      messages.length >
      MAX_HISTORY
    ) {
      messages.shift();
    }

    socket.emit(
      'private-message',
      message
    );

    sendToUser(
      to,
      'private-message',
      message
    );
  });

  /*
   * TYPING
   */

  socket.on('typing', raw => {
    if (rateLimited(socket)) return;

    const from =
      socket.data.username;

    const to = clean(
      raw?.to,
      50
    );

    if (from && to) {
      sendToUser(
        to,
        'typing',
        { from }
      );
    }
  });

  socket.on('stop-typing', raw => {
    if (rateLimited(socket)) return;

    const from =
      socket.data.username;

    const to = clean(
      raw?.to,
      50
    );

    if (from && to) {
      sendToUser(
        to,
        'stop-typing',
        { from }
      );
    }
  });

  /*
   * 1-TO-1 WEBRTC SIGNALING
   *
   * Server la sèlman relay signal yo.
   * Audio/video pa pase nan server sa a.
   */

  const callEvents = [
    'call-offer',
    'call-answer',
    'ice-candidate',
    'call-reject',
    'call-end'
  ];

  for (const event of callEvents) {

    socket.on(event, raw => {

      if (rateLimited(socket)) {
        return;
      }

      const from =
        socket.data.username;

      const to = clean(
        raw?.to,
        50
      );

      if (
        !from ||
        !to ||
        !byName.has(to)
      ) {
        if (
          event ===
          'call-offer'
        ) {
          socket.emit(
            'call-error',
            {
              message:
                'Kontak la pa online kounye a.'
            }
          );
        }

        return;
      }

      const packet = {
        ...raw,
        from,
        to
      };

      delete packet.sender;

      sendToUser(
        to,
        event,
        packet
      );
    });
  }

  /*
   * GROUP CALL CREATE
   */

  socket.on(
    'group-call-create',
    raw => {

      const from =
        socket.data.username;

      const members =
        Array.isArray(
          raw?.members
        )
          ? raw.members
              .map(
                username =>
                  clean(
                    username,
                    50
                  )
              )
              .filter(Boolean)
          : [];

      if (!from) return;

      const roomId =
        createId();

      const unique = [
        ...new Set([
          from,
          ...members
        ])
      ];

      socket.join(
        `group:${roomId}`
      );

      for (
        const username
        of unique
      ) {

        if (
          username !== from &&
          byName.has(username)
        ) {
          sendToUser(
            username,
            'group-call-invite',
            {
              roomId,
              from,
              members: unique
            }
          );
        }
      }

      socket.emit(
        'group-call-created',
        {
          roomId,
          members: unique
        }
      );
    }
  );

  /*
   * GROUP CALL JOIN
   */

  socket.on(
    'group-call-join',
    raw => {

      const from =
        socket.data.username;

      const roomId = clean(
        raw?.roomId,
        100
      );

      if (
        !from ||
        !roomId
      ) {
        return;
      }

      socket.join(
        `group:${roomId}`
      );

      socket
        .to(`group:${roomId}`)
        .emit(
          'group-peer-joined',
          {
            roomId,
            username: from
          }
        );

      const room =
        io.sockets.adapter.rooms.get(
          `group:${roomId}`
        ) || new Set();

      const peers = [];

      for (
        const socketId
        of room
      ) {

        const username =
          users.get(
            socketId
          )?.username;

        if (
          username &&
          username !== from
        ) {
          peers.push(username);
        }
      }

      socket.emit(
        'group-peers',
        {
          roomId,
          peers
        }
      );
    }
  );

  /*
   * GROUP CALL SIGNAL
   */

  socket.on(
    'group-signal',
    raw => {

      const from =
        socket.data.username;

      const to = clean(
        raw?.to,
        50
      );

      const roomId = clean(
        raw?.roomId,
        100
      );

      if (
        !from ||
        !to ||
        !roomId ||
        !byName.has(to)
      ) {
        return;
      }

      sendToUser(
        to,
        'group-signal',
        {
          ...raw,
          from,
          to,
          roomId
        }
      );
    }
  );

  /*
   * GROUP CALL LEAVE
   */

  socket.on(
    'group-call-leave',
    raw => {

      const from =
        socket.data.username;

      const roomId = clean(
        raw?.roomId,
        100
      );

      if (roomId) {
        socket.leave(
          `group:${roomId}`
        );
      }

      if (
        from &&
        roomId
      ) {
        socket
          .to(`group:${roomId}`)
          .emit(
            'group-peer-left',
            {
              roomId,
              username: from
            }
          );
      }
    }
  );

  /*
   * DISCONNECT
   */

  socket.on(
    'disconnect',
    () => {

      eventWindows.delete(
        socket.id
      );

      if (socket.data.ip) {

        const remaining =
          Math.max(
            0,
            (
              ipConnections.get(
                socket.data.ip
              ) || 1
            ) - 1
          );

        if (remaining) {
          ipConnections.set(
            socket.data.ip,
            remaining
          );
        } else {
          ipConnections.delete(
            socket.data.ip
          );
        }
      }

      const username =
        socket.data.username;

      if (
        username &&
        byName.get(username) ===
          socket.id
      ) {
        byName.delete(
          username
        );
      }

      users.delete(
        socket.id
      );

      broadcastPresence();
    }
  );
});

server.listen(
  PORT,
  '0.0.0.0',
  () => {
    console.log(
      `ZIQVONA running on port ${PORT}`
    );
  }
);
