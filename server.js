const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 20 * 1024 * 1024
});

const PORT = process.env.PORT || 10000;

const MAX_USERS = 1200;
const MAX_TEXT = 4000;
const MAX_MEDIA = 12 * 1024 * 1024;
const MAX_HISTORY = 5000;
const MAX_STATUS = 120;
const MAX_AVATAR = 2 * 1024 * 1024;

const users = new Map();
const byName = new Map();
const profiles = new Map();
const messages = [];

// Mesaj ki fèt pandan moun nan offline.
// Yo rete nan RAM server la jiskaske moun nan rekonekte.
const pendingDeliveries = new Map();

app.use(express.json({ limit: '20mb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    app: 'ZIQVONA',
    onlineUsers: users.size
  });
});

app.get('/config', (req, res) => {
  res.json({
    iceServers: [
      {
        urls: [
          'stun:stun.l.google.com:19302',
          'stun:stun1.l.google.com:19302'
        ]
      },

      ...(process.env.TURN_URL
        ? [{
            urls: process.env.TURN_URL
              .split(',')
              .map(x => x.trim())
              .filter(Boolean),

            username: process.env.TURN_USERNAME || '',
            credential: process.env.TURN_CREDENTIAL || ''
          }]
        : [])
    ]
  });
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

function clean(v, max = 1000) {
  return String(v ?? '').trim().slice(0, max);
}

function id() {
  return `${Date.now()}-${Math.random()
    .toString(36)
    .slice(2)}-${Math.random()
    .toString(36)
    .slice(2)}`;
}

function publicProfile(username) {
  const p = profiles.get(username) || {};

  return {
    username,
    displayName: p.displayName || username,
    status: p.status || 'Disponib',
    avatar: p.avatar || '',
    online: byName.has(username)
  };
}

function allProfiles() {
  const out = {};

  profiles.forEach((_, username) => {
    out[username] = publicProfile(username);
  });

  return out;
}

function presence() {
  io.emit('presence', {
    online: [...byName.keys()],
    profiles: allProfiles()
  });
}

function sendToUser(username, event, data) {
  const sid = byName.get(username);

  if (sid) {
    io.to(sid).emit(event, data);
  }

  return !!sid;
}

function privateHistory(a, b) {
  return messages
    .filter(m =>
      (m.from === a && m.to === b) ||
      (m.from === b && m.to === a)
    )
    .slice(-200);
}

io.on('connection', socket => {

  // ==============================
  // REGISTER USER
  // ==============================

  socket.on('register', raw => {
    const username = clean(
      raw?.username || raw?.name,
      40
    );

    if (!username || username.length < 2) {
      return socket.emit('error-message', {
        message: 'Non an dwe gen omwen 2 karaktè.'
      });
    }

    const oldSocketId = byName.get(username);

    if (oldSocketId && oldSocketId !== socket.id) {
      io.to(oldSocketId).emit('force-disconnect', {
        message: 'Kont sa a konekte sou yon lòt aparèy.'
      });

      io.sockets.sockets
        .get(oldSocketId)
        ?.disconnect(true);

      users.delete(oldSocketId);
    }

    const old = profiles.get(username) || {};

    const user = {
      id: socket.id,
      username,

      displayName: clean(
        raw?.displayName ||
        raw?.name ||
        old.displayName ||
        username,
        50
      ),

      status: clean(
        raw?.status ||
        old.status ||
        'Disponib',
        MAX_STATUS
      ),

      avatar: clean(
        raw?.avatar ||
        old.avatar ||
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

    socket.emit('registered', {
      me: publicProfile(username),
      profiles: allProfiles(),
      online: [...byName.keys()]
    });

    // ==========================================
    // LIVRE MESAJ KI T AP TANN POU USER LA
    // ==========================================

    const waiting = pendingDeliveries.get(username) || [];

    if (waiting.length) {
      for (const msg of waiting) {
        socket.emit('private-message', msg);
      }

      pendingDeliveries.delete(username);
    }

    presence();
  });


  // ==============================
  // UPDATE PROFILE
  // ==============================

  socket.on('update-profile', raw => {
    const username = socket.data.username;

    if (!username) return;

    const old = profiles.get(username) || {};

    const p = {
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

    profiles.set(username, p);

    const u = users.get(socket.id);

    if (u) {
      Object.assign(u, p);
    }

    socket.emit(
      'profile-updated',
      publicProfile(username)
    );

    presence();
  });


  // ==============================
  // SEARCH USERS
  // ==============================

  socket.on('search-users', raw => {
    const me = socket.data.username || '';

    const q = clean(
      raw?.query,
      50
    ).toLowerCase();

    const results = [...profiles.keys()]
      .filter(u => u !== me)
      .map(publicProfile)
      .filter(p =>
        !q ||
        p.username.toLowerCase().includes(q) ||
        p.displayName.toLowerCase().includes(q)
      )
      .slice(0, 100);

    socket.emit(
      'search-results',
      results
    );
  });


  // ==============================
  // GET CHAT HISTORY
  // ==============================

  socket.on('get-history', raw => {
    const me = socket.data.username;
    const other = clean(raw?.with, 50);

    if (!me || !other) return;

    socket.emit('history', {
      with: other,
      messages: privateHistory(me, other)
    });
  });


  // ==============================
  // PRIVATE TEXT MESSAGE
  // ==============================

  socket.on('private-message', raw => {

    const from = socket.data.username;

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

    // clientId pèmèt ZIQVONA rekonèt
    // menm mesaj la lè li soti nan offline queue.
    const msg = {
      id: id(),

      clientId: clean(
        raw?.clientId,
        100
      ),

      kind: 'text',

      from,
      to,
      text,

      time: new Date().toISOString()
    };

    // Kenbe mesaj la nan history server la.
    messages.push(msg);

    while (
      messages.length >
      MAX_HISTORY
    ) {
      messages.shift();
    }

    // Konfime mesaj la bay moun ki voye l.
    socket.emit(
      'private-message',
      msg
    );

    // Si moun k ap resevwa a online,
    // voye mesaj la imedyatman.
    if (!sendToUser(
      to,
      'private-message',
      msg
    )) {

      // Si li offline,
      // mete mesaj la nan queue.
      const queue =
        pendingDeliveries.get(to) || [];

      queue.push(msg);

      // Pa kite queue a vin twò gwo.
      while (
        queue.length > 200
      ) {
        queue.shift();
      }

      pendingDeliveries.set(
        to,
        queue
      );
    }
  });


  // ==============================
  // MEDIA MESSAGE
  // ==============================

  socket.on('media-message', raw => {

    const from =
      socket.data.username;

    const to = clean(
      raw?.to,
      50
    );

    const kind =
      raw?.kind === 'video'
        ? 'video'
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

    const msg = {
      id: id(),
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

    messages.push(msg);

    while (
      messages.length >
      MAX_HISTORY
    ) {
      messages.shift();
    }

    socket.emit(
      'private-message',
      msg
    );

    sendToUser(
      to,
      'private-message',
      msg
    );
  });


  // ==============================
  // TYPING
  // ==============================

  socket.on('typing', raw => {

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


  // ==============================
  // WEBRTC CALL SIGNALING
  // ==============================

  for (
    const event of [
      'call-offer',
      'call-answer',
      'ice-candidate',
      'call-reject',
      'call-end'
    ]
  ) {

    socket.on(event, raw => {

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
          event === 'call-offer'
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


  // ==============================
  // GROUP CALL
  // ==============================

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
              .map(x =>
                clean(x, 50)
              )
              .filter(Boolean)
          : [];

      if (!from) return;

      const roomId = id();

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
        const username of unique
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


  socket.on(
    'group-call-join',
    raw => {

      const from =
        socket.data.username;

      const roomId =
        clean(
          raw?.roomId,
          100
        );

      if (
        !from ||
        !roomId
      ) return;

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
        const sid of room
      ) {

        const u =
          users.get(sid)
            ?.username;

        if (
          u &&
          u !== from
        ) {
          peers.push(u);
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


  socket.on(
    'group-signal',
    raw => {

      const from =
        socket.data.username;

      const to =
        clean(raw?.to, 50);

      const roomId =
        clean(
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


  socket.on(
    'group-call-leave',
    raw => {

      const from =
        socket.data.username;

      const roomId =
        clean(
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


  // ==============================
  // DISCONNECT
  // ==============================

  socket.on(
    'disconnect',
    () => {

      const username =
        socket.data.username;

      if (
        username &&
        byName.get(username) ===
          socket.id
      ) {
        byName.delete(username);
      }

      users.delete(
        socket.id
      );

      presence();
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
