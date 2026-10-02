'use strict';

const { io } = require('socket.io-client');

const SERVER =
  process.env.SERVER ||
  'http://localhost:10000';

const USERS =
  Number(process.env.USERS || 100);

const CONNECTION_TIMEOUT =
  Number(process.env.TIMEOUT || 15000);

if (
  !Number.isInteger(USERS) ||
  USERS < 1 ||
  USERS > 10000
) {
  console.error(
    'USERS dwe yon nimewo ant 1 ak 10000.'
  );

  process.exit(1);
}

let connected = 0;
let failed = 0;
let finished = 0;

const sockets = [];

const startedAt = Date.now();

function finishOne() {
  finished += 1;

  if (finished !== USERS) {
    return;
  }

  const elapsed =
    ((Date.now() - startedAt) / 1000)
      .toFixed(2);

  console.log('');
  console.log('================================');
  console.log('ZIQVONA LOAD TEST FINISHED');
  console.log('================================');
  console.log(`Server:    ${SERVER}`);
  console.log(`Users:     ${USERS}`);
  console.log(`Connected: ${connected}`);
  console.log(`Failed:    ${failed}`);
  console.log(`Time:      ${elapsed}s`);
  console.log('================================');

  for (const socket of sockets) {
    try {
      socket.disconnect();
    } catch (_) {}
  }

  process.exit(
    failed > 0 ? 1 : 0
  );
}

function createUser(index) {
  const username =
    `loadtest_${Date.now()}_${index}`;

  const socket = io(
    SERVER,
    {
      transports: ['websocket'],
      reconnection: false,
      timeout: CONNECTION_TIMEOUT
    }
  );

  sockets.push(socket);

  let done = false;

  const timeout = setTimeout(() => {
    if (done) {
      return;
    }

    done = true;
    failed += 1;

    console.error(
      `[${index}] TIMEOUT`
    );

    try {
      socket.disconnect();
    } catch (_) {}

    finishOne();
  }, CONNECTION_TIMEOUT + 1000);

  socket.on(
    'connect',
    () => {
      console.log(
        `[${index}] connected: ${socket.id}`
      );

      socket.emit(
        'register',
        {
          username,
          displayName:
            `Load User ${index}`,
          status:
            'ZIQVONA load test'
        }
      );
    }
  );

  socket.on(
    'registered',
    data => {
      if (done) {
        return;
      }

      done = true;
      clearTimeout(timeout);

      connected += 1;

      console.log(
        `[${index}] registered: ${
          data?.me?.username ||
          username
        }`
      );

      finishOne();
    }
  );

  socket.on(
    'connect_error',
    error => {
      if (done) {
        return;
      }

      done = true;
      clearTimeout(timeout);

      failed += 1;

      console.error(
        `[${index}] connection error: ${
          error?.message ||
          'unknown error'
        }`
      );

      try {
        socket.disconnect();
      } catch (_) {}

      finishOne();
    }
  );

  socket.on(
    'error-message',
    data => {
      console.error(
        `[${index}] server error: ${
          data?.message ||
          'unknown error'
        }`
      );
    }
  );

  socket.on(
    'disconnect',
    reason => {
      if (!done) {
        return;
      }

      console.log(
        `[${index}] disconnected: ${reason}`
      );
    }
  );
}

console.log('');
console.log('================================');
console.log('ZIQVONA LOAD TEST');
console.log('================================');
console.log(`Server: ${SERVER}`);
console.log(`Users:  ${USERS}`);
console.log(
  `Timeout: ${CONNECTION_TIMEOUT}ms`
);
console.log('================================');
console.log('');

for (
  let i = 1;
  i <= USERS;
  i++
) {
  createUser(i);
}
