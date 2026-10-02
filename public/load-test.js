const { io } = require("socket.io-client");

const SERVER =
  process.env.SERVER ||
  "http://localhost:10000";

const TOTAL =
  Number(
    process.env.USERS || 100
  );

const connections = [];

let connected = 0;
let failed = 0;

console.log(
  `ZIQVONA load test: ${TOTAL} users`
);

for (
  let i = 0;
  i < TOTAL;
  i++
) {

  const username =
    `loadtest_${i}`;

  const socket =
    io(SERVER, {
      transports: [
        "websocket"
      ],

      reconnection: false,

      timeout: 10000
    });

  connections.push(
    socket
  );

  socket.on(
    "connect",
    () => {

      connected++;

      socket.emit(
        "register",
        {
          username,

          displayName:
            username,

          status:
            "Load Test"
        }
      );

      if (
        connected + failed >=
        TOTAL
      ) {
        finish();
      }
    }
  );

  socket.on(
    "connect_error",
    error => {

      failed++;

      console.error(
        username,
        error.message
      );

      if (
        connected + failed >=
        TOTAL
      ) {
        finish();
      }
    }
  );
}

function finish() {

  console.log(
    `Connected: ${connected}`
  );

  console.log(
    `Failed: ${failed}`
  );

  setTimeout(
    () => {

      for (
        const socket
        of connections
      ) {
        socket.disconnect();
      }

      process.exit(
        failed > 0
          ? 1
          : 0
      );

    },
    3000
  );
}
