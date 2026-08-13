const WebSocket = require('ws');

const ws = new WebSocket('ws://127.0.0.1:9001');

ws.on('open', function open() {
  // console.log('Connected');
});

ws.on('message', function incoming(data) {
  const event = JSON.parse(data);
  if (event.type === 'snapshot') {
    // Print the full raw JSON of the snapshot event
    console.log(JSON.stringify(event, null, 2));
    process.exit(0);
  }
});

ws.on('error', function error(err) {
  console.error('Error:', err);
  process.exit(1);
});

setTimeout(() => {
  process.exit(1);
}, 5000);
