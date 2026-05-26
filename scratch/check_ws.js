const WebSocket = require('ws');

const ws = new WebSocket('ws://127.0.0.1:9001');

ws.on('open', function open() {
  console.log('Connected to ws://127.0.0.1:9001');
});

ws.on('message', function incoming(data) {
  const event = JSON.parse(data);
  if (event.type === 'snapshot') {
    console.log('Received Snapshot:');
    console.log(`Sessions count: ${event.sessions.length}`);
    event.sessions.forEach((s, i) => {
      console.log(`Session ${i}: ID=${s.acp_session_id}, History Length=${s.history ? s.history.length : 'undefined'}`);
      if (s.history && s.history.length > 0) {
          console.log(`  First event: ${JSON.stringify(s.history[0]).substring(0, 100)}...`);
      }
    });
    process.exit(0);
  } else {
    console.log('Received event:', event.type);
  }
});

ws.on('error', function error(err) {
  console.error('Error:', err);
  process.exit(1);
});

setTimeout(() => {
  console.log('Timeout waiting for snapshot');
  process.exit(1);
}, 5000);
