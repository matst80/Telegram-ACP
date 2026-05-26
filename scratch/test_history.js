const WebSocket = require('ws');

const ws = new WebSocket('ws://127.0.0.1:9001');

ws.on('open', function open() {
  console.log('Connected to ws://127.0.0.1:9001');
  
  // Send a prompt
  const command = {
    type: 'send_prompt',
    session_id: 'f034b030-1db9-48c7-a5e4-0b1a54a3a289',
    text: 'Hello, are you there?'
  };
  ws.send(JSON.stringify(command));
  console.log('Sent prompt command');
});

let messageCount = 0;
ws.on('message', function incoming(data) {
  const event = JSON.parse(data);
  console.log(`Received event ${++messageCount}:`, event.type);
  
  if (messageCount === 1 && event.type === 'snapshot') {
      console.log('Initial history length:', event.sessions[0].history.length);
  }
  
  // After sending a prompt, we expect some updates and then maybe we can request a list_sessions to see the updated snapshot
  if (messageCount === 5) { // Arbitrary number to wait for some updates
      console.log('Requesting session list...');
      ws.send(JSON.stringify({ type: 'list_sessions' }));
  }
  
  if (messageCount > 1 && event.type === 'snapshot') {
      console.log('Updated Snapshot history length:', event.sessions[0].history.length);
      event.sessions[0].history.forEach((h, i) => {
          console.log(`  Event ${i}: ${h.type} ${h.text ? '(' + h.text + ')' : ''}`);
      });
      process.exit(0);
  }
});

ws.on('error', function error(err) {
  console.error('Error:', err);
  process.exit(1);
});

setTimeout(() => {
  console.log('Timeout');
  process.exit(1);
}, 10000);
