const WebSocket = require('ws');

const ws = new WebSocket('ws://127.0.0.1:9001');

ws.on('open', function open() {
  console.log('Connected to ws://127.0.0.1:9001');
  
  const command = {
    type: 'send_prompt',
    session_id: 'f034b030-1db9-48c7-a5e4-0b1a54a3a289',
    text: 'Hello'
  };
  ws.send(JSON.stringify(command));
});

let messageCount = 0;
ws.on('message', function incoming(data) {
  const event = JSON.parse(data);
  messageCount++;
  
  if (event.type === 'snapshot') {
      console.log(`Snapshot received (count ${messageCount}):`);
      const s = event.sessions.find(s => s.acp_session_id === 'f034b030-1db9-48c7-a5e4-0b1a54a3a289');
      if (s) {
          console.log(`History length for session f034b030...: ${s.history.length}`);
          s.history.forEach((h, i) => {
              let detail = '';
              if (h.type === 'user_prompt') detail = `: ${h.text}`;
              if (h.type === 'agent_update') {
                  if (h.event && h.event.update) {
                      const update = h.event.update;
                      if (update.type === 'agent_message_chunk') detail = `: chunk(${update.chunk.text})`;
                      else detail = `: ${update.type}`;
                  }
              }
              console.log(`  [${i}] ${h.type}${detail}`);
          });
      }
      
      if (messageCount > 1) process.exit(0);
      else {
          console.log('Waiting for updates...');
          setTimeout(() => {
              ws.send(JSON.stringify({ type: 'list_sessions' }));
          }, 2000);
      }
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
