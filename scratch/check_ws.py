import asyncio
import websockets
import json
import sys

async def check_snapshot():
    uri = "ws://127.0.0.1:9001"
    try:
        async with websockets.connect(uri) as websocket:
            print(f"Connected to {uri}")
            # The first message should be the snapshot
            message = await websocket.recv()
            data = json.loads(message)
            
            if data.get("type") == "snapshot":
                print("Received Snapshot:")
                # Check for history in sessions
                sessions = data.get("sessions", [])
                print(f"Found {len(sessions)} sessions.")
                for i, s in enumerate(sessions):
                    history = s.get("history", [])
                    print(f"Session {i}: ID={s.get('acp_session_id')}, History Length={len(history)}")
                    if history:
                        print(f"  First event type: {history[0].get('type')}")
                
                # Output the whole snapshot for inspection if small, otherwise just summary
                # print(json.dumps(data, indent=2))
            else:
                print(f"Received unexpected event type: {data.get('type')}")
                print(json.dumps(data, indent=2))
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(check_snapshot())
