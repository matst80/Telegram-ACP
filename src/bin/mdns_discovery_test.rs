use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent};

fn main() -> Result<()> {
    let mdns = ServiceDaemon::new()?;
    let receiver = mdns.browse("_acp-ws._tcp.local.")?;

    println!("Scanning for local ACP WebSocket servers over mDNS...");
    println!("Press Ctrl+C to exit.");

    while let Ok(event) = receiver.recv() {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                let port = info.get_port();
                let addresses = info.get_addresses();
                println!("Service resolved:");
                println!("  Fullname:  {}", info.get_fullname());
                println!("  Addresses: {:?}", addresses);
                println!("  Port:      {}", port);
                let props = info.get_properties();
                println!("  Properties: {:?}", props);
                println!("---");
            }
            ServiceEvent::ServiceRemoved(_type, fullname) => {
                println!("Service removed: {}", fullname);
            }
            _ => {}
        }
    }

    Ok(())
}
