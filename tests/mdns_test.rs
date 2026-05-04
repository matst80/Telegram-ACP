use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::collections::HashMap;
use std::time::Duration;

#[test]
fn test_mdns_advertise_and_discover() -> Result<()> {
    let mdns = ServiceDaemon::new()?;
    let service_type = "_acp-ws._tcp.local.";
    let instance_name = "test-ws-9005";
    let mut properties = HashMap::new();
    properties.insert("version".to_string(), "1.0".to_string());

    let local_ip = if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                addr.ip().to_string()
            } else {
                "127.0.0.1".to_string()
            }
        } else {
            "127.0.0.1".to_string()
        }
    } else {
        "127.0.0.1".to_string()
    };

    let service_info = ServiceInfo::new(
        service_type,
        instance_name,
        "localhost.local.",
        &local_ip,
        9005,
        Some(properties),
    )?;

    mdns.register(service_info)?;

    let mdns_browser = ServiceDaemon::new()?;
    let receiver = mdns_browser.browse(service_type)?;
    let start = std::time::Instant::now();
    let mut resolved = false;

    while start.elapsed() < Duration::from_secs(10) {
        if let Ok(event) = receiver.recv_timeout(Duration::from_millis(500)) {
            println!("Received event: {:?}", event);
            if let ServiceEvent::ServiceResolved(info) = event {
                if info.get_fullname().contains(instance_name) {
                    assert_eq!(info.get_port(), 9005);
                    resolved = true;
                    break;
                }
            }
        }
    }

    assert!(resolved, "Should resolve the advertised service within 10 seconds");
    Ok(())
}
