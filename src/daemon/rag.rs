use crate::config::Config;

pub async fn run_rag_registration(config: Config, actual_port: u16) {
    let url = match config.rag_register_url.as_ref() {
        Some(u) => u,
        None => return,
    };
    let token = match config.rag_token.as_ref() {
        Some(t) => t,
        None => {
            tracing::warn!("RAG registration URL provided but no token");
            return;
        }
    };
    let name = config.rag_register_name.clone().unwrap_or_else(|| {
        let raw_hostname = if let Ok(output) = std::process::Command::new("hostname").output() {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        } else {
            format!("acp-ws-{}", actual_port)
        };
        if raw_hostname.is_empty() {
            format!("acp-ws-{}", actual_port)
        } else {
            raw_hostname
        }
    });
    let host = config
        .rag_register_host
        .clone()
        .unwrap_or_else(|| crate::websocket::get_local_ip());

    let client = reqwest::Client::new();

    // Heartbeat URL: replace /register with /heartbeat
    let heartbeat_url = url.replace("/register", "/heartbeat");

    let register_payload = serde_json::json!({
        "name": name,
        "host": host,
        "port": actual_port,
    });

    let heartbeat_payload = serde_json::json!({
        "name": name,
    });

    loop {
        tracing::info!(
            url = %url,
            name = %name,
            host = %host,
            port = actual_port,
            payload = ?register_payload,
            "Sending registration request to remote RAG"
        );
        let res = client
            .post(url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&register_payload)
            .send()
            .await;

        match res {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!("Successfully registered with remote RAG");

                // Heartbeat loop
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                    tracing::info!(
                        url = %heartbeat_url,
                        payload = ?heartbeat_payload,
                        "Sending heartbeat to remote RAG"
                    );
                    let res = client
                        .post(&heartbeat_url)
                        .header("Authorization", format!("Bearer {}", token))
                        .json(&heartbeat_payload)
                        .send()
                        .await;

                    match res {
                        Ok(resp) if resp.status().is_success() => {
                            #[derive(serde::Deserialize)]
                            struct HeartbeatResp {
                                #[serde(default)]
                                refreshed: bool,
                            }
                            let body = resp.text().await.unwrap_or_default();
                            let refreshed = serde_json::from_str::<HeartbeatResp>(&body)
                                .map(|r| r.refreshed)
                                .unwrap_or(false);
                            if !refreshed {
                                tracing::warn!(
                                    body = %body,
                                    "RAG heartbeat returned refreshed=false, re-registering"
                                );
                                break;
                            }
                            tracing::info!("RAG heartbeat sent successfully");
                        }
                        Ok(resp) => {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            tracing::warn!(
                                status = %status,
                                body = %body,
                                "RAG heartbeat failed, re-registering"
                            );
                            break; // break inner loop to re-register
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "RAG heartbeat error, re-registering");
                            break; // break inner loop to re-register
                        }
                    }
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(
                    status = %status,
                    body = %body,
                    "Failed to register with remote RAG, retrying in 30s"
                );
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "Error registering with remote RAG, retrying in 30s"
                );
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }
}
