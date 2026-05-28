use anyhow::Result;
use serde_json::Value as JsonValue;
use rmcp::service::RxJsonRpcMessage;
use rmcp::RoleServer;
use crate::daemon::DaemonHandle;

impl DaemonHandle {
    pub async fn handle_mcp_message(
        &self,
        session_id: &str,
        payload: &str,
    ) -> Result<Option<String>> {
        tracing::debug!(session_id, "handle_mcp_message: looking up session");
        let mcp_session = self
            .session_manager
            .get_mcp_session_by_id(session_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown MCP session id: {}", session_id))?;

        let payload_value: JsonValue = serde_json::from_str(payload)
            .map_err(|e| anyhow::anyhow!("Invalid MCP payload: {e}"))?;
        let expects_response = crate::mcp::mcp_expects_response(&payload_value);
        tracing::debug!(
            session_id,
            expects_response,
            payload = %payload,
            "handle_mcp_message: routing message"
        );
        let message: RxJsonRpcMessage<RoleServer> = serde_json::from_value(payload_value)
            .map_err(|e| anyhow::anyhow!("Failed to decode MCP payload: {e}"))?;

        mcp_session.send(message).await?;
        tracing::debug!(session_id, "handle_mcp_message: message sent to McpSession");

        if expects_response {
            tracing::debug!(session_id, "handle_mcp_message: waiting for response");
            if let Some(response) = mcp_session.next_response().await {
                let payload = serde_json::to_string(&response)?;
                tracing::debug!(session_id, response = %payload, "handle_mcp_message: got response");
                Ok(Some(payload))
            } else {
                tracing::warn!(
                    session_id,
                    "handle_mcp_message: outgoing channel closed while waiting for response"
                );
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
