use crate::relay::SessionEvent;
use crate::types::AgentEvent;
use crate::daemon::DaemonHandle;

impl DaemonHandle {
    pub(crate) fn sanitize_history_event(event: SessionEvent) -> SessionEvent {
        match event {
            SessionEvent::AgentUpdate {
                thread_id,
                acp_session_id,
                event: AgentEvent::Finished { content },
            } => SessionEvent::AgentUpdate {
                thread_id,
                acp_session_id,
                event: AgentEvent::Finished {
                    content: crate::session::normalize_stop_reason_token(&content),
                },
            },
            other => other,
        }
    }

    pub(crate) fn sanitize_history(events: Vec<SessionEvent>) -> Vec<SessionEvent> {
        events
            .into_iter()
            .map(Self::sanitize_history_event)
            .collect()
    }

    pub(crate) fn generate_two_words() -> String {
        const ADJECTIVES: &[&str] = &[
            "swift", "clever", "vibrant", "silent", "golden", "hyper", "stellar", "bright",
            "sleek", "agile", "zen", "iron", "quantum", "cyber", "rapid", "solar", "bold", "cool",
            "epic", "grand", "lunar", "neon", "prime", "sonic",
        ];
        const NOUNS: &[&str] = &[
            "eagle", "fox", "panda", "owl", "tiger", "wave", "storm", "pulse", "orbit", "spark",
            "edge", "forge", "core", "nexus", "link", "zenith", "hawk", "wolf", "lion", "bear",
            "crest", "flux", "nova", "shift",
        ];

        let u = uuid::Uuid::new_v4().as_u128();
        let adj = ADJECTIVES[(u & 0xFF) as usize % ADJECTIVES.len()];
        let noun = NOUNS[((u >> 8) & 0xFF) as usize % NOUNS.len()];
        format!("{} {}", adj, noun)
    }
}
