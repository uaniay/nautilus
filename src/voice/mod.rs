use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::protocol::ServerMessage;
use crate::session::SessionManager;
use crate::stt::{SttProvider, SttStream, StreamConfig};
use crate::protocol::AudioFormat;
use crate::config::SttConfig;

pub struct VoiceSession {
    pub audio_tx: mpsc::Sender<Vec<u8>>,
}

pub struct VoiceManager {
    sessions: HashMap<String, VoiceSession>,
    stt_provider: Arc<dyn SttProvider>,
    stt_config: SttConfig,
}

impl VoiceManager {
    pub fn new(stt_provider: Arc<dyn SttProvider>, stt_config: SttConfig) -> Self {
        Self {
            sessions: HashMap::new(),
            stt_provider,
            stt_config,
        }
    }

    pub async fn start(
        &mut self,
        session_id: String,
        format: AudioFormat,
        sample_rate: u32,
        channels: u8,
        session_mgr: SessionManager,
        ws_tx: mpsc::UnboundedSender<ServerMessage>,
    ) -> anyhow::Result<()> {
        if self.sessions.contains_key(&session_id) {
            anyhow::bail!("Voice session already active for {}", session_id);
        }

        let stream_config = StreamConfig {
            format,
            sample_rate,
            channels,
            language: self.stt_config.language.clone(),
        };

        let stt_stream = self.stt_provider.start_stream(stream_config).await?;
        let SttStream { audio_tx, mut transcript_rx } = stt_stream;

        let voice_session = VoiceSession {
            audio_tx,
        };
        self.sessions.insert(session_id.clone(), voice_session);

        let auto_submit = self.stt_config.auto_submit;
        let sid = session_id.clone();
        tokio::spawn(async move {
            while let Some(event) = transcript_rx.recv().await {
                if event.is_final && !event.text.is_empty() {
                    let mut text = event.text.clone();
                    if auto_submit {
                        text.push('\n');
                    }
                    if let Err(e) = session_mgr.write(&sid, text.into_bytes()) {
                        warn!(session_id = %sid, "Failed to write transcript to PTY: {}", e);
                    }
                }

                let _ = ws_tx.send(ServerMessage::VoiceTranscript {
                    session_id: sid.clone(),
                    text: event.text,
                    is_final: event.is_final,
                });
            }

            let _ = ws_tx.send(ServerMessage::VoiceStatus {
                session_id: sid,
                active: false,
            });
        });

        info!(session_id = %session_id, "Voice session started");
        Ok(())
    }

    pub async fn feed_audio(&self, session_id: &str, data: Vec<u8>) -> anyhow::Result<()> {
        let session = self.sessions.get(session_id)
            .ok_or_else(|| anyhow::anyhow!("No active voice session for {}", session_id))?;

        session.audio_tx.send(data).await
            .map_err(|_| anyhow::anyhow!("Voice session audio channel closed"))?;

        Ok(())
    }

    pub fn stop(&mut self, session_id: &str) {
        if let Some(session) = self.sessions.remove(session_id) {
            drop(session.audio_tx);
            info!(session_id = %session_id, "Voice session stopped");
        }
    }

    pub fn cleanup(&mut self) {
        self.sessions.clear();
    }
}
