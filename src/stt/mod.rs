pub mod deepgram;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::protocol::AudioFormat;

#[derive(Debug, Clone)]
pub struct TranscriptEvent {
    pub text: String,
    pub is_final: bool,
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub format: AudioFormat,
    pub sample_rate: u32,
    pub channels: u8,
    pub language: Option<String>,
}

pub struct SttStream {
    pub audio_tx: mpsc::Sender<Vec<u8>>,
    pub transcript_rx: mpsc::Receiver<TranscriptEvent>,
}

#[async_trait]
pub trait SttProvider: Send + Sync {
    async fn start_stream(&self, config: StreamConfig) -> anyhow::Result<SttStream>;
    fn name(&self) -> &str;
}
