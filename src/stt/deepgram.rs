use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error};

use crate::config::SttConfig;
use crate::protocol::AudioFormat;
use super::{SttProvider, SttStream, StreamConfig, TranscriptEvent};

pub struct DeepgramProvider {
    api_key: String,
    model: String,
    language: String,
    endpoint: String,
}

impl DeepgramProvider {
    pub fn new(config: &SttConfig) -> Self {
        Self {
            api_key: config.api_key.clone(),
            model: config.model.clone().unwrap_or_else(|| "nova-2".to_string()),
            language: config.language.clone().unwrap_or_else(|| "en".to_string()),
            endpoint: config.endpoint.clone().unwrap_or_else(|| "wss://api.deepgram.com/v1/listen".to_string()),
        }
    }

    fn build_url(&self, config: &StreamConfig) -> String {
        let encoding = match config.format {
            AudioFormat::Pcm16 => "linear16",
            AudioFormat::Opus => "opus",
        };

        let language = config.language.as_deref().unwrap_or(&self.language);

        format!(
            "{}?model={}&language={}&encoding={}&sample_rate={}&channels={}&interim_results=true&punctuate=true",
            self.endpoint, self.model, language, encoding, config.sample_rate, config.channels
        )
    }
}

#[derive(Debug, Deserialize)]
struct DeepgramResponse {
    channel: Option<DeepgramChannel>,
    is_final: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DeepgramChannel {
    alternatives: Vec<DeepgramAlternative>,
}

#[derive(Debug, Deserialize)]
struct DeepgramAlternative {
    transcript: String,
}

#[async_trait]
impl SttProvider for DeepgramProvider {
    async fn start_stream(&self, config: StreamConfig) -> anyhow::Result<SttStream> {
        let url = self.build_url(&config);
        let request = tokio_tungstenite::tungstenite::http::Request::builder()
            .uri(&url)
            .header("Authorization", format!("Token {}", self.api_key))
            .header("Host", "api.deepgram.com")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .body(())?;

        let (ws_stream, _) = connect_async(request).await?;
        let (mut ws_sink, mut ws_source) = ws_stream.split();

        let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<u8>>(64);
        let (transcript_tx, transcript_rx) = mpsc::channel::<TranscriptEvent>(32);

        // Forward audio chunks to Deepgram
        tokio::spawn(async move {
            while let Some(data) = audio_rx.recv().await {
                if ws_sink.send(Message::Binary(data.into())).await.is_err() {
                    break;
                }
            }
            // Signal end of audio
            let close_msg = serde_json::json!({"type": "CloseStream"}).to_string();
            let _ = ws_sink.send(Message::Text(close_msg.into())).await;
            let _ = ws_sink.close().await;
        });

        // Read transcript events from Deepgram
        tokio::spawn(async move {
            while let Some(msg) = ws_source.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        let text_str: &str = &text;
                        match serde_json::from_str::<DeepgramResponse>(text_str) {
                            Ok(resp) => {
                                if let Some(channel) = resp.channel {
                                    if let Some(alt) = channel.alternatives.first() {
                                        if !alt.transcript.is_empty() {
                                            let event = TranscriptEvent {
                                                text: alt.transcript.clone(),
                                                is_final: resp.is_final.unwrap_or(false),
                                            };
                                            if transcript_tx.send(event).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("Ignoring non-transcript Deepgram message: {}", e);
                            }
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Err(e) => {
                        error!("Deepgram WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(SttStream {
            audio_tx,
            transcript_rx,
        })
    }

    fn name(&self) -> &str {
        "deepgram"
    }
}
