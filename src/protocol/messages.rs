use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "session.create")]
    Create {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: std::collections::HashMap<String, String>,
        cols: Option<u16>,
        rows: Option<u16>,
    },
    #[serde(rename = "session.attach")]
    Attach { session_id: String },
    #[serde(rename = "session.list")]
    List,
    #[serde(rename = "session.input")]
    Input { session_id: String, data: String },
    #[serde(rename = "session.resize")]
    Resize {
        session_id: String,
        cols: u16,
        rows: u16,
    },
    #[serde(rename = "session.kill")]
    Kill {
        session_id: String,
        signal: Option<String>,
    },
    #[serde(rename = "fs.list")]
    FsList { path: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "session.created")]
    Created { session_id: String },
    #[serde(rename = "session.attached")]
    Attached { session_id: String, replay: String },
    #[serde(rename = "session.list")]
    SessionList { sessions: Vec<SessionListEntry> },
    #[serde(rename = "session.output")]
    Output { session_id: String, data: String },
    #[serde(rename = "session.exit")]
    Exit { session_id: String, code: i32 },
    #[serde(rename = "fs.list")]
    FsList {
        path: String,
        entries: Vec<FsEntry>,
    },
    #[serde(rename = "fs.list.error")]
    FsListError { path: String, message: String },
    #[serde(rename = "error")]
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsEntry {
    pub name: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionListEntry {
    pub id: String,
    pub command: String,
    pub created_at: String,
}
