use std::collections::HashMap;
use std::io::{Read, Write};

use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

use crate::session::types::SessionInfo;

pub struct PtyProcess {
    pub input_tx: mpsc::Sender<Vec<u8>>,
    pub output_tx: broadcast::Sender<Vec<u8>>,
    pub resize_tx: mpsc::Sender<(u16, u16)>,
    pub exit_rx: mpsc::Receiver<i32>,
}

impl PtyProcess {
    pub fn spawn(
        info: SessionInfo,
        env: HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let pty_system = native_pty_system();

        let pty_pair = pty_system.openpty(PtySize {
            rows: info.rows,
            cols: info.cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;

        let mut cmd = CommandBuilder::new(&info.command);
        cmd.args(&info.args);
        for (k, v) in &env {
            cmd.env(k, v);
        }

        let child = pty_pair.slave.spawn_command(cmd)?;
        drop(pty_pair.slave);

        let (input_tx, input_rx) = mpsc::channel::<Vec<u8>>(256);
        let (output_tx, _) = broadcast::channel::<Vec<u8>>(1024);
        let (resize_tx, resize_rx) = mpsc::channel::<(u16, u16)>(16);

        let writer = pty_pair.master.take_writer()?;
        let reader = pty_pair.master.try_clone_reader()?;

        let output_tx_clone = output_tx.clone();
        let session_id = info.id.clone();

        // Read loop: PTY stdout → broadcast channel
        tokio::task::spawn_blocking({
            let session_id = session_id.clone();
            move || {
                Self::read_loop(reader, output_tx_clone, &session_id);
            }
        });

        // Write loop: input channel → PTY stdin
        tokio::spawn(Self::write_loop(input_rx, writer, session_id.clone()));

        // Resize loop
        let master_for_resize = pty_pair.master;
        tokio::spawn(Self::resize_loop(resize_rx, master_for_resize, session_id.clone()));

        // Child wait loop
        let output_tx_exit = output_tx.clone();
        let (exit_tx, exit_rx) = mpsc::channel::<i32>(1);
        tokio::task::spawn_blocking(move || {
            Self::wait_loop(child, output_tx_exit, &session_id, exit_tx);
        });

        Ok(Self {
            input_tx,
            output_tx,
            resize_tx,
            exit_rx,
        })
    }

    fn read_loop(
        mut reader: Box<dyn Read + Send>,
        output_tx: broadcast::Sender<Vec<u8>>,
        session_id: &str,
    ) {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => {
                    info!(session_id, "PTY read EOF");
                    break;
                }
                Ok(n) => {
                    if output_tx.send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    warn!(session_id, error = %e, "PTY read error");
                    break;
                }
            }
        }
    }

    async fn write_loop(
        mut input_rx: mpsc::Receiver<Vec<u8>>,
        mut writer: Box<dyn Write + Send>,
        session_id: String,
    ) {
        while let Some(data) = input_rx.recv().await {
            if let Err(e) = writer.write_all(&data) {
                warn!(session_id = %session_id, error = %e, "PTY write error");
                break;
            }
            let _ = writer.flush();
        }
    }

    async fn resize_loop(
        mut resize_rx: mpsc::Receiver<(u16, u16)>,
        master: Box<dyn portable_pty::MasterPty + Send>,
        session_id: String,
    ) {
        while let Some((cols, rows)) = resize_rx.recv().await {
            if let Err(e) = master.resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            }) {
                warn!(session_id = %session_id, error = %e, "PTY resize error");
            }
        }
    }

    fn wait_loop(
        mut child: Box<dyn portable_pty::Child + Send + Sync>,
        _output_tx: broadcast::Sender<Vec<u8>>,
        session_id: &str,
        exit_tx: mpsc::Sender<i32>,
    ) {
        match child.wait() {
            Ok(status) => {
                let code = status.exit_code() as i32;
                info!(session_id, code, "Process exited");
                let _ = exit_tx.blocking_send(code);
            }
            Err(e) => {
                error!(session_id, error = %e, "Failed to wait on child");
                let _ = exit_tx.blocking_send(-1);
            }
        }
    }
}
