use crate::devices::states::profile::ProfileManager;
use crate::{APP_NAME, Args, ManagerMessages, WindowMessage};
use anyhow::{Result, bail};
use beacn_lib::audio::messages::eq_common::{EQBand, EQBandType};
use beacn_lib::flume::{Receiver, Sender};
use directories::BaseDirs;
use interprocess::local_socket::{
    GenericFilePath, GenericNamespaced, ListenerOptions, Name, NameType, ToFsName, ToNsName,
    tokio::prelude::{LocalSocketListener, LocalSocketStream},
    traits::tokio::{Listener, Stream},
};
use log::{debug, warn};
use std::io::ErrorKind;
use std::{env, fs, path::PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub fn parse_eq_band(s: &str) -> Result<EQBand, String> {
    let clean = s.trim().to_ascii_lowercase();
    match clean.as_str() {
        "1" | "band1" => Ok(EQBand::Band1),
        "2" | "band2" => Ok(EQBand::Band2),
        "3" | "band3" => Ok(EQBand::Band3),
        "4" | "band4" => Ok(EQBand::Band4),
        "5" | "band5" => Ok(EQBand::Band5),
        "6" | "band6" => Ok(EQBand::Band6),
        "7" | "band7" => Ok(EQBand::Band7),
        "8" | "band8" => Ok(EQBand::Band8),
        "9" | "band9" => Ok(EQBand::Band9),
        _ => Err(format!(
            "Invalid EQ band '{s}'. Expected 1-9 or Band1-Band9"
        )),
    }
}

pub fn parse_eq_shape(s: &str) -> Result<EQBandType, String> {
    let clean = s.trim().to_ascii_lowercase();
    match clean.as_str() {
        "bell" | "pk" | "peak" | "bellband" => Ok(EQBandType::BellBand),
        "hp" | "highpass" | "hpf" | "highpassfilter" => Ok(EQBandType::HighPassFilter),
        "lp" | "lowpass" | "lpf" | "lowpassfilter" => Ok(EQBandType::LowPassFilter),
        "ls" | "lowshelf" | "lsc" => Ok(EQBandType::LowShelf),
        "hs" | "highshelf" | "hsc" => Ok(EQBandType::HighShelf),
        "notch" | "no" | "notchfilter" => Ok(EQBandType::NotchFilter),
        _ => Err(format!(
            "Invalid filter shape '{s}'. Expected bell, hp, lp, ls, hs, or notch"
        )),
    }
}

pub fn parse_eq_band_args(
    band_str: &str,
    shape_str: &str,
    freq_str: &str,
    gain_str: &str,
    q_str: &str,
) -> Result<(EQBand, EQBandType, f32, f32, f32), String> {
    let band = parse_eq_band(band_str)?;
    let band_type = parse_eq_shape(shape_str)?;
    let freq = freq_str
        .trim()
        .parse::<f32>()
        .map_err(|_| format!("Invalid frequency '{freq_str}'"))?
        .clamp(20.0, 20000.0);
    let gain = gain_str
        .trim()
        .parse::<f32>()
        .map_err(|_| format!("Invalid gain '{gain_str}'"))?
        .clamp(-12.0, 12.0);
    let q = q_str
        .trim()
        .parse::<f32>()
        .map_err(|_| format!("Invalid Q factor '{q_str}'"))?
        .clamp(0.1, 10.0);
    Ok((band, band_type, freq, gain, q))
}

pub async fn handle_ipc(
    manager_rx: Receiver<ManagerMessages>,
    main_tx: Sender<WindowMessage>,
) -> Result<()> {
    debug!("Spawning IPC Socket");

    let name = get_socket_name()?;
    let listener = match bind_listener(&name) {
        Ok(listener) => listener,
        Err(e) => {
            warn!("Failed to bind to socket: {e}");
            bail!("Failed to bind to socket: {e}");
        }
    };

    debug!("IPC listener started at {name:?}");
    loop {
        tokio::select! {
            msg = manager_rx.recv_async() => {
                match msg {
                    Ok(ManagerMessages::Quit) => break,
                    Err(_) => {
                        warn!("Message Handler channel broken, bailing");
                        break;
                    }
                }
            }

            accepted = listener.accept() => {
                match accepted {
                    Ok(stream) => {
                        let mut reader = BufReader::new(stream);
                        let mut line = String::new();
                        if let Err(e) = reader.read_line(&mut line).await {
                            warn!("Failed to read line from stream: {e}");
                            continue;
                        }
                        let trimmed = line.trim();
                        if trimmed == "TRIGGER" {
                            let _ = main_tx.send(WindowMessage::OpenWindow);
                            let _ = reader.write_all(b"OK: Window opened\n").await;
                            let _ = reader.flush().await;
                        } else if let Some(name) = trimmed.strip_prefix("PROFILE:") {
                            let _ = main_tx.send(WindowMessage::SwitchProfile(name.trim().to_string()));
                            let reply = format!("OK: Switched profile to '{}'\n", name.trim());
                            let _ = reader.write_all(reply.as_bytes()).await;
                            let _ = reader.flush().await;
                        } else if trimmed == "RELOAD" {
                            let _ = main_tx.send(WindowMessage::ReloadProfile);
                            let _ = reader.write_all(b"OK: Profile reloaded\n").await;
                            let _ = reader.flush().await;
                        } else if let Some(gain_str) = trimmed.strip_prefix("SET_GAIN:") {
                            if let Ok(gain) = gain_str.trim().parse::<u8>() {
                                let _ = main_tx.send(WindowMessage::SetGain(gain));
                                let reply = format!("OK: Set mic gain to {gain} dB\n");
                                let _ = reader.write_all(reply.as_bytes()).await;
                                let _ = reader.flush().await;
                            } else {
                                let _ = reader.write_all(b"ERR: Invalid gain value\n").await;
                                let _ = reader.flush().await;
                            }
                        } else if let Some(eq_args) = trimmed.strip_prefix("SET_EQ_BAND:") {
                            let parts: Vec<&str> = eq_args.split(':').collect();
                            if parts.len() == 5 {
                                match parse_eq_band_args(parts[0], parts[1], parts[2], parts[3], parts[4]) {
                                    Ok((band, band_type, frequency, gain, q)) => {
                                        let _ = main_tx.send(WindowMessage::SetEqBand {
                                            band,
                                            band_type,
                                            frequency,
                                            gain,
                                            q,
                                        });
                                        let reply = format!(
                                            "OK: Set EQ Band {:?} to {:?} {:.1}Hz {:+.1}dB Q{:.2}\n",
                                            band, band_type, frequency, gain, q
                                        );
                                        let _ = reader.write_all(reply.as_bytes()).await;
                                        let _ = reader.flush().await;
                                    }
                                    Err(err) => {
                                        let reply = format!("ERR: {err}\n");
                                        let _ = reader.write_all(reply.as_bytes()).await;
                                        let _ = reader.flush().await;
                                    }
                                }
                            } else {
                                let _ = reader
                                    .write_all(b"ERR: Expected SET_EQ_BAND:<band>:<shape>:<freq>:<gain>:<q>\n")
                                    .await;
                                let _ = reader.flush().await;
                            }
                        } else if trimmed == "LIST_PROFILES" {
                            let profiles = ProfileManager::list_profiles();
                            let active = ProfileManager::get_active_profile_name();
                            let mut reply = String::new();
                            for p in profiles {
                                if p == active {
                                    reply.push_str(&format!("* {p} [active]\n"));
                                } else {
                                    reply.push_str(&format!("  {p}\n"));
                                }
                            }
                            let _ = reader.write_all(reply.as_bytes()).await;
                            let _ = reader.flush().await;
                        } else {
                            debug!("Unknown Message: {trimmed}");
                            let _ = reader.write_all(b"ERR: Unknown command\n").await;
                            let _ = reader.flush().await;
                        }
                    }
                    Err(e) => {
                        warn!("Unexpected socket error: {e}");
                    }
                }
            }
        }
    }

    debug!("IPC Socket closed");
    Ok(())
}

/// Binds the listener, transparently recovering from a stale socket left behind
/// by a previous, uncleanly-terminated instance.
fn bind_listener(name: &Name<'static>) -> std::io::Result<LocalSocketListener> {
    match ListenerOptions::new().name(name.clone()).create_tokio() {
        Ok(listener) => Ok(listener),
        Err(e) if e.kind() == ErrorKind::AddrInUse => {
            debug!("Socket appears to be in use; treating as stale and retrying bind");
            remove_stale_file_socket(name);
            ListenerOptions::new().name(name.clone()).create_tokio()
        }
        Err(e) => Err(e),
    }
}

/// Removes the on-disk socket file for the `GenericFilePath` fallback name type.
/// A no-op for namespaced names, which have nothing on the filesystem to remove.
fn remove_stale_file_socket(_name: &Name<'static>) {
    let path = get_socket_file_path();
    if path.exists() {
        let _ = fs::remove_file(&path);
    }
}

/// Checks whether another instance is already running by attempting to connect
/// to its socket. If so, forwards the CLI command or trigger to it and returns `true`.
pub async fn handle_active_instance(args: &Args) -> bool {
    let name = match get_socket_name() {
        Ok(name) => name,
        Err(e) => {
            debug!("Failed to build socket name: {e}");
            return false;
        }
    };

    let cmd = if let Some(ref name) = args.profile {
        format!("PROFILE:{name}")
    } else if args.reload {
        "RELOAD".to_string()
    } else if let Some(gain) = args.set_gain {
        format!("SET_GAIN:{gain}")
    } else if let Some(ref eq) = args.set_eq_band {
        if eq.len() == 5 {
            format!(
                "SET_EQ_BAND:{}:{}:{}:{}:{}",
                eq[0], eq[1], eq[2], eq[3], eq[4]
            )
        } else {
            "TRIGGER".to_string()
        }
    } else {
        "TRIGGER".to_string()
    };

    debug!("Attempting to Connect to Existing Socket at {name:?}");
    match LocalSocketStream::connect(name.clone()).await {
        Ok(stream) => {
            debug!("Connected to Existing Socket, Sending: {cmd}");
            let mut reader = BufReader::new(stream);
            let out = format!("{cmd}\n");
            let _ = reader.write_all(out.as_bytes()).await;
            let _ = reader.flush().await;

            let mut reply = String::new();
            if let Ok(n) = reader.read_line(&mut reply).await
                && n > 0
            {
                print!("{reply}");
            }
            true
        }
        Err(e) => {
            debug!("Failed to Connect to Socket: {e}");
            debug!("Removing Stale Socket File (if any)");
            remove_stale_file_socket(&name);
            false
        }
    }
}

fn get_socket_name() -> Result<Name<'static>> {
    let socket_file_name = get_socket_file_name();

    if GenericNamespaced::is_supported() {
        Ok(socket_file_name.to_ns_name::<GenericNamespaced>()?)
    } else {
        let path = get_socket_file_path();
        if let Some(parent) = path.parent()
            && let Err(e) = fs::create_dir_all(parent)
        {
            warn!("Failed to create socket directory {parent:?}: {e}");
            bail!("Failed to create socket directory");
        }
        Ok(path
            .to_string_lossy()
            .into_owned()
            .to_fs_name::<GenericFilePath>()?)
    }
}

fn get_socket_file_path() -> PathBuf {
    let base_path = BaseDirs::new()
        .and_then(|base| base.runtime_dir().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| {
            let tmp_dir = env::temp_dir();
            if !tmp_dir.exists() {
                let _ = fs::create_dir_all(&tmp_dir);
            }
            tmp_dir
        });

    base_path.join(APP_NAME).join(get_socket_file_name())
}

fn get_socket_file_name() -> String {
    format!("{APP_NAME}.socket")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_parser_helpers() {
        assert_eq!(parse_eq_band("1").unwrap(), EQBand::Band1);
        assert_eq!(parse_eq_band("Band5").unwrap(), EQBand::Band5);
        assert_eq!(parse_eq_band("9").unwrap(), EQBand::Band9);
        assert!(parse_eq_band("10").is_err());

        assert_eq!(parse_eq_shape("bell").unwrap(), EQBandType::BellBand);
        assert_eq!(parse_eq_shape("hp").unwrap(), EQBandType::HighPassFilter);
        assert_eq!(parse_eq_shape("notch").unwrap(), EQBandType::NotchFilter);
        assert_eq!(parse_eq_shape("hs").unwrap(), EQBandType::HighShelf);
        assert_eq!(parse_eq_shape("ls").unwrap(), EQBandType::LowShelf);
        assert!(parse_eq_shape("unknown").is_err());

        let (band, shape, freq, gain, q) =
            parse_eq_band_args("5", "notch", "6080", "-3.0", "2.5").unwrap();
        assert_eq!(band, EQBand::Band5);
        assert_eq!(shape, EQBandType::NotchFilter);
        assert_eq!(freq, 6080.0);
        assert_eq!(gain, -3.0);
        assert_eq!(q, 2.5);
    }
}
