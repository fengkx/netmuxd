// Jackson Coxson

use idevice::{
    HeartbeatError, Idevice, IdeviceError, heartbeat::HeartbeatClient, lockdown::LockdownClient,
};
use log::{debug, info, warn};
use std::net::SocketAddr;
use tokio::sync::oneshot::Sender;

use crate::{
    devices::MuxerDevice,
    manager::{ManagerRequest, ManagerSender},
};

async fn connect_heartbeat(
    device: &MuxerDevice,
    pairing_file: &idevice::pairing_file::PairingFile,
) -> Result<HeartbeatClient, IdeviceError> {
    let lockdown_socket = SocketAddr::new(
        device.network_address.unwrap(),
        LockdownClient::LOCKDOWND_PORT,
    );
    let socket = Box::new(tokio::net::TcpStream::connect(lockdown_socket).await?);
    let idevice = Idevice::new(socket, "netmuxd_heartbeat_reconnect");
    let mut lockdown_client = LockdownClient { idevice };
    lockdown_client.start_session(pairing_file).await?;

    let (port, _) = lockdown_client
        .start_service("com.apple.mobile.heartbeat")
        .await?;
    let heartbeat_socket = SocketAddr::new(device.network_address.unwrap(), port);
    let socket = Box::new(tokio::net::TcpStream::connect(heartbeat_socket).await?);
    let mut idevice = Idevice::new(socket, "heartbeat_client_reconnect");
    idevice.start_session(pairing_file, false).await?;

    Ok(HeartbeatClient { idevice })
}

pub async fn heartbeat(
    device: MuxerDevice,
    response: Option<Sender<plist::Dictionary>>,
    pairing_file: idevice::pairing_file::PairingFile,
    sender: ManagerSender,
    monitor: bool,
) {
    debug!(
        "event=heartbeat_spawn udid={} device={device:?}",
        device.serial_number
    );
    tokio::spawn(async move {
        let udid = device.serial_number.clone();
        let generation = device.device_id;
        let mut heartbeat_client = match tokio::time::timeout(
            std::time::Duration::from_secs(15),
            connect_heartbeat(&device, &pairing_file),
        )
        .await
        .map_err(|e| e.to_string())
        .and_then(|result| result.map_err(|e| e.to_string()))
        {
            Ok(client) => client,
            Err(error) => {
                warn!(
                    "event=heartbeat_setup_failed udid={udid} generation={generation} error={error}"
                );
                if let Some(response) = response {
                    let _ = response.send(plist_macro::plist!(dict { "Result": 0u64 }));
                }
                let _ = sender
                    .send(ManagerRequest::heartbeat_failed(udid, generation))
                    .await;
                return;
            }
        };
        let mut interval = 10;
        let (accepted, registered) = tokio::sync::oneshot::channel();

        // now that we successfully created the heartbeat client, we can send the deferred add
        sender
            .send(ManagerRequest {
                request_type: crate::manager::ManagerRequestType::DeferredMuxerAdd {
                    device: device.clone(),
                    response,
                    accepted,
                },
                response: None,
            })
            .await
            .ok();

        if registered.await != Ok(true) || !monitor {
            return;
        }

        info!("event=heartbeat_ready udid={udid} interval_seconds={interval}");

        loop {
            match heartbeat_client.get_marco(interval + 5).await {
                Ok(i) => {
                    interval = i;
                }
                Err(IdeviceError::Heartbeat(HeartbeatError::SleepyTime)) => {
                    info!(
                        "event=heartbeat_sleep_notification udid={udid} action=send_polo_and_reconnect_keep_device_and_streams"
                    );
                    if let Err(e) = heartbeat_client.send_polo().await {
                        warn!(
                            "event=heartbeat_failed udid={udid} phase=send_after_sleep error={e:?}"
                        );
                        sender
                            .send(ManagerRequest::heartbeat_failed(udid.clone(), generation))
                            .await
                            .ok();
                        break;
                    }
                    match connect_heartbeat(&device, &pairing_file).await {
                        Ok(replacement) => {
                            heartbeat_client = replacement;
                            interval = 10;
                            info!(
                                "event=heartbeat_reconnected_after_sleep udid={udid} interval_seconds={interval}"
                            );
                        }
                        Err(e) => {
                            warn!(
                                "event=heartbeat_failed udid={udid} phase=reconnect_after_sleep error={e:?}"
                            );
                            sender
                                .send(ManagerRequest::heartbeat_failed(udid.clone(), generation))
                                .await
                                .ok();
                            break;
                        }
                    }
                    continue;
                }
                Err(e) => {
                    warn!("event=heartbeat_failed udid={udid} phase=recv error={e:?}");
                    sender
                        .send(ManagerRequest::heartbeat_failed(udid.clone(), generation))
                        .await
                        .ok();
                    break;
                }
            }
            if let Err(e) = heartbeat_client.send_polo().await {
                warn!("event=heartbeat_failed udid={udid} phase=send error={e:?}");
                sender
                    .send(ManagerRequest::heartbeat_failed(udid.clone(), generation))
                    .await
                    .ok();
                break;
            }
            if sender.is_disconnected() {
                break;
            }
        }
    });
}
