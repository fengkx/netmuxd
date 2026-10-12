// Jackson Coxson
// When I original wrote netmuxd, I was a naive high school student
// and placed everything in an Arc<Muxtex<>>. While it has its uses,
// I much prefer the channel-runner paradigm for multithreaded programs.

use std::{collections::HashMap, net::IpAddr};

use crossfire::{AsyncRx, MAsyncTx, mpmc::unbounded_async};
use log::{debug, info, warn};
use tokio::sync::{mpsc::UnboundedSender, oneshot::Sender};

use crate::{
    config::NetmuxdConfig, devices::MuxerDevice, heartbeat::heartbeat,
    pairing_file::PairingFileFinder, usb::mux::UsbMuxHandle,
};

pub type ManagerSender = MAsyncTx<ManagerRequest>;
pub type ManagerReceiver = AsyncRx<ManagerRequest>;

/// First device ID handed out to network devices in shim mode.
///
/// Upstream usbmuxd assigns USB device IDs counting up from 1; the shim's
/// network devices must use a disjoint range so a `Connect` DeviceID
/// unambiguously identifies which muxer owns the device. usbmuxd will never
/// realistically allocate this many IDs in one session.
pub const SHIM_NETWORK_ID_BASE: u64 = 0x1000_0000;

pub struct ManagerRequest {
    pub request_type: ManagerRequestType,
    pub response: Option<Sender<plist::Dictionary>>,
}

pub enum ManagerRequestType {
    DiscoveredNetworkDevice {
        udid: String,
        network_address: IpAddr,
        service_name: String,
        connection_type: String,
    },
    DiscoveredUsbDevice {
        udid: String,
        location_id: u64,
        product_id: u64,
        speed: u64,
        handle: UsbMuxHandle,
    },
    DeferredMuxerAdd {
        device: MuxerDevice,
        response: Option<Sender<plist::Dictionary>>,
        accepted: Sender<bool>,
    },
    RemoveDevice {
        udid: String,
        connection_type: Option<String>,
    },
    ListDevices,
    GetDeviceConnection {
        id: u64,
        response: tokio::sync::oneshot::Sender<Option<DeviceConnection>>,
    },
    HeartbeatFailed {
        udid: String,
        generation: u64,
    },
    OpenSocket {
        device_id: u64,
        kill: Sender<()>,
    },
    Subscribe {
        listener: UnboundedSender<ListenerEvent>,
    },
}

#[derive(Clone)]
pub enum ListenerEvent {
    Attached(plist::Dictionary),
    Detached(u64),
}

#[derive(Clone)]
pub struct DeviceConnection {
    pub connection_type: String,
    pub serial_number: String,
    pub network_address: Option<IpAddr>,
    pub usb: Option<UsbMuxHandle>,
}

impl ManagerRequest {
    pub fn discovered_device(
        udid: String,
        network_address: IpAddr,
        service_name: String,
        connection_type: String,
    ) -> Self {
        Self {
            request_type: ManagerRequestType::DiscoveredNetworkDevice {
                udid,
                network_address,
                service_name,
                connection_type,
            },
            response: None,
        }
    }
    pub fn heartbeat_failed(udid: String, generation: u64) -> Self {
        Self {
            request_type: ManagerRequestType::HeartbeatFailed { udid, generation },
            response: None,
        }
    }
}

/// Find the device_id for a (udid, connection_type) pair, if any
fn find_device_id(
    devices: &HashMap<u64, MuxerDevice>,
    udid: &str,
    connection_type: &str,
) -> Option<u64> {
    devices
        .iter()
        .find(|(_, d)| d.serial_number == udid && d.connection_type == connection_type)
        .map(|(id, _)| *id)
}

fn drop_entry(
    id: u64,
    devices: &mut HashMap<u64, MuxerDevice>,
    usb_handles: &mut HashMap<u64, UsbMuxHandle>,
    open_sockets: &mut HashMap<u64, Vec<Sender<()>>>,
) -> Option<UsbMuxHandle> {
    devices.remove(&id);
    if let Some(l) = open_sockets.remove(&id) {
        for s in l {
            let _ = s.send(());
        }
    }
    usb_handles.remove(&id)
}

fn attached_plist(device: &MuxerDevice) -> plist::Dictionary {
    plist_macro::plist!(dict {
        "DeviceID": device.device_id,
        "MessageType": "Attached",
        "Properties": plist::Value::Dictionary(device.into()),
    })
}

fn broadcast(listeners: &mut Vec<UnboundedSender<ListenerEvent>>, event: ListenerEvent) {
    listeners.retain(|tx| tx.send(event.clone()).is_ok());
}

pub fn new_manager_thread(config: &NetmuxdConfig) -> ManagerSender {
    let (manager_sender, manager_recv) = new_channel_pair();
    let to_return = manager_sender.clone();
    let config = config.clone();
    let pairing_file_finder = PairingFileFinder::new(&config);

    let mut devices: HashMap<u64, MuxerDevice> = HashMap::new();
    let mut usb_handles: HashMap<u64, UsbMuxHandle> = HashMap::new();
    let mut open_sockets: HashMap<u64, Vec<Sender<()>>> = HashMap::new();
    let mut listeners: Vec<UnboundedSender<ListenerEvent>> = Vec::new();
    let mut pending = HashMap::<String, (u64, IpAddr)>::new();
    let mut last_index: u64 = if config.upstream.is_some() {
        SHIM_NETWORK_ID_BASE
    } else {
        1
    };
    let mut last_interface_index: u64 = 1;

    tokio::task::spawn(async move {
        loop {
            let message = match manager_recv.recv().await {
                Ok(m) => m,
                Err(_) => {
                    debug!("All senders are closed, stopping manager thread");
                    break;
                }
            };
            match message.request_type {
                ManagerRequestType::DiscoveredNetworkDevice {
                    udid,
                    network_address,
                    service_name,
                    connection_type,
                } => {
                    let current = find_device_id(&devices, &udid, &connection_type);
                    if current
                        .is_some_and(|id| devices[&id].network_address == Some(network_address))
                    {
                        reply_add(message.response, true);
                        continue;
                    }
                    if pending
                        .get(&udid)
                        .is_some_and(|(_, ip)| *ip == network_address)
                    {
                        reply_add(message.response, false);
                        continue;
                    }
                    let pairing_file = match pairing_file_finder.get_pairing_record(&udid).await {
                        Ok(p) => p,
                        Err(e) => {
                            debug!("Failed to get pairing record: {e:?}");
                            reply_add(message.response, false);
                            continue;
                        }
                    };

                    let device = MuxerDevice {
                        connection_type,
                        device_id: last_index,
                        interface_index: last_interface_index,
                        serial_number: udid.clone(),
                        network_address: Some(network_address),
                        service_name: Some(service_name),
                        connection_speed: None,
                        location_id: None,
                        product_id: None,
                    };
                    last_index = last_index.wrapping_add(1);
                    last_interface_index = last_interface_index.wrapping_add(1);

                    // Device IDs are monotonic connection generations, including pending probes.
                    pending.insert(udid, (device.device_id, network_address));
                    if config.use_heartbeat || current.is_some() {
                        heartbeat(
                            device,
                            message.response,
                            pairing_file,
                            manager_sender.clone(),
                            config.use_heartbeat,
                        )
                        .await;

                        continue;
                    }

                    pending.remove(&device.serial_number);
                    let attached = attached_plist(&device);
                    devices.insert(device.device_id, device);
                    broadcast(&mut listeners, ListenerEvent::Attached(attached));
                    if let Some(response) = message.response {
                        response
                            .send(plist_macro::plist!(dict {
                            "Result": 1,
                            }))
                            .ok();
                    }
                }
                ManagerRequestType::DiscoveredUsbDevice {
                    udid,
                    location_id,
                    product_id,
                    speed,
                    handle,
                } => {
                    if let Some(id) = find_device_id(&devices, &udid, "USB") {
                        // Replace the handle but keep the device entry.
                        usb_handles.insert(id, handle);
                        continue;
                    }
                    let device = MuxerDevice {
                        connection_type: "USB".into(),
                        device_id: last_index,
                        interface_index: last_interface_index,
                        serial_number: udid.clone(),
                        network_address: None,
                        service_name: None,
                        connection_speed: Some(speed),
                        location_id: Some(location_id),
                        product_id: Some(product_id),
                    };
                    println!("Adding USB device {udid}");
                    let attached = attached_plist(&device);
                    devices.insert(last_index, device);
                    usb_handles.insert(last_index, handle);
                    broadcast(&mut listeners, ListenerEvent::Attached(attached));
                    last_index = last_index.wrapping_add(1);
                    last_interface_index = last_interface_index.wrapping_add(1);
                }
                ManagerRequestType::DeferredMuxerAdd {
                    device,
                    response,
                    accepted,
                } => {
                    let published = complete_network_device(
                        device,
                        &mut pending,
                        &mut devices,
                        &mut usb_handles,
                        &mut open_sockets,
                        &mut listeners,
                    );
                    reply_add(response, published);
                    let _ = accepted.send(published);
                }
                ManagerRequestType::RemoveDevice {
                    udid,
                    connection_type,
                } => {
                    if connection_type.as_deref().is_none_or(|ct| ct == "Network") {
                        pending.remove(&udid);
                    }
                    let ids: Vec<u64> = devices
                        .iter()
                        .filter(|(_, d)| {
                            d.serial_number == udid
                                && connection_type
                                    .as_deref()
                                    .map(|ct| d.connection_type == ct)
                                    .unwrap_or(true)
                        })
                        .map(|(id, _)| *id)
                        .collect();
                    for id in ids {
                        if let Some(h) =
                            drop_entry(id, &mut devices, &mut usb_handles, &mut open_sockets)
                        {
                            h.shutdown().await;
                        }
                        broadcast(&mut listeners, ListenerEvent::Detached(id));
                    }
                }
                ManagerRequestType::ListDevices => {
                    if let Some(response) = message.response {
                        let mut device_list = Vec::new();
                        for d in devices.values() {
                            device_list.push(plist::Value::Dictionary(attached_plist(d)));
                        }
                        response
                            .send(plist_macro::plist!(dict {
                                "DeviceList": device_list
                            }))
                            .ok();
                    }
                }
                ManagerRequestType::GetDeviceConnection { id, response } => {
                    let lookup = devices.get(&id).map(|d| DeviceConnection {
                        connection_type: d.connection_type.clone(),
                        serial_number: d.serial_number.clone(),
                        network_address: d.network_address,
                        usb: usb_handles.get(&id).cloned(),
                    });
                    let _ = response.send(lookup);
                }
                ManagerRequestType::HeartbeatFailed { udid, generation } => {
                    fail_network_device(
                        &udid,
                        generation,
                        &mut pending,
                        &mut devices,
                        &mut usb_handles,
                        &mut open_sockets,
                        &mut listeners,
                    );
                }
                ManagerRequestType::OpenSocket { device_id, kill } => {
                    let sockets = open_sockets.entry(device_id).or_default();
                    sockets.push(kill);
                    info!(
                        "event=proxy_socket_opened device_id={device_id} registered_sockets={}",
                        sockets.len()
                    );
                }
                ManagerRequestType::Subscribe { listener } => {
                    let mut ok = true;
                    for d in devices.values() {
                        if listener
                            .send(ListenerEvent::Attached(attached_plist(d)))
                            .is_err()
                        {
                            ok = false;
                            break;
                        }
                    }
                    if ok {
                        listeners.push(listener);
                    }
                }
            }
        }
    });

    to_return
}

fn new_channel_pair() -> (ManagerSender, ManagerReceiver) {
    let (t, r) = unbounded_async();
    (t.into(), r.into())
}

fn reply_add(response: Option<Sender<plist::Dictionary>>, success: bool) {
    if let Some(response) = response {
        let _ = response
            .send(plist_macro::plist!(dict { "Result": if success { 1u64 } else { 0u64 } }));
    }
}

// Only the latest validated candidate may replace the active connection.
fn complete_network_device(
    device: MuxerDevice,
    pending: &mut HashMap<String, (u64, IpAddr)>,
    devices: &mut HashMap<u64, MuxerDevice>,
    usb_handles: &mut HashMap<u64, UsbMuxHandle>,
    open_sockets: &mut HashMap<u64, Vec<Sender<()>>>,
    listeners: &mut Vec<UnboundedSender<ListenerEvent>>,
) -> bool {
    if pending.get(&device.serial_number).map(|p| p.0) != Some(device.device_id) {
        return false;
    }
    pending.remove(&device.serial_number);
    if let Some(id) = find_device_id(devices, &device.serial_number, &device.connection_type) {
        drop_entry(id, devices, usb_handles, open_sockets);
        broadcast(listeners, ListenerEvent::Detached(id));
    }
    info!(
        "event=device_added udid={} generation={} address={:?}",
        device.serial_number, device.device_id, device.network_address
    );
    let attached = attached_plist(&device);
    devices.insert(device.device_id, device);
    broadcast(listeners, ListenerEvent::Attached(attached));
    true
}

fn fail_network_device(
    udid: &str,
    generation: u64,
    pending: &mut HashMap<String, (u64, IpAddr)>,
    devices: &mut HashMap<u64, MuxerDevice>,
    usb_handles: &mut HashMap<u64, UsbMuxHandle>,
    open_sockets: &mut HashMap<u64, Vec<Sender<()>>>,
    listeners: &mut Vec<UnboundedSender<ListenerEvent>>,
) {
    if pending.get(udid).map(|p| p.0) == Some(generation) {
        pending.remove(udid);
    }
    if let Some(id) = find_device_id(devices, udid, "Network")
        && id == generation
    {
        let open_socket_count = open_sockets.get(&id).map_or(0, Vec::len);
        warn!(
            "event=heartbeat_failed udid={udid} generation={generation} action=remove_device_and_stop_streams open_sockets={open_socket_count}"
        );
        drop_entry(id, devices, usb_handles, open_sockets);
        broadcast(listeners, ListenerEvent::Detached(id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Registry {
        pending: HashMap<String, (u64, IpAddr)>,
        devices: HashMap<u64, MuxerDevice>,
        usb: HashMap<u64, UsbMuxHandle>,
        sockets: HashMap<u64, Vec<Sender<()>>>,
        listeners: Vec<UnboundedSender<ListenerEvent>>,
    }

    impl Registry {
        fn candidate(&mut self, generation: u64, ip: &str) -> MuxerDevice {
            let ip = ip.parse().unwrap();
            self.pending.insert("phone".into(), (generation, ip));
            MuxerDevice {
                connection_type: "Network".into(),
                device_id: generation,
                interface_index: generation,
                serial_number: "phone".into(),
                network_address: Some(ip),
                service_name: Some("_apple-mobdev2._tcp.local".into()),
                connection_speed: None,
                location_id: None,
                product_id: None,
            }
        }

        fn complete(&mut self, device: MuxerDevice) -> bool {
            complete_network_device(
                device,
                &mut self.pending,
                &mut self.devices,
                &mut self.usb,
                &mut self.sockets,
                &mut self.listeners,
            )
        }

        fn fail(&mut self, generation: u64) {
            fail_network_device(
                "phone",
                generation,
                &mut self.pending,
                &mut self.devices,
                &mut self.usb,
                &mut self.sockets,
                &mut self.listeners,
            );
        }
    }

    #[test]
    fn dhcp_change_after_leaving_home() {
        let mut registry = Registry::default();
        let old = registry.candidate(1, "192.168.2.135");
        assert!(registry.complete(old.clone()));
        registry.fail(1);
        assert!(registry.devices.is_empty());
        let new = registry.candidate(2, "192.168.2.134");
        assert!(registry.complete(new));
        registry.fail(1);
        assert!(!registry.complete(old));
        assert_eq!(registry.devices.len(), 1);
        assert_eq!(
            registry.devices[&2].network_address,
            Some("192.168.2.134".parse().unwrap())
        );
    }

    #[test]
    fn replacement_waits_for_validation_and_detaches_old_streams() {
        let mut registry = Registry::default();
        let old = registry.candidate(1, "192.168.2.135");
        assert!(registry.complete(old));
        let (kill, mut killed) = tokio::sync::oneshot::channel();
        registry.sockets.insert(1, vec![kill]);
        let (listener, mut events) = tokio::sync::mpsc::unbounded_channel();
        registry.listeners.push(listener);
        let new = registry.candidate(2, "192.168.2.134");
        assert!(registry.devices.contains_key(&1));
        assert!(events.try_recv().is_err());
        assert!(killed.try_recv().is_err());
        assert!(registry.complete(new));
        assert_eq!(killed.try_recv(), Ok(()));
        assert!(matches!(events.try_recv(), Ok(ListenerEvent::Detached(1))));
        assert!(matches!(events.try_recv(), Ok(ListenerEvent::Attached(_))));
        registry.fail(1);
        assert!(registry.devices.contains_key(&2));
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn failed_candidate_preserves_working_connection_and_can_retry() {
        let mut registry = Registry::default();
        let old = registry.candidate(1, "192.168.2.135");
        assert!(registry.complete(old));
        let failed = registry.candidate(2, "192.168.2.134");
        registry.fail(2);
        assert!(registry.devices.contains_key(&1));
        assert!(registry.pending.is_empty());
        assert!(!registry.complete(failed));
        let retry = registry.candidate(3, "192.168.2.134");
        assert!(registry.complete(retry));
        registry.fail(2);
        assert!(registry.devices.contains_key(&3));
    }

    #[test]
    fn out_of_order_validation_cannot_overwrite_newer_candidate() {
        let mut registry = Registry::default();
        let old = registry.candidate(1, "192.168.2.135");
        let new = registry.candidate(2, "192.168.2.134");
        assert!(!registry.complete(old.clone()));
        registry.fail(1);
        assert!(registry.complete(new));
        assert!(!registry.complete(old));
        registry.fail(2);
        assert!(registry.devices.is_empty());
    }
}
