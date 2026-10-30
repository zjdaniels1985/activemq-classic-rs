//! AMQP 1.0 transport adapter and compact hand-written AMQP type codec.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::ProtocolContext;
use crate::core::BrokerCore;
use crate::core::destination::{Destination, DestinationKind};
use crate::core::message::{Body, Message};
use crate::core::scheduler::SCHEDULED_DELAY;
use crate::core::types::{AckOp, Acks, ConnectInfo, ConsumerOptions, Event, TxId};

const AMQP_HEADER: &[u8; 8] = b"AMQP\0\x01\0\0";
const SASL_HEADER: &[u8; 8] = b"AMQP\x03\x01\0\0";
const AMQP_FRAME: u8 = 0;
const SASL_FRAME: u8 = 1;

pub async fn serve(context: ProtocolContext, listener: TcpListener) -> io::Result<()> {
    super::accept_loop("amqp", context, listener, handle_connection).await
}

async fn handle_connection(
    context: ProtocolContext,
    mut stream: TcpStream,
    remote: std::net::SocketAddr,
) -> io::Result<()> {
    let mut peer_header = [0; 8];
    stream.read_exact(&mut peer_header).await?;
    let (username, password) = if &peer_header == SASL_HEADER {
        stream.write_all(SASL_HEADER).await?;
        let mut supported = vec!["PLAIN"];
        if authenticate_probe(&context.core, None, None, remote) {
            supported.push("ANONYMOUS");
        }
        let mechanisms = described(0x40, list(vec![array_symbols(&supported)]));
        write_frame(
            &mut stream,
            SASL_FRAME,
            0,
            &encode(&mechanisms),
            context.max_frame_size,
        )
        .await?;
        let init = read_frame(&mut stream, context.max_frame_size).await?;
        if init.frame_type != SASL_FRAME || descriptor(&init.value) != Some(0x41) {
            return Err(invalid_data("expected sasl-init"));
        }
        let fields = list_items(&init.value).unwrap_or_default();
        let mechanism = fields
            .first()
            .and_then(as_symbol_or_string)
            .unwrap_or_default();
        let auth = fields.get(1).and_then(as_binary).unwrap_or_default();
        let (user, pass) = match mechanism.as_str() {
            "PLAIN" => {
                let mut parts = auth.splitn(3, |b| *b == 0);
                let _authzid = parts.next();
                let user = parts.next().unwrap_or_default();
                let pass = parts.next().unwrap_or_default();
                (
                    Some(String::from_utf8_lossy(user).into_owned()),
                    Some(String::from_utf8_lossy(pass).into_owned()),
                )
            }
            "ANONYMOUS" => (None, None),
            _ => (None, None),
        };
        let valid_mech = mechanism == "PLAIN" || mechanism == "ANONYMOUS";
        let authenticated = valid_mech
            && authenticate_probe(&context.core, user.as_deref(), pass.as_deref(), remote);
        let outcome = described(0x44, list(vec![ubyte(if authenticated { 0 } else { 1 })]));
        write_frame(
            &mut stream,
            SASL_FRAME,
            0,
            &encode(&outcome),
            context.max_frame_size,
        )
        .await?;
        if !authenticated {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "AMQP SASL authentication failed",
            ));
        }
        stream.read_exact(&mut peer_header).await?;
        if &peer_header != AMQP_HEADER {
            return Err(invalid_data("expected AMQP protocol header after SASL"));
        }
        (user, pass)
    } else if &peer_header == AMQP_HEADER {
        (None, None)
    } else {
        return Err(invalid_data("unsupported AMQP protocol header"));
    };
    stream.write_all(AMQP_HEADER).await?;
    let mut core_conn = None;
    let mut _connection_guard = None;
    let mut events: Option<tokio::sync::mpsc::UnboundedReceiver<Event>> = None;
    let mut links = HashMap::<(u16, u32), Link>::new();
    let mut link_aliases = HashMap::<(u16, u32), (u16, u32)>::new();
    let mut remote_channels = HashMap::<u16, u16>::new();
    let mut transactions = HashMap::<Vec<u8>, TxId>::new();
    let mut local_unsettled = HashMap::<u32, (u16, u32, u64, u64)>::new();
    let mut incoming = HashMap::<(u16, u32), Vec<u8>>::new();
    let mut incoming_delivery_ids = HashMap::<(u16, u32), u32>::new();
    let mut incoming_settled = HashMap::<(u16, u32), bool>::new();
    let mut incoming_transactions = HashMap::<(u16, u32), Option<Vec<u8>>>::new();
    let mut remote_delivery = 0u32;
    let mut outgoing_delivery = 0u32;
    let mut peer_frame_max = context.max_frame_size.min(512 * 1024) as u32;
    let mut open_seen = false;
    let mut last_activity = tokio::time::Instant::now();

    loop {
        tokio::select! {
            frame = read_frame(&mut stream, context.max_frame_size) => {
                let frame = match frame { Ok(frame) => frame, Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break, Err(error) => return Err(error) };
                last_activity = tokio::time::Instant::now();
                if frame.frame_type != AMQP_FRAME { return Err(invalid_data("unexpected SASL frame after AMQP header")); }
                let channel = remote_channels.get(&frame.channel).copied().unwrap_or(frame.channel);
                tracing::debug!(channel = frame.channel, session_channel = channel, descriptor = ?descriptor(&frame.value), "received AMQP performative");
                match descriptor(&frame.value) {
                    Some(0x10) => {
                        if open_seen { return Err(invalid_data("duplicate open")); }
                        open_seen = true;
                        let fields = list_items(&frame.value).unwrap_or_default();
                        let container = fields.first().and_then(as_string).unwrap_or_default();
                        let client_id = (!container.is_empty()).then_some(container.clone());
                        let max_in = fields.get(2).and_then(as_u32).filter(|v| *v >= 512).unwrap_or(context.max_frame_size.min(u32::MAX as usize) as u32);
                        peer_frame_max = max_in.min(context.max_frame_size.min(u32::MAX as usize) as u32);
                        let info = ConnectInfo { client_id: client_id.clone(), username: username.clone(), password: password.clone(), protocol: "amqp".into(), remote_address: remote.to_string(), redeliver_on_rollback: true, ..Default::default() };
                        let (connection, rx) = match context.core.connect(info) {
                            Ok(result) => result,
                            Err(error) => {
                                let close = described(0x18, list(vec![described(0x1d, list(vec![symbol(error_condition(&error)), string(error.to_string())]))]));
                                write_frame(&mut stream, AMQP_FRAME, frame.channel, &encode(&close), peer_frame_max as usize).await?;
                                return Ok(());
                            }
                        };
                        core_conn = Some(connection);
                        _connection_guard = Some(ConnectionGuard {
                            core: Arc::clone(&context.core),
                            connection,
                        });
                        events = Some(rx);
                        let response = described(0x10, list(vec![
                            string(format!("{}-broker", container)), null(),
                            uint(context.max_frame_size.min(u32::MAX as usize) as u32), Value::U16(u16::MAX), uint(60_000),
                            null(), null(), array_symbols(&["ANONYMOUS-RELAY", "DELAYED_DELIVERY"]), null(),
                            map(vec![(symbol("product"), string("ActiveMQ Classic")), (symbol("version"), string("6.2.0")), (symbol("topic-prefix"), string("topic://")), (symbol("queue-prefix"), string("queue://"))]),
                        ]));
                        write_frame(&mut stream, AMQP_FRAME, frame.channel, &encode(&response), peer_frame_max as usize).await?;
                    }
                    Some(0x11) => {
                        let fields = list_items(&frame.value).unwrap_or_default();
                        if let Some(remote_channel) = fields.first().and_then(as_u32).and_then(|id| u16::try_from(id).ok()) {
                            remote_channels.insert(frame.channel, remote_channel);
                            continue;
                        }
                        let response = described(0x11, list(vec![Value::U16(frame.channel), uint(0), uint(0x7fffffff), uint(0x7fffffff), uint(0x7fffffff)]));
                        write_frame(&mut stream, AMQP_FRAME, frame.channel, &encode(&response), peer_frame_max as usize).await?;
                    }
                    Some(0x12) => {
                        let Some(conn) = core_conn else { return Err(invalid_data("attach before open")); };
                        let f = list_items(&frame.value).unwrap_or_default();
                        tracing::debug!(channel = channel, ?f, "received AMQP attach fields");
                        let name = f.first().and_then(as_string).unwrap_or_else(|| "link".into());
                        let handle = f.get(1).and_then(as_u32).unwrap_or(0);
                        let receiver_role = f.get(2).and_then(as_bool).unwrap_or(false);
                        let snd_settle = f.get(3).and_then(as_u32).unwrap_or(2);
                        let mut source = f.get(5).cloned().unwrap_or(Value::Null);
                        let mut target = f.get(6).cloned().unwrap_or(Value::Null);
                        if frame.channel != channel
                            && let Some((prior_key, prior)) = links.iter().find(|(_, prior)| {
                                prior.name == name
                                    && if receiver_role {
                                        prior.consumer.is_some()
                                    } else {
                                        prior.sender.is_some() || prior.coordinator
                                    }
                            }) {
                                let prior_key = *prior_key;
                                let prior = prior.clone();
                                link_aliases.insert((channel, handle), prior_key);
                                if prior.coordinator {
                                    let response = described(0x12, list(vec![
                                        string(name), uint(handle), boolean(!receiver_role), uint(snd_settle), uint(0),
                                        prior.source.clone(), prior.target.clone(),
                                    ]));
                                    write_frame(&mut stream, AMQP_FRAME, frame.channel, &encode(&response), peer_frame_max as usize).await?;
                                    if !receiver_role {
                                        let flow = described(0x13, list(vec![null(), uint(0x7fffffff), uint(0), uint(0x7fffffff), uint(handle), uint(0), uint(1000), null(), boolean(false)]));
                                        write_frame(&mut stream, AMQP_FRAME, frame.channel, &encode(&flow), peer_frame_max as usize).await?;
                                    }
                                }
                                continue;
                        }
                        let resumed_destination = links.values().find(|prior| {
                            prior.name == name && if receiver_role { prior.consumer.is_some() } else { prior.sender.is_some() }
                        }).and_then(|prior| prior.destination.clone());
                        if receiver_role && terminus_address(&source).is_none() && !terminus_dynamic(&source) {
                            if let Some(destination) = &resumed_destination {
                                source = make_terminus(0x28, Some(destination.to_uri()), false);
                            }
                        } else if !receiver_role
                            && terminus_address(&target).is_none()
                            && !terminus_dynamic(&target)
                            && let Some(destination) = &resumed_destination
                        {
                            target = make_terminus(0x29, Some(destination.to_uri()), false);
                        }
                        let is_coordinator = descriptor(&target) == Some(0x30);
                        let mut link = Link { name: name.clone(), handle, channel, coordinator: is_coordinator, sender: None, consumer: None, destination: None, source: source.clone(), target: target.clone(), credit: 0, delivery_count: 0, next_delivery: 0, presettled: snd_settle == 1, durable: false };
                        let mut response_source = link.source.clone();
                        let mut response_target = link.target.clone();
                        if is_coordinator {
                            response_target = described(0x30, list(vec![]));
                        } else if receiver_role {
                            let address = terminus_address(&source);
                            let dynamic = terminus_dynamic(&source);
                            if dynamic || address.is_some() {
                                let dest = if dynamic {
                                    context.core.new_temp(conn, terminus_kind(&source, true)).map_err(core_error)?
                                } else {
                                    parse_address(address.as_deref().unwrap_or_default(), capability_kind(&source, true))?
                                };
                                let durable = terminus_durable(&source) > 0 && terminus_expiry_never(&source);
                                let mut options = ConsumerOptions::new(dest.clone());
                                options.credit_mode = true;
                                options.prefetch = 0;
                                options.external_id = Some(name.clone());
                                options.durable_name = durable.then_some(name.clone());
                                options.selector = link_filter(&source);
                                options.no_local = has_filter(&source, "no-local");
                                options.browser = link_distribution_copy(&source) && dest.is_queue();
                                let consumer = context.core.subscribe(conn, options).map_err(core_error)?;
                                link.consumer = Some(consumer);
                                link.destination = Some(dest.clone());
                                if dynamic { response_source = make_terminus(0x28, Some(dest.to_uri()), true); }
                                link.durable = durable;
                            }
                        } else {
                            let address = terminus_address(&target);
                            let dynamic = terminus_dynamic(&target);
                            if dynamic {
                                let dest = context.core.new_temp(conn, terminus_kind(&target, false)).map_err(core_error)?;
                                link.destination = Some(dest.clone());
                                let producer = context.core.register_producer(conn, Some(dest.clone())).map_err(core_error)?;
                                link.sender = Some(producer);
                                response_target = make_terminus(0x29, Some(dest.to_uri()), true);
                            } else if let Some(address) = address {
                                let dest = parse_address(&address, capability_kind(&target, false))?;
                                link.destination = Some(dest.clone());
                                let producer = context.core.register_producer(conn, Some(dest.clone())).map_err(core_error)?;
                                link.sender = Some(producer);
                                response_target = target.clone();
                            } else {
                                // A null target address is an anonymous relay; messages carry their own `to`.
                                link.sender = Some(context.core.register_producer(conn, None).map_err(core_error)?);
                            }
                        }
                        links.insert((channel, handle), link);
                        let attach = described(0x12, list(vec![string(name), uint(handle), boolean(!receiver_role), uint(snd_settle), uint(0), response_source, response_target]));
                        tracing::debug!(?attach, "sending AMQP attach response");
                        write_frame(&mut stream, AMQP_FRAME, channel, &encode(&attach), peer_frame_max as usize).await?;
                        if !receiver_role || is_coordinator {
                            let flow = described(0x13, list(vec![null(), uint(0x7fffffff), uint(0), uint(0x7fffffff), uint(handle), uint(0), uint(1000), null(), boolean(false)]));
                            write_frame(&mut stream, AMQP_FRAME, channel, &encode(&flow), peer_frame_max as usize).await?;
                        }
                    }
                    Some(0x13) => {
                        let f = list_items(&frame.value).unwrap_or_default();
                        tracing::debug!(channel, ?f, "received AMQP flow fields");
                        if let Some(handle) = f.get(4).and_then(as_u32) {
                            let key = link_aliases.get(&(channel, handle)).copied().unwrap_or((channel, handle));
                            if let Some(link) = links.get_mut(&key) {
                                link.delivery_count = f.get(5).and_then(as_u32).unwrap_or(link.delivery_count);
                                let link_credit = f.get(6).and_then(as_u32).unwrap_or(0);
                                link.credit = link.delivery_count.saturating_add(link_credit).saturating_sub(link.next_delivery);
                                if let Some(consumer) = link.consumer { context.core.set_credit(consumer, link.credit); }
                                if f.get(8).and_then(as_bool).unwrap_or(false)
                                    && let Some(consumer) = link.consumer
                                {
                                    context.core.drain(consumer);
                                }
                            }
                        }
                    }
                    Some(0x14) => {
                        let f = list_items(&frame.value).unwrap_or_default();
                        let handle = f.first().and_then(as_u32).ok_or_else(|| invalid_data("transfer missing handle"))?;
                        let link_key = link_aliases.get(&(channel, handle)).copied().unwrap_or((channel, handle));
                        let first_frame = !incoming.contains_key(&link_key);
                        let delivery_id = if first_frame { f.get(1).and_then(as_u32).unwrap_or(remote_delivery) } else { incoming_delivery_ids.get(&link_key).copied().unwrap_or(remote_delivery) };
                        if first_frame { remote_delivery = delivery_id.wrapping_add(1); incoming_delivery_ids.insert(link_key, delivery_id); incoming_settled.insert(link_key, f.get(4).and_then(as_bool).unwrap_or(false)); incoming_transactions.insert(link_key, f.get(7).and_then(transactional_id)); }
                        let more = f.get(5).and_then(as_bool).unwrap_or(false);
                        let aborted = f.get(9).and_then(as_bool).unwrap_or(false);
                        let body = frame.payload_after_performative;
                        if aborted { incoming.remove(&link_key); incoming_delivery_ids.remove(&link_key); continue; }
                        let bytes = incoming.entry(link_key).or_default();
                        bytes.extend_from_slice(&body);
                        if more { continue; }
                        let bytes = incoming.remove(&link_key).unwrap_or_default();
                        let delivery_id = incoming_delivery_ids.remove(&link_key).unwrap_or(delivery_id);
                        let settled = incoming_settled.remove(&link_key).unwrap_or(false);
                        let tx_id = incoming_transactions.remove(&link_key).flatten();
                        let link = links.get(&link_key).ok_or_else(|| invalid_data("transfer on unknown handle"))?;
                        let conn = core_conn.ok_or_else(|| invalid_data("transfer before open"))?;
                        if link.coordinator {
                            let command = message_value_body(&bytes)?;
                            let response_state = match descriptor(&command) {
                                Some(0x31) => {
                                    let raw_id = delivery_id.to_be_bytes().to_vec();
                                    let tx = TxId::Local(hex::encode(&raw_id));
                                    context.core.begin(conn, tx.clone()).map_err(core_error)?;
                                    transactions.insert(raw_id.clone(), tx);
                                    described(0x33, list(vec![binary(raw_id)]))
                                }
                                Some(0x32) => {
                                    let fields = described_value(&command).and_then(|v|match v {Value::List(items)=>Some(items.as_slice()),_=>None}).unwrap_or_default();
                                    let id = fields.first().and_then(as_binary).ok_or_else(||invalid_data("discharge missing transaction id"))?;
                                    let tx = transactions.remove(&id).ok_or_else(||invalid_data("unknown transaction id"))?;
                                    if fields.get(1).and_then(as_bool).unwrap_or(false) { context.core.rollback(conn, &tx).map_err(core_error)?; } else { context.core.commit(conn, &tx, true).map_err(core_error)?; }
                                    described(0x24, list(vec![]))
                                }
                                _ => return Err(invalid_data("unknown transaction coordinator command")),
                            };
                            let response = described(0x15, list(vec![boolean(true), uint(delivery_id), null(), boolean(true), response_state]));
                            write_frame(&mut stream, AMQP_FRAME, channel, &encode(&response), peer_frame_max as usize).await?;
                        } else {
                            let (message, _) = decode_message(&bytes, link.destination.clone())?;
                            let tx = tx_id.as_ref().and_then(|id| transactions.get(id)).cloned();
                            tracing::debug!(destination = ?message.destination, "AMQP sending decoded message to core");
                            match context.core.send(conn, message, tx.as_ref()) {
                                Ok(()) if !settled => {
                                    let state = if let Some(tx) = tx { transactional_state(&tx) } else { described(0x24, list(vec![])) };
                                    let disp = described(0x15, list(vec![boolean(true), uint(delivery_id), null(), boolean(true), state]));
                                    write_frame(&mut stream, AMQP_FRAME, channel, &encode(&disp), peer_frame_max as usize).await?;
                                }
                                Ok(()) => {}
                                Err(error) => {
                                    let rejected = described(0x25, list(vec![described(0x1d, list(vec![symbol(error_condition(&error)), string(error.to_string())]))]));
                                    let disp = described(0x15, list(vec![boolean(true), uint(delivery_id), null(), boolean(true), rejected]));
                                    write_frame(&mut stream, AMQP_FRAME, channel, &encode(&disp), peer_frame_max as usize).await?;
                                }
                            }
                        }
                    }
                    Some(0x15) => {
                        let f = list_items(&frame.value).unwrap_or_default();
                        let role = f.first().and_then(as_bool).unwrap_or(false);
                        if !role {
                            let first = f.get(1).and_then(as_u32).unwrap_or(0);
                            let last = f.get(2).and_then(as_u32).unwrap_or(first);
                            let state = f.get(4).cloned().unwrap_or(Value::Null);
                            let _op = outcome_op(&state);
                            let ids = (first..=last).collect::<Vec<_>>();
                            for id in ids {
                                let _ = id;
                            }
                        } else {
                            let first = f.get(1).and_then(as_u32).unwrap_or(0);
                            let last = f.get(2).and_then(as_u32).unwrap_or(first);
                            let state = f.get(4).cloned().unwrap_or(Value::Null);
                            let op = outcome_op(&state);
                            for id in first..=last {
                                if let Some((link_channel, handle, consumer, core_delivery)) = local_unsettled.remove(&id) {
                                    if let Some(conn) = core_conn { context.core.ack(conn, consumer, Acks::Deliveries(vec![core_delivery]), op, outcome_transaction(&state).and_then(|id| transactions.get(&id))).map_err(core_error)?; }
                                    if let Some(link) = links.get_mut(&(link_channel, handle)) { link.credit = link.credit.saturating_add(1); if let Some(consumer) = link.consumer { context.core.set_credit(consumer, link.credit); } }
                                }
                            }
                        }
                    }
                    Some(0x16) => {
                        let f = list_items(&frame.value).unwrap_or_default();
                        let handle = f.first().and_then(as_u32).unwrap_or(0);
                        let closed = f.get(1).and_then(as_bool).unwrap_or(false);
                        let key = link_aliases.remove(&(channel, handle)).unwrap_or((channel, handle));
                        if let Some(link) = links.remove(&key) {
                            if let Some(consumer) = link.consumer { context.core.close_consumer(consumer); }
                            if let Some(producer) = link.sender { context.core.unregister_producer(producer); }
                            if closed && link.durable && let Some(conn) = core_conn { let _ = context.core.remove_durable(conn, &link.name); }
                        }
                        let response = described(0x16, list(vec![uint(handle), boolean(true), null()]));
                        write_frame(&mut stream, AMQP_FRAME, channel, &encode(&response), peer_frame_max as usize).await?;
                    }
                    Some(0x17) => {
                        write_frame(&mut stream, AMQP_FRAME, channel, &encode(&described(0x17, list(vec![null()]))), peer_frame_max as usize).await?;
                    }
                    Some(0x18) => {
                        write_frame(&mut stream, AMQP_FRAME, channel, &encode(&described(0x18, list(vec![null()]))), peer_frame_max as usize).await?;
                        break;
                    }
                    Some(0x30) => {
                        // Coordinator link: coordinator attach is handled by the normal attach path in a full peer; declare/discharge are accepted on incoming transfer.
                    }
                    _ => tracing::debug!(descriptor = ?descriptor(&frame.value), "ignoring unsupported AMQP performative"),
                }
            }
            event = async { match events.as_mut() { Some(rx) => rx.recv().await, None => std::future::pending().await } } => {
                match event {
                    Some(Event::Deliver(delivery)) => {
                        tracing::debug!(consumer = delivery.consumer, "AMQP core delivery event");
                        let link = links.values_mut().find(|l| l.consumer == Some(delivery.consumer));
                        if let Some(link) = link {
                            let id = outgoing_delivery;
                            outgoing_delivery = outgoing_delivery.wrapping_add(1);
                            let presettled = link.presettled;
                            send_transfer_message(&mut stream, link.channel, link.handle, id, presettled, &delivery.message, delivery.redelivered, peer_frame_max as usize).await?;
                            if presettled {
                                if let Some(conn) = core_conn { context.core.ack(conn, delivery.consumer, Acks::Deliveries(vec![delivery.delivery_id]), AckOp::Consumed, None).map_err(core_error)?; }
                            } else { local_unsettled.insert(id, (link.channel, link.handle, delivery.consumer, delivery.delivery_id)); }
                            link.next_delivery = link.next_delivery.wrapping_add(1);
                            link.credit = link.credit.saturating_sub(1);
                        }
                    }
                    Some(Event::Drained { consumer, credit }) => {
                        if let Some(link) = links.values_mut().find(|l| l.consumer == Some(consumer)) {
                            link.credit = 0;
                            let flow = described(0x13, list(vec![null(), uint(0x7fffffff), uint(0x7fffffff), uint(0), uint(link.handle), uint(link.next_delivery), uint(0), uint(0), boolean(true), null()]));
                            write_frame(&mut stream, AMQP_FRAME, link.channel, &encode(&flow), peer_frame_max as usize).await?;
                            let _ = credit;
                        }
                    }
                    Some(Event::ConnectionClosed { reason }) => {
                        let close = described(0x18, list(vec![described(0x1d, list(vec![symbol("amqp:internal-error"), string(reason)]))]));
                        write_frame(&mut stream, AMQP_FRAME, 0, &encode(&close), peer_frame_max as usize).await?;
                        break;
                    }
                    Some(Event::ConsumerClosed { consumer, .. }) => {
                        if let Some(link) = links.values_mut().find(|l| l.consumer == Some(consumer)) { link.consumer = None; }
                    }
                    Some(Event::BrowseDone { .. } | Event::PullEmpty { .. }) | None => {}
                }
            }
            _ = tokio::time::sleep_until(last_activity + std::time::Duration::from_secs(20)), if open_seen => {
                write_frame(&mut stream, AMQP_FRAME, 0, &[], peer_frame_max as usize).await?;
                last_activity = tokio::time::Instant::now();
            }
        }
    }
    Ok(())
}

struct ConnectionGuard {
    core: Arc<BrokerCore>,
    connection: u64,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.core.disconnect(self.connection);
    }
}

#[derive(Clone, Debug)]
struct Link {
    name: String,
    handle: u32,
    channel: u16,
    coordinator: bool,
    sender: Option<u64>,
    consumer: Option<u64>,
    destination: Option<Destination>,
    source: Value,
    target: Value,
    credit: u32,
    delivery_count: u32,
    next_delivery: u32,
    presettled: bool,
    durable: bool,
}

#[derive(Debug)]
struct Frame {
    frame_type: u8,
    channel: u16,
    value: Value,
    payload_after_performative: Vec<u8>,
}

async fn read_frame(stream: &mut TcpStream, max: usize) -> io::Result<Frame> {
    let mut head = [0u8; 8];
    stream.read_exact(&mut head).await?;
    let size = u32::from_be_bytes(head[..4].try_into().unwrap()) as usize;
    let doff = (head[4] as usize)
        .checked_mul(4)
        .ok_or_else(|| invalid_data("invalid frame data offset"))?;
    if size < 8 || size > max || doff < 8 || doff > size {
        return Err(invalid_data("invalid AMQP frame size or data offset"));
    }
    let mut rest = vec![0; size - 8];
    stream.read_exact(&mut rest).await?;
    let channel = u16::from_be_bytes([head[6], head[7]]);
    let payload = &rest[doff - 8..];
    if payload.is_empty() {
        return Ok(Frame {
            frame_type: head[5],
            channel,
            value: Value::Null,
            payload_after_performative: vec![],
        });
    }
    let (value, used) = decode(payload)?;
    Ok(Frame {
        frame_type: head[5],
        channel,
        value,
        payload_after_performative: payload[used..].to_vec(),
    })
}

async fn write_frame(
    stream: &mut TcpStream,
    frame_type: u8,
    channel: u16,
    payload: &[u8],
    max: usize,
) -> io::Result<()> {
    let size = 8usize
        .checked_add(payload.len())
        .ok_or_else(|| invalid_data("AMQP frame overflow"))?;
    if size > max {
        return Err(invalid_data("AMQP frame exceeds negotiated maximum"));
    }
    let mut frame = Vec::with_capacity(size);
    frame.extend_from_slice(&(size as u32).to_be_bytes());
    frame.push(2);
    frame.push(frame_type);
    frame.extend_from_slice(&channel.to_be_bytes());
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await
}

fn authenticate_probe(
    core: &Arc<BrokerCore>,
    user: Option<&str>,
    pass: Option<&str>,
    remote: std::net::SocketAddr,
) -> bool {
    match core.connect(ConnectInfo {
        username: user.map(str::to_owned),
        password: pass.map(str::to_owned),
        protocol: "amqp-auth".into(),
        remote_address: remote.to_string(),
        ..Default::default()
    }) {
        Ok((conn, _)) => {
            core.disconnect(conn);
            true
        }
        Err(_) => false,
    }
}

fn transactional_id(value: &Value) -> Option<Vec<u8>> {
    if descriptor(value) == Some(0x34) {
        list_items(value)?.first().and_then(as_binary)
    } else {
        None
    }
}

fn message_value_body(bytes: &[u8]) -> io::Result<Value> {
    let mut offset = 0;
    while offset < bytes.len() {
        let (section, used) = decode(&bytes[offset..])?;
        offset += used;
        if descriptor(&section) == Some(0x77) {
            return Ok(described_value(&section).cloned().unwrap_or(Value::Null));
        }
    }
    Err(invalid_data(
        "transaction coordinator transfer has no amqp-value body",
    ))
}

fn decode_message(
    bytes: &[u8],
    destination: Option<Destination>,
) -> io::Result<(Message, Option<Vec<u8>>)> {
    let mut offset = 0;
    let mut header = vec![];
    let mut properties = vec![];
    let mut annotations = vec![];
    let mut app = vec![];
    let mut body = None;
    while offset < bytes.len() {
        let (section, used) = decode(&bytes[offset..])?;
        offset += used;
        let Some(code) = descriptor(&section) else {
            continue;
        };
        let val = described_value(&section).cloned().unwrap_or(Value::Null);
        match code {
            0x70 => header = list_items_value(val),
            0x72 => annotations = map_entries(val),
            0x73 => properties = list_items_value(val),
            0x74 => app = map_entries(val),
            0x75..=0x77 => body = Some((code, val)),
            _ => {}
        }
    }
    let persistent = header.first().and_then(as_bool).unwrap_or(false);
    let priority = header.get(1).and_then(as_u32).unwrap_or(4).min(9) as u8;
    let ttl = header.get(2).and_then(as_u32).unwrap_or(0) as i64;
    let msg_id = properties
        .first()
        .and_then(message_id_string)
        .unwrap_or_else(crate::core::message::generate_message_id);
    let user_id = properties
        .get(1)
        .and_then(as_binary)
        .map(|v| String::from_utf8_lossy(&v).into_owned());
    let to = properties.get(2).and_then(as_string);
    let destination = match destination {
        Some(destination) => destination,
        None => parse_address(
            to.as_deref().ok_or_else(|| {
                invalid_data("anonymous relay message is missing the to property")
            })?,
            None,
        )?,
    };
    let subject = properties.get(3).and_then(as_string);
    let reply_to = properties
        .get(4)
        .and_then(as_string)
        .and_then(|v| parse_address(&v, None).ok());
    let correlation_id = properties.get(5).and_then(message_id_string);
    let content_type = properties.get(6).and_then(as_symbol_or_string);
    let creation = properties
        .get(creation_time_index())
        .and_then(as_i64)
        .unwrap_or_else(crate::core::message::now_millis);
    let expiry = properties
        .get(absolute_expiry_index())
        .and_then(as_i64)
        .unwrap_or(0);
    let mut message = Message::new(destination, Body::Empty);
    message.message_id = msg_id;
    message.persistent = persistent;
    message.priority = priority;
    message.timestamp = creation;
    message.expiration = if expiry > 0 {
        expiry
    } else if ttl > 0 {
        creation + ttl
    } else {
        0
    };
    message.user_id = user_id;
    message.correlation_id = correlation_id;
    message.message_type = subject.or(content_type);
    message.reply_to = reply_to;
    if let Some(group) = properties.get(group_id_index()).and_then(as_string) {
        message.group_id = Some(group);
    }
    if let Some(group_sequence) = properties.get(group_sequence_index()).and_then(as_u32) {
        message.group_sequence = group_sequence as i32;
    }
    for (key, value) in app {
        if let Some(key) = as_string(&key) {
            message.properties.insert(key, to_core_value(value));
        }
    }
    for (key, value) in annotations {
        if let Some(key) = as_symbol_or_string(&key) {
            if key == "x-opt-delivery-delay" {
                if let Some(delay) = as_i64(&value) {
                    message.properties.insert(
                        SCHEDULED_DELAY.to_owned(),
                        crate::core::message::Value::Long(delay),
                    );
                }
            } else if key == "x-opt-delivery-time" {
                if let Some(time) = as_i64(&value) {
                    message.properties.insert(
                        SCHEDULED_DELAY.to_owned(),
                        crate::core::message::Value::Long(
                            (time - crate::core::message::now_millis()).max(0),
                        ),
                    );
                }
            } else if key == "x-opt-jms-msg-type" {
                message.message_type = as_u32(&value).map(|v| v.to_string());
            }
        }
    }
    if let Some((kind, value)) = body {
        message.body = Arc::new(match (kind, value) {
            (0x75, Value::Binary(data)) => Body::Bytes(data),
            (0x77, Value::String(text)) => Body::Text(text),
            (0x77, Value::Map(map)) => Body::Map(
                map.into_iter()
                    .filter_map(|(k, v)| as_string(&k).map(|k| (k, to_core_value(v))))
                    .collect(),
            ),
            (0x76, Value::List(items)) => {
                Body::Stream(items.into_iter().map(to_core_value).collect())
            }
            (_, Value::Binary(data)) => Body::Bytes(data),
            (_, Value::String(text)) => Body::Text(text),
            _ => Body::Empty,
        });
    }
    Ok((message, None))
}

fn creation_time_index() -> usize {
    9
}
fn absolute_expiry_index() -> usize {
    8
}
fn group_id_index() -> usize {
    10
}
fn group_sequence_index() -> usize {
    11
}

fn encode_message(message: &Message, redelivered: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let header = described(
        0x70,
        list(vec![
            boolean(message.persistent),
            ubyte(message.priority),
            uint(if message.expiration > 0 {
                (message.expiration - message.timestamp).max(0) as u32
            } else {
                0
            }),
            boolean(redelivered),
            uint(message.redelivery_counter),
        ]),
    );
    out.extend_from_slice(&encode(&header));
    let props = described(
        0x73,
        list(vec![
            string(message.message_id.clone()),
            if let Some(id) = &message.user_id {
                binary(id.as_bytes().to_vec())
            } else {
                null()
            },
            string(message.destination.to_uri()),
            string_opt(message.message_type.clone()),
            message
                .reply_to
                .as_ref()
                .map(|d| string(d.to_uri()))
                .unwrap_or_else(null),
            message
                .correlation_id
                .as_ref()
                .map(|s| string(s.clone()))
                .unwrap_or_else(null),
            null(),
            null(),
            null(),
            timestamp(message.timestamp),
            if message.expiration > 0 {
                timestamp(message.expiration)
            } else {
                null()
            },
            message
                .group_id
                .as_ref()
                .map(|s| string(s.clone()))
                .unwrap_or_else(null),
            int(message.group_sequence),
        ]),
    );
    out.extend_from_slice(&encode(&props));
    if !message.properties.is_empty() {
        let entries = message
            .properties
            .iter()
            .map(|(k, v)| (string(k.clone()), from_core_value(v.clone())))
            .collect();
        out.extend_from_slice(&encode(&described(0x74, map(entries))));
    }
    match message.body.as_ref() {
        Body::Empty => {}
        Body::Text(text) => out.extend_from_slice(&encode(&described(0x77, string(text.clone())))),
        Body::Bytes(data) | Body::Object(data) => {
            out.extend_from_slice(&encode(&described(0x75, binary(data.clone()))))
        }
        Body::Map(values) => {
            let entries: Vec<(Value, Value)> = values
                .iter()
                .map(|(k, v)| (string(k.clone()), from_core_value(v.clone())))
                .collect();
            out.extend_from_slice(&encode(&described(0x77, map(entries))));
        }
        Body::Stream(values) => out.extend_from_slice(&encode(&described(
            0x76,
            list(values.iter().cloned().map(from_core_value).collect()),
        ))),
    }
    out
}

#[allow(clippy::too_many_arguments)]
async fn send_transfer_message(
    stream: &mut TcpStream,
    channel: u16,
    handle: u32,
    delivery_id: u32,
    settled: bool,
    message: &Message,
    redelivered: bool,
    max_frame: usize,
) -> io::Result<()> {
    let body = encode_message(message, redelivered);
    let first_performative = described(
        0x14,
        list(vec![
            uint(handle),
            uint(delivery_id),
            binary(delivery_id.to_be_bytes().to_vec()),
            uint(0),
            boolean(settled),
            boolean(false),
        ]),
    );
    let first_header = encode(&first_performative);
    let first_capacity = max_frame
        .checked_sub(8 + first_header.len())
        .ok_or_else(|| invalid_data("negotiated AMQP frame too small"))?;
    if body.len() <= first_capacity {
        let mut payload = first_header;
        payload.extend_from_slice(&body);
        return write_frame(stream, AMQP_FRAME, channel, &payload, max_frame).await;
    }
    let first_end = first_capacity.min(body.len());
    let mut first = encode(&described(
        0x14,
        list(vec![
            uint(handle),
            uint(delivery_id),
            binary(delivery_id.to_be_bytes().to_vec()),
            uint(0),
            boolean(settled),
            boolean(true),
        ]),
    ));
    first.extend_from_slice(&body[..first_end]);
    write_frame(stream, AMQP_FRAME, channel, &first, max_frame).await?;
    let mut sent = first_end;
    while sent < body.len() {
        let more = sent + max_frame.saturating_sub(40).max(1) < body.len();
        let performative = described(
            0x14,
            list(vec![
                uint(handle),
                null(),
                null(),
                null(),
                null(),
                boolean(more),
            ]),
        );
        let header = encode(&performative);
        let capacity = max_frame
            .checked_sub(8 + header.len())
            .ok_or_else(|| invalid_data("negotiated AMQP frame too small"))?;
        let end = (sent + capacity).min(body.len());
        let mut payload = header;
        payload.extend_from_slice(&body[sent..end]);
        write_frame(stream, AMQP_FRAME, channel, &payload, max_frame).await?;
        sent = end;
    }
    Ok(())
}

fn parse_address(address: &str, kind: Option<DestinationKind>) -> io::Result<Destination> {
    if let Some(destination) = Destination::parse_uri(address) {
        return Ok(destination);
    }
    if address.is_empty() {
        return Err(invalid_data("empty AMQP address"));
    }
    Ok(Destination::new(
        kind.unwrap_or(DestinationKind::Queue),
        address.to_owned(),
    ))
}
fn capability_kind(terminus: &Value, source: bool) -> Option<DestinationKind> {
    let capabilities = list_items(terminus)
        .and_then(|f| f.get(if source { 10 } else { 6 }).cloned())
        .map(|v| array_values(&v))
        .unwrap_or_default();
    capabilities
        .iter()
        .filter_map(as_symbol_or_string)
        .find_map(|s| match s.as_str() {
            "queue" => Some(DestinationKind::Queue),
            "topic" => Some(DestinationKind::Topic),
            "temporary-queue" => Some(DestinationKind::TempQueue),
            "temporary-topic" => Some(DestinationKind::TempTopic),
            _ => None,
        })
}
fn terminus_kind(term: &Value, source: bool) -> DestinationKind {
    capability_kind(term, source).unwrap_or(DestinationKind::TempQueue)
}
fn terminus_address(term: &Value) -> Option<String> {
    list_items(term).and_then(|f| f.first()).and_then(as_string)
}
fn terminus_dynamic(term: &Value) -> bool {
    list_items(term)
        .and_then(|f| f.get(4))
        .and_then(as_bool)
        .unwrap_or(false)
}
fn terminus_durable(term: &Value) -> u32 {
    list_items(term)
        .and_then(|f| f.get(1))
        .and_then(as_u32)
        .unwrap_or(0)
}
fn terminus_expiry_never(term: &Value) -> bool {
    list_items(term)
        .and_then(|f| f.get(2))
        .and_then(as_symbol_or_string)
        .is_some_and(|s| s == "never")
}
fn link_distribution_copy(term: &Value) -> bool {
    list_items(term)
        .and_then(|f| f.get(3))
        .and_then(as_symbol_or_string)
        .is_some_and(|s| s == "copy")
}
fn make_terminus(descriptor: u64, address: Option<String>, dynamic: bool) -> Value {
    let mut fields = vec![
        address.map(string).unwrap_or_else(null),
        uint(0),
        symbol("never"),
        null(),
        boolean(dynamic),
        null(),
    ];
    if descriptor == 0x28 {
        fields.extend([
            null(),
            null(),
            null(),
            null(),
            array_symbols(&["queue", "topic", "temporary-queue", "temporary-topic"]),
        ]);
    } else {
        fields.push(array_symbols(&[
            "queue",
            "topic",
            "temporary-queue",
            "temporary-topic",
        ]));
    }
    described(descriptor, list(fields))
}

fn filter_key_is(key: &Value, name: &str) -> bool {
    as_symbol_or_string(key)
        .is_some_and(|value| value == name || name == "jms-selector" && value == "selector")
        || matches!(
            (name, descriptor(key)),
            ("jms-selector", Some(0x468c00000004)) | ("no-local", Some(0x468c00000003))
        )
}
fn link_filter(source: &Value) -> Option<String> {
    list_items(source)
        .and_then(|fields| fields.get(7))
        .and_then(|filters| {
            map_entries(filters.clone())
                .into_iter()
                .find(|(key, _)| filter_key_is(key, "jms-selector"))
                .and_then(|(_, value)| match value {
                    Value::Described(_, body) => as_string(&body),
                    _ => None,
                })
        })
}
fn has_filter(source: &Value, filter_name: &str) -> bool {
    list_items(source)
        .and_then(|fields| fields.get(7))
        .is_some_and(|filters| {
            map_entries(filters.clone())
                .iter()
                .any(|(key, _)| filter_key_is(key, filter_name))
        })
}
fn error_condition(error: &crate::core::types::CoreError) -> &'static str {
    use crate::core::types::CoreError::*;
    match error {
        Security(_) => "amqp:unauthorized-access",
        NotFound(_) => "amqp:not-found",
        InvalidDestination(_) => "amqp:invalid-field",
        InvalidSelector(_) => "amqp:invalid-field",
        _ => "amqp:internal-error",
    }
}
fn core_error(error: crate::core::types::CoreError) -> io::Error {
    io::Error::other(error.to_string())
}

fn outcome_op(value: &Value) -> AckOp {
    match descriptor(value) {
        Some(0x24) => AckOp::Consumed,
        Some(0x26) => AckOp::Release { increment: false },
        Some(0x27) => {
            let fields = list_items(value).unwrap_or_default();
            if fields.get(1).and_then(as_bool).unwrap_or(false) {
                AckOp::Reject
            } else if fields.first().and_then(as_bool).unwrap_or(false) {
                AckOp::Release { increment: true }
            } else {
                AckOp::Release { increment: false }
            }
        }
        Some(0x25) => AckOp::Reject,
        _ => AckOp::Consumed,
    }
}
fn outcome_transaction(value: &Value) -> Option<Vec<u8>> {
    if descriptor(value) == Some(0x34) {
        list_items(value)
            .and_then(|f| f.first())
            .and_then(as_binary)
    } else {
        None
    }
}
fn transactional_state(tx: &TxId) -> Value {
    let id = match tx {
        TxId::Local(s) => hex::decode(s).unwrap_or_else(|_| s.as_bytes().to_vec()),
        TxId::Xa(x) => [x.global_id.clone(), x.branch_id.clone()].concat(),
    };
    described(0x34, list(vec![binary(id), described(0x24, list(vec![]))]))
}

// AMQP value model. The same representation backs all primitive/container encoding.
#[derive(Clone, Debug, PartialEq)]
enum Value {
    Null,
    Bool(bool),
    U8(u8),
    U16(u16),
    Decimal32([u8; 4]),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal64([u8; 8]),
    Decimal128([u8; 16]),
    Char(char),
    Timestamp(i64),
    Uuid([u8; 16]),
    Binary(Vec<u8>),
    String(String),
    Symbol(String),
    List(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Array(Vec<Value>),
    Described(Box<Value>, Box<Value>),
}

fn null() -> Value {
    Value::Null
}
fn boolean(v: bool) -> Value {
    Value::Bool(v)
}
fn ubyte(v: u8) -> Value {
    Value::U8(v)
}
fn uint(v: u32) -> Value {
    Value::U32(v)
}
fn int(v: i32) -> Value {
    Value::I32(v)
}
fn timestamp(v: i64) -> Value {
    Value::Timestamp(v)
}
fn string(v: impl Into<String>) -> Value {
    Value::String(v.into())
}
fn string_opt(v: Option<String>) -> Value {
    v.map(string).unwrap_or_else(null)
}
fn symbol(v: impl Into<String>) -> Value {
    Value::Symbol(v.into())
}
fn binary(v: Vec<u8>) -> Value {
    Value::Binary(v)
}
fn list(v: Vec<Value>) -> Value {
    Value::List(v)
}
fn map(v: Vec<(Value, Value)>) -> Value {
    Value::Map(v)
}
fn described(d: u64, v: Value) -> Value {
    Value::Described(Box::new(Value::U64(d)), Box::new(v))
}
fn array_symbols(items: &[&str]) -> Value {
    Value::Array(items.iter().map(|v| symbol(*v)).collect())
}
fn descriptor(v: &Value) -> Option<u64> {
    if let Value::Described(d, _) = v {
        match d.as_ref() {
            Value::U64(v) => Some(*v),
            Value::U32(v) => Some(*v as u64),
            Value::U8(v) => Some(*v as u64),
            Value::Symbol(name) => match name.as_str() {
                "amqp:coordinator:list" => Some(0x30),
                "amqp:declare:list" => Some(0x31),
                "amqp:discharge:list" => Some(0x32),
                "amqp:declared:list" => Some(0x33),
                "amqp:transactional-state:list" => Some(0x34),
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    }
}
fn described_value(v: &Value) -> Option<&Value> {
    if let Value::Described(_, v) = v {
        Some(v)
    } else {
        None
    }
}
fn list_items(v: &Value) -> Option<&[Value]> {
    match described_value(v).unwrap_or(v) {
        Value::List(v) => Some(v),
        _ => None,
    }
}
fn list_items_value(v: Value) -> Vec<Value> {
    match v {
        Value::List(v) => v,
        _ => vec![],
    }
}
fn array_values(v: &Value) -> Vec<Value> {
    match v {
        Value::Array(v) => v.clone(),
        _ => vec![],
    }
}
fn map_entries(v: Value) -> Vec<(Value, Value)> {
    match v {
        Value::Map(v) => v,
        _ => vec![],
    }
}
fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(v) => Some(*v),
        _ => None,
    }
}
fn as_binary(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Binary(v) => Some(v.clone()),
        _ => None,
    }
}
fn as_string(v: &Value) -> Option<String> {
    match v {
        Value::String(v) | Value::Symbol(v) => Some(v.clone()),
        _ => None,
    }
}
fn as_symbol_or_string(v: &Value) -> Option<String> {
    as_string(v)
}
fn as_u32(v: &Value) -> Option<u32> {
    match v {
        Value::U8(v) => Some(*v as u32),
        Value::U16(v) => Some(*v as u32),
        Value::U32(v) => Some(*v),
        Value::U64(v) => Some(*v as u32),
        Value::I32(v) => u32::try_from(*v).ok(),
        _ => None,
    }
}
fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::I8(v) => Some(*v as i64),
        Value::I16(v) => Some(*v as i64),
        Value::I32(v) => Some(*v as i64),
        Value::I64(v) | Value::Timestamp(v) => Some(*v),
        Value::U8(v) => Some(*v as i64),
        Value::U16(v) => Some(*v as i64),
        Value::U32(v) => Some(*v as i64),
        Value::U64(v) => i64::try_from(*v).ok(),
        _ => None,
    }
}
fn message_id_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) | Value::Symbol(s) => Some(s.clone()),
        Value::U64(i) => Some(i.to_string()),
        Value::U32(i) => Some(i.to_string()),
        Value::Binary(b) => Some(hex::encode(b)),
        _ => None,
    }
}
fn to_core_value(v: Value) -> crate::core::message::Value {
    use crate::core::message::Value as C;
    match v {
        Value::Null => C::Null,
        Value::Bool(v) => C::Bool(v),
        Value::U8(v) => C::Byte(v as i8),
        Value::U16(v) => C::Int(v as i32),
        Value::I8(v) => C::Byte(v),
        Value::I16(v) => C::Short(v),
        Value::U32(v) => C::Long(v as i64),
        Value::I32(v) => C::Int(v),
        Value::U64(v) => C::Long(v as i64),
        Value::I64(v) | Value::Timestamp(v) => C::Long(v),
        Value::F32(v) => C::Float(v),
        Value::F64(v) => C::Double(v),
        Value::Decimal32(v) => C::Bytes(v.to_vec()),
        Value::Decimal64(v) => C::Bytes(v.to_vec()),
        Value::Decimal128(v) => C::Bytes(v.to_vec()),
        Value::Char(v) => C::Char(v as u16),
        Value::Binary(v) => C::Bytes(v),
        Value::String(v) | Value::Symbol(v) => C::String(v),
        Value::List(v) | Value::Array(v) => C::List(v.into_iter().map(to_core_value).collect()),
        Value::Map(v) => C::Map(
            v.into_iter()
                .filter_map(|(k, v)| as_string(&k).map(|k| (k, to_core_value(v))))
                .collect(),
        ),
        Value::Described(_, v) => to_core_value(*v),
        Value::Uuid(v) => C::Bytes(v.to_vec()),
    }
}
fn from_core_value(v: crate::core::message::Value) -> Value {
    use crate::core::message::Value as C;
    match v {
        C::Null => Value::Null,
        C::Bool(v) => Value::Bool(v),
        C::Byte(v) => Value::I8(v),
        C::Short(v) => Value::I16(v),
        C::Char(v) => Value::Char(char::from_u32(v as u32).unwrap_or('\0')),
        C::Int(v) => Value::I32(v),
        C::Long(v) => Value::I64(v),
        C::Float(v) => Value::F32(v),
        C::Double(v) => Value::F64(v),
        C::String(v) => Value::String(v),
        C::Bytes(v) => Value::Binary(v),
        C::Map(v) => Value::Map(
            v.into_iter()
                .map(|(k, v)| (Value::String(k), from_core_value(v)))
                .collect(),
        ),
        C::List(v) => Value::List(v.into_iter().map(from_core_value).collect()),
    }
}

fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    put_value(&mut out, value);
    out
}
fn put_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.push(0x40),
        Value::Bool(true) => out.push(0x41),
        Value::Bool(false) => out.push(0x42),
        Value::U16(n) => {
            out.push(0x60);
            out.extend_from_slice(&n.to_be_bytes())
        }
        Value::Decimal32(b) => {
            out.push(0x74);
            out.extend_from_slice(b)
        }
        Value::Decimal64(b) => {
            out.push(0x84);
            out.extend_from_slice(b)
        }
        Value::Decimal128(b) => {
            out.push(0x94);
            out.extend_from_slice(b)
        }
        Value::U8(n) => {
            out.push(0x50);
            out.push(*n)
        }
        Value::U32(0) => out.push(0x43),
        Value::U32(n) if *n <= 255 => {
            out.push(0x52);
            out.push(*n as u8)
        }
        Value::U32(n) => {
            out.push(0x70);
            out.extend_from_slice(&n.to_be_bytes())
        }
        Value::U64(0) => out.push(0x44),
        Value::U64(n) if *n <= 255 => {
            out.push(0x53);
            out.push(*n as u8)
        }
        Value::U64(n) => {
            out.push(0x80);
            out.extend_from_slice(&n.to_be_bytes())
        }
        Value::I8(n) => {
            out.push(0x51);
            out.push(*n as u8)
        }
        Value::I16(n) => {
            out.push(0x61);
            out.extend_from_slice(&n.to_be_bytes())
        }
        Value::I32(0) => out.push(0x54),
        Value::I32(n) if (-128..=127).contains(n) => {
            out.push(0x55);
            out.push(*n as i8 as u8)
        }
        Value::I32(n) => {
            out.push(0x71);
            out.extend_from_slice(&n.to_be_bytes())
        }
        Value::I64(0) => out.push(0x44),
        Value::I64(n) => {
            out.push(0x81);
            out.extend_from_slice(&n.to_be_bytes())
        }
        Value::F32(n) => {
            out.push(0x72);
            out.extend_from_slice(&n.to_bits().to_be_bytes())
        }
        Value::F64(n) => {
            out.push(0x82);
            out.extend_from_slice(&n.to_bits().to_be_bytes())
        }
        Value::Char(c) => {
            out.push(0x73);
            out.extend_from_slice(&(*c as u32).to_be_bytes())
        }
        Value::Timestamp(n) => {
            out.push(0x83);
            out.extend_from_slice(&n.to_be_bytes())
        }
        Value::Uuid(u) => {
            out.push(0x98);
            out.extend_from_slice(u)
        }
        Value::Binary(b) => put_variable(out, 0xa0, 0xb0, b),
        Value::String(s) => put_variable(out, 0xa1, 0xb1, s.as_bytes()),
        Value::Symbol(s) => put_variable(out, 0xa3, 0xb3, s.as_bytes()),
        Value::List(v) => put_container(
            out,
            0xc0,
            0xd0,
            &v.iter().flat_map(encode).collect::<Vec<_>>(),
            v.len(),
        ),
        Value::Map(v) => {
            let b = v
                .iter()
                .flat_map(|(k, v)| [encode(k), encode(v)].concat())
                .collect::<Vec<_>>();
            put_container(out, 0xc1, 0xd1, &b, v.len() * 2)
        }
        Value::Array(v) => put_array(out, v),
        Value::Described(d, v) => {
            out.push(0);
            put_value(out, d);
            put_value(out, v)
        }
    }
}
fn put_variable(out: &mut Vec<u8>, small: u8, large: u8, b: &[u8]) {
    if b.len() <= 255 {
        out.push(small);
        out.push(b.len() as u8)
    } else {
        out.push(large);
        out.extend_from_slice(&(b.len() as u32).to_be_bytes())
    }
    out.extend_from_slice(b)
}
fn put_container(out: &mut Vec<u8>, small: u8, large: u8, b: &[u8], count: usize) {
    if b.len() < 255 && count <= 255 {
        out.push(small);
        out.push((b.len() + 1) as u8);
        out.push(count as u8)
    } else {
        out.push(large);
        out.extend_from_slice(&((b.len() + 4) as u32).to_be_bytes());
        out.extend_from_slice(&(count as u32).to_be_bytes())
    }
    out.extend_from_slice(b)
}
fn put_array(out: &mut Vec<u8>, values: &[Value]) {
    let (code, body) = if values.is_empty() {
        (0x40, Vec::new())
    } else {
        let code = match values.first().unwrap() {
            Value::Null => 0x40,
            Value::Bool(_) => 0x56,
            Value::U8(_) => 0x50,
            Value::U16(_) => 0x60,
            Value::U32(_) => 0x70,
            Value::U64(_) => 0x80,
            Value::I8(_) => 0x51,
            Value::I16(_) => 0x61,
            Value::I32(_) => 0x71,
            Value::I64(_) => 0x81,
            Value::F32(_) => 0x72,
            Value::F64(_) => 0x82,
            Value::Decimal32(_) => 0x74,
            Value::Decimal64(_) => 0x84,
            Value::Decimal128(_) => 0x94,
            Value::Char(_) => 0x73,
            Value::Timestamp(_) => 0x83,
            Value::Uuid(_) => 0x98,
            Value::Binary(_) => 0xb0,
            Value::String(_) => 0xb1,
            Value::Symbol(_) => 0xb3,
            _ => 0x40,
        };
        let mut body = Vec::new();
        for value in values {
            put_array_element(&mut body, value, code);
        }
        (code, body)
    };
    if body.len() + 2 <= 255 && values.len() <= 255 {
        out.extend_from_slice(&[0xe0, (body.len() + 2) as u8, values.len() as u8, code]);
    } else {
        out.push(0xf0);
        out.extend_from_slice(&((body.len() + 5) as u32).to_be_bytes());
        out.extend_from_slice(&(values.len() as u32).to_be_bytes());
        out.push(code);
    }
    out.extend_from_slice(&body);
}

fn put_array_element(out: &mut Vec<u8>, value: &Value, code: u8) {
    match (code, value) {
        (0x40, Value::Null) => {}
        (0x56, Value::Bool(v)) => out.push(*v as u8),
        (0x50, Value::U8(v)) => out.push(*v),
        (0x60, Value::U16(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (0x70, Value::U32(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (0x80, Value::U64(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (0x51, Value::I8(v)) => out.push(*v as u8),
        (0x61, Value::I16(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (0x71, Value::I32(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (0x81, Value::I64(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (0x72, Value::F32(v)) => out.extend_from_slice(&v.to_bits().to_be_bytes()),
        (0x82, Value::F64(v)) => out.extend_from_slice(&v.to_bits().to_be_bytes()),
        (0x74, Value::Decimal32(v)) => out.extend_from_slice(v),
        (0x84, Value::Decimal64(v)) => out.extend_from_slice(v),
        (0x94, Value::Decimal128(v)) => out.extend_from_slice(v),
        (0x73, Value::Char(v)) => out.extend_from_slice(&(*v as u32).to_be_bytes()),
        (0x83, Value::Timestamp(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (0x98, Value::Uuid(v)) => out.extend_from_slice(v),
        (0xb0, Value::Binary(v)) => {
            out.extend_from_slice(&(v.len() as u32).to_be_bytes());
            out.extend_from_slice(v);
        }
        (0xb1, Value::String(v)) | (0xb3, Value::Symbol(v)) => {
            out.extend_from_slice(&(v.len() as u32).to_be_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        _ => {
            let encoded = encode(value);
            if encoded.first() == Some(&code) {
                out.extend_from_slice(&encoded[1..]);
            } else {
                out.extend_from_slice(&encoded);
            }
        }
    }
}

fn decode(data: &[u8]) -> io::Result<(Value, usize)> {
    let mut offset = 0;
    let value = get_value(data, &mut offset, None)?;
    Ok((value, offset))
}
fn get_value(data: &[u8], offset: &mut usize, forced: Option<u8>) -> io::Result<Value> {
    let code = if let Some(c) = forced {
        c
    } else {
        let c = *data
            .get(*offset)
            .ok_or_else(|| invalid_data("truncated AMQP value"))?;
        *offset += 1;
        c
    };
    macro_rules! take {
        ($n:expr) => {{
            let end = offset
                .checked_add($n)
                .ok_or_else(|| invalid_data("AMQP size overflow"))?;
            let s = data
                .get(*offset..end)
                .ok_or_else(|| invalid_data("truncated AMQP value"))?;
            *offset = end;
            s
        }};
    }
    macro_rules! num {
        ($n:expr,$ty:ty) => {{
            let b = take!($n);
            <$ty>::from_be_bytes(b.try_into().unwrap())
        }};
    }
    Ok(match code {
        0x00 => {
            let d = get_value(data, offset, None)?;
            let v = get_value(data, offset, None)?;
            Value::Described(Box::new(d), Box::new(v))
        }
        0x40 => Value::Null,
        0x45 => Value::List(vec![]),
        0x41 => Value::Bool(true),
        0x42 => Value::Bool(false),
        0x56 => Value::Bool(take!(1)[0] != 0),
        0x43 => Value::U32(0),
        0x44 => Value::U64(0),
        0x50 => Value::U8(take!(1)[0]),
        0x51 => Value::I8(take!(1)[0] as i8),
        0x52 => Value::U32(take!(1)[0] as u32),
        0x53 => Value::U64(take!(1)[0] as u64),
        0x54 => Value::I32(0),
        0x55 => Value::I32(take!(1)[0] as i8 as i32),
        0x60 => Value::U16(num!(2, u16)),
        0x61 => Value::I16(num!(2, i16)),
        0x74 => Value::Decimal32(take!(4).try_into().unwrap()),
        0x84 => Value::Decimal64(take!(8).try_into().unwrap()),
        0x94 => Value::Decimal128(take!(16).try_into().unwrap()),
        0x70 => Value::U32(num!(4, u32)),
        0x71 => Value::I32(num!(4, i32)),
        0x72 => Value::F32(f32::from_bits(num!(4, u32))),
        0x73 => Value::Char(char::from_u32(num!(4, u32)).unwrap_or('\0')),
        0x80 => Value::U64(num!(8, u64)),
        0x81 => Value::I64(num!(8, i64)),
        0x82 => Value::F64(f64::from_bits(num!(8, u64))),
        0x83 => Value::Timestamp(num!(8, i64)),
        0x98 => Value::Uuid(take!(16).try_into().unwrap()),
        0xa0 | 0xa1 | 0xa3 => {
            let n = take!(1)[0] as usize;
            variable(code, take!(n))?
        }
        0xb0 | 0xb1 | 0xb3 => {
            let n = num!(4, u32) as usize;
            variable(code, take!(n))?
        }
        0xc0 | 0xc1 | 0xe0 => {
            let n = take!(1)[0] as usize;
            let end = *offset + n;
            let count = take!(1)[0] as usize;
            container(code, data, offset, end, count)?
        }
        0xd0 | 0xd1 | 0xf0 => {
            let n = num!(4, u32) as usize;
            let end = offset
                .checked_add(n)
                .ok_or_else(|| invalid_data("AMQP size overflow"))?;
            let count = num!(4, u32) as usize;
            container(code, data, offset, end, count)?
        }
        _ => return Err(invalid_data("unsupported AMQP type code")),
    })
}
fn variable(code: u8, b: &[u8]) -> io::Result<Value> {
    Ok(match code {
        0xa0 | 0xb0 => Value::Binary(b.to_vec()),
        0xa1 | 0xb1 => Value::String(
            String::from_utf8(b.to_vec()).map_err(|_| invalid_data("invalid UTF-8 AMQP string"))?,
        ),
        0xa3 | 0xb3 => Value::Symbol(
            String::from_utf8(b.to_vec()).map_err(|_| invalid_data("invalid UTF-8 AMQP symbol"))?,
        ),
        _ => return Err(invalid_data("invalid variable type")),
    })
}
fn container(
    code: u8,
    data: &[u8],
    offset: &mut usize,
    end: usize,
    count: usize,
) -> io::Result<Value> {
    if end > data.len() {
        return Err(invalid_data("truncated AMQP container"));
    }
    let value = match code {
        0xc0 | 0xd0 => {
            let mut items = Vec::new();
            for _ in 0..count {
                items.push(get_value(data, offset, None)?)
            }
            Value::List(items)
        }
        0xc1 | 0xd1 => {
            let mut items = Vec::new();
            for _ in 0..count / 2 {
                let k = get_value(data, offset, None)?;
                let v = get_value(data, offset, None)?;
                items.push((k, v))
            }
            Value::Map(items)
        }
        0xe0 | 0xf0 => {
            let type_code = *data
                .get(*offset)
                .ok_or_else(|| invalid_data("truncated AMQP array"))?;
            *offset += 1;
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(get_value(data, offset, Some(type_code))?)
            }
            Value::Array(values)
        }
        _ => return Err(invalid_data("invalid container code")),
    };
    if *offset > end {
        return Err(invalid_data("AMQP container overrun"));
    }
    *offset = end;
    Ok(value)
}
fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn round_trips_primitives_described_values_and_containers() {
        let values = [
            Value::Null,
            Value::Bool(true),
            Value::U8(7),
            Value::U16(1024),
            Value::Decimal32([1, 2, 3, 4]),
            Value::U32(65_536),
            Value::U64(1 << 40),
            Value::I8(-7),
            Value::I16(-1024),
            Value::I32(-65_536),
            Value::I64(-1 << 40),
            Value::F32(1.25),
            Value::F64(-2.5),
            Value::Decimal64([1, 2, 3, 4, 5, 6, 7, 8]),
            Value::Decimal128([9; 16]),
            Value::Char('λ'),
            Value::Timestamp(1_700_000_000_000),
            Value::Uuid([0x42; 16]),
            Value::Binary(vec![0, 1, 0xff]),
            Value::String("AMQP λ".into()),
            Value::Symbol("amqp:test".into()),
            Value::List(vec![Value::U32(1), Value::String("two".into())]),
            Value::Map(vec![(Value::Symbol("key".into()), Value::Bool(false))]),
            Value::Array(vec![
                Value::Symbol("one".into()),
                Value::Symbol("two".into()),
            ]),
            Value::Described(
                Box::new(Value::U64(0x1234)),
                Box::new(Value::List(vec![Value::Null, Value::U32(42)])),
            ),
        ];

        for value in values {
            let encoded = encode(&value);
            let (decoded, consumed) = decode(&encoded).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, encoded.len());
        }
    }
}
