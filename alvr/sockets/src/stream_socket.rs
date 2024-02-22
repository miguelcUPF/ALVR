// Note: for StreamSocket, the client uses a server socket, the server uses a client socket.
// This is because of certificate management. The server needs to trust a client and its certificate
//
// StreamSender and StreamReceiver endpoints allow for convenient conversion of the header to/from
// bytes while still handling the additional byte buffer with zero copies and extra allocations.

// Performance analysis:
// We want to minimize the transmission time for various sizes of packets.
// The current code locks the write socket *per shard* and not *per packet*. This leds to the best
// performance outcome given that the possible packets can be either very small (one shard) or very
// large (hundreds/thousands of shards, for video). if we don't allow interleaving shards, a very
// small packet will need to wait a long time before getting received if there was an ongoing
// transmission of a big packet before. If we allow interleaving shards, small packets can be
// transmitted quicker, with only minimal latency increase for the ongoing transmission of the big
// packet.
// Note: We can't clone the underlying socket for each StreamSender and the mutex around the socket
// cannot be removed. This is because we need to make sure at least shards are written whole.

use crate::backend::{tcp, udp, SocketReader, SocketWriter};
use alvr_common::{
    anyhow::Result, debug, parking_lot::Mutex, AnyhowToCon, ConResult, HandleTryAgain, ToCon,
};
use alvr_session::{DscpTos, SocketBufferSize, SocketProtocol};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    mem,
    net::{IpAddr, TcpListener, UdpSocket},
    sync::{mpsc, Arc},
    time::Duration,
};

const SHARD_PREFIX_SIZE: usize = mem::size_of::<u32>() // packet length - field itself (4 bytes)
    + mem::size_of::<u16>() // stream ID
    + mem::size_of::<u32>() // packet index
    + mem::size_of::<u32>() // shards count
    + mem::size_of::<u32>(); // shards index

/// Memory buffer that contains a hidden prefix
#[derive(Default)]
pub struct Buffer<H = ()> {
    inner: Vec<u8>,
    hidden_offset: usize, // this corresponds to prefix + header
    length: usize,
    _phantom: PhantomData<H>,
}

impl<H> Buffer<H> {
    /// Length of payload (without prefix)
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the whole payload of the buffer
    pub fn get(&self) -> &[u8] {
        &self.inner[self.hidden_offset..][..self.length]
    }

    /// If the range is outside the valid range, new space will be allocated
    /// NB: the offset parameter is applied on top of the internal offset of the buffer
    pub fn get_range_mut(&mut self, offset: usize, size: usize) -> &mut [u8] {
        let required_size = self.hidden_offset + offset + size;
        if required_size > self.inner.len() {
            self.inner.resize(required_size, 0);
        }

        self.length = self.length.max(offset + size);

        &mut self.inner[self.hidden_offset + offset..][..size]
    }

    /// If length > current length, allocate more space
    pub fn set_len(&mut self, length: usize) {
        self.inner.resize(self.hidden_offset + length, 0);
        self.length = length;
    }
}

#[derive(Clone)]
pub struct StreamSender<H> {
    inner: Arc<Mutex<Box<dyn SocketWriter>>>,
    stream_id: u16,
    max_packet_size: usize,
    prev_packet_index: u32,
    used_buffers: Vec<Vec<u8>>,
    _phantom: PhantomData<H>,
}

impl<H> StreamSender<H> {
    /// Shard and send a buffer with zero copies and zero allocations.
    /// The prefix of each shard is written over the previously sent shard to avoid reallocations.
    pub fn send(&mut self, mut buffer: Buffer<H>) -> Result<()> {
        let max_shard_data_size = self.max_packet_size - SHARD_PREFIX_SIZE;
        let actual_buffer_size = buffer.hidden_offset + buffer.length;
        let data_size = actual_buffer_size - SHARD_PREFIX_SIZE;
        let shards_count = (data_size as f32 / max_shard_data_size as f32).ceil() as usize;

        let packet_index = self.prev_packet_index + 1;
        if packet_index == u32::MAX {
            // If the next index is the maximum value, wrap around to 0
            packet_index = 0;
        }

        for idx in 0..shards_count {
            // this overlaps with the previous shard, this is intended behavior and allows to
            // reduce allocations
            let packet_start_position = idx * max_shard_data_size;
            let sub_buffer = &mut buffer.inner[packet_start_position..];

            // NB: true shard length (account for last shard that is smaller)
            let packet_length = usize::min(
                self.max_packet_size,
                actual_buffer_size - packet_start_position,
            );

            // todo: switch to little endian
            // todo: do not remove sizeof<u32> for packet length
            sub_buffer[0..4]
                .copy_from_slice(&((packet_length - mem::size_of::<u32>()) as u32).to_be_bytes());
            sub_buffer[4..6].copy_from_slice(&self.stream_id.to_be_bytes());
            sub_buffer[6..10].copy_from_slice(&packet_index.to_be_bytes());
            sub_buffer[10..14].copy_from_slice(&(shards_count as u32).to_be_bytes());
            sub_buffer[14..18].copy_from_slice(&(idx as u32).to_be_bytes());

            self.inner.lock().send(&sub_buffer[..packet_length])?;
        }

        self.prev_packet_index = packet_index;

        self.used_buffers.push(buffer.inner);

        Ok(())
    }
}

impl<H: Serialize> StreamSender<H> {
    pub fn get_buffer(&mut self, header: &H) -> Result<Buffer<H>> {
        let mut buffer = self.used_buffers.pop().unwrap_or_default();

        let header_size = bincode::serialized_size(header)? as usize;
        let hidden_offset = SHARD_PREFIX_SIZE + header_size;

        if buffer.len() < hidden_offset {
            buffer.resize(hidden_offset, 0);
        }

        bincode::serialize_into(&mut buffer[SHARD_PREFIX_SIZE..hidden_offset], header)?;

        Ok(Buffer {
            inner: buffer,
            hidden_offset,
            length: 0,
            _phantom: PhantomData,
        })
    }

    pub fn send_header(&mut self, header: &H) -> Result<()> {
        let buffer = self.get_buffer(header)?;
        self.send(buffer)
    }
}

pub struct ReceiverData<H> {
    buffer: Option<Vec<u8>>,
    size: usize, // including the prefix

    used_buffer_queue: mpsc::Sender<Vec<u8>>,
    _phantom: PhantomData<H>,

    // Packet metrics
    packet_span: Duration,
    packet_shard_interval_average: Duration,

    reception_interval: Duration,

    bytes_received: usize,
    shards_received: usize,

    packets_lost_discarded: usize,
    packets_discarded: usize,

    shards_duplicated: usize,

    highest_packet_index: u32,
    highest_shard_index: u32,
}

impl<H> ReceiverData<H> {
    
    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn get_packet_span(&self) -> Duration {
        self.packet_span
    }

    pub fn get_packet_shard_interval_average(&self) -> Duration {
        self.packet_shard_interval_average
    }

    pub fn get_reception_interval(&self) -> usize {
        self.reception_interval
    }

    pub fn get_bytes_received(&self) -> usize {
        self.bytes_received
    }

    pub fn get_shards_received(&self) -> usize {
        self.shards_received
    }

    pub fn get_packets_lost_discarded(&self) -> usize {
        self.packets_lost_discarded
    }

    pub fn get_packets_discarded(&self) -> usize {
        self.packets_discarded
    }

    pub fn get_shards_duplicated(&self) -> usize {
        self.shards_duplicated
    }

    pub fn get_highest_packet_index(&self) -> usize {
        self.highest_packet_index
    }

    pub fn get_highest_shard_index(&self) -> usize {
        self.highest_shard_index
    }
}

impl<H: DeserializeOwned> ReceiverData<H> {
    pub fn get(&self) -> Result<(H, &[u8])> {
        let mut data: &[u8] = &self.buffer.as_ref().unwrap()[SHARD_PREFIX_SIZE..self.size];
        // This will partially consume the slice, leaving only the actual payload
        let header = bincode::deserialize_from(&mut data)?;

        Ok((header, data))
    }
    pub fn get_header(&self) -> Result<H> {
        Ok(self.get()?.0)
    }
}

impl<H> Drop for ReceiverData<H> {
    fn drop(&mut self) {
        self.used_buffer_queue
            .send(self.buffer.t: boolake().unwrap())
            .ok();
    }
}

struct ReconstructedPacket {
    index: u32,
    buffer: Vec<u8>,
    size: usize, // including the prefix

    // Packet metrics
    packet_span: Duration,
    packet_shard_interval_average: Duration,

    reception_interval: Duration,

    bytes_received: usize,
    shards_received: usize,

    shards_duplicated: usize,

    highest_packet_index: u32,
    highest_shard_index: u32,

}

pub struct StreamReceiver<H> {
    packet_receiver: mpsc::Receiver<ReconstructedPacket>,
    used_buffer_queue: mpsc::Sender<Vec<u8>>,
    prev_packet_index: u32,
    _phantom: PhantomData<H>,

    packets_lost_discarded: usize, // non-cumulative
    packets_discarded: usize, // non-cumulative
}

fn wrapping_cmp(lhs: u32, rhs: u32) -> Ordering {
    let diff = lhs.wrapping_sub(rhs);
    if diff == 0 {
        Ordering::Equal
    } else if diff < u32::MAX / 2 {
        Ordering::Greater
    } else {
        // if diff > u32::MAX / 2, it means the sub operation wrapped
        Ordering::Less
    }
}

/// Get next packet reconstructing from shards.
/// Returns true if a packet has been recontructed and copied into the buffer.
impl<H: DeserializeOwned + Serialize> StreamReceiver<H> {
    pub fn recv(&mut self, timeout: Duration) -> ConResult<ReceiverData<H>> {
        let packet = self
            .packet_receiver
            .recv_timeout(timeout)
            .handle_try_again()?;

        let mut packet_index = self.prev_packet_index + 1;
        if packet_index == u32::MAX {
            // If the next index is the maximum value, wrap around to 0
            packet_index = 0;
        }

    // Use wrapping arithmetics
        match wrapping_cmp(packet.index, packet_index) {
            Ordering::Equal => (),
            Ordering::Greater => {
                // Skipped some indices
                self.packets_lost_discarded += packet.index.wrapping_sub(packet_index);
            }
            Ordering::Less => {
                // Old packet, discard
                self.packets_discarded  += 1;
                self.used_buffer_queue.send(packet.buffer).to_con()?;
                return alvr_common::try_again();
            }

        }

        let lost_discarded = self.packets_lost_discarded;
        let discarded = self.packets_discarded;
        
        self.prev_packet_index = packet.index;
        self.packets_lost_discarded = 0;
        self.packets_discarded = 0;

        Ok(ReceiverData {
            buffer: Some(packet.buffer),
            size: packet.size,
            used_buffer_queue: self.used_buffer_queue.clone(),
            _phantom: PhantomData,
            packet_span: packet.packet_span,
            packet_shard_interval_average: packet.packet_shard_interval_average,
            reception_interval: packet.reception_interval,
            bytes_received: packet.bytes_received,
            shards_received: packet.shards_received,
            packets_lost_discarded: lost_discarded,
            packets_discarded: discarded,
            shards_duplicated: packet.shards_duplicated,
            highest_packet_index: packet.highest_packet_index,
            highest_shard_index: packet.highest_shard_index,
        })

    }
}

pub enum StreamSocketBuilder {
    Tcp(TcpListener),
    Udp(UdpSocket),
}

impl StreamSocketBuilder {
    pub fn listen_for_server(
        timeout: Duration,
        port: u16,
        stream_socket_config: SocketProtocol,
        stream_tos_config: Option<DscpTos>,
        send_buffer_bytes: SocketBufferSize,
        recv_buffer_bytes: SocketBufferSize,
    ) -> Result<Self> {
        Ok(match stream_socket_config {
            SocketProtocol::Udp => StreamSocketBuilder::Udp(udp::bind(
                port,
                stream_tos_config,
                send_buffer_bytes,
                recv_buffer_bytes,
            )?),
            SocketProtocol::Tcp => StreamSocketBuilder::Tcp(tcp::bind(
                timeout,
                port,
                stream_tos_config,
                send_buffer_bytes,
                recv_buffer_bytes,
            )?),
        })
    }

    pub fn accept_from_server(
        self,
        server_ip: IpAddr,
        port: u16,
        max_packet_size: usize,
        timeout: Duration,
    ) -> ConResult<StreamSocket> {
        let (send_socket, receive_socket): (Box<dyn SocketWriter>, Box<dyn SocketReader>) =
            match self {
                StreamSocketBuilder::Udp(socket) => {
                    let (send_socket, receive_socket) =
                        udp::connect(&socket, server_ip, port, timeout).to_con()?;

                    (Box::new(send_socket), Box::new(receive_socket))
                }
                StreamSocketBuilder::Tcp(listener) => {
                    let (send_socket, receive_socket) =
                        tcp::accept_from_server(&listener, Some(server_ip), timeout)?;

                    (Box::new(send_socket), Box::new(receive_socket))
                }
            };

        Ok(StreamSocket {
            // +4 is a workaround to retain compatibilty with old protocol
            // todo: remove +4
            max_packet_size: max_packet_size + 4,
            send_socket: Arc::new(Mutex::new(send_socket)),
            receive_socket,
            shard_recv_state: None,
            stream_recv_components: HashMap::new(),

            packets_shard_instant: HashMap::new(),
            prev_packet_instant: HashMap::new(),
            bytes_received: HashMap::new(),
            shards_received: HashMap::new(),
            shards_duplicated: HashMap::new(),
            highest_packet_index: HashMap::new(),
            highest_shard_index: HashMap::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn connect_to_client(
        timeout: Duration,
        client_ip: IpAddr,
        port: u16,
        protocol: SocketProtocol,
        dscp: Option<DscpTos>,
        send_buffer_bytes: SocketBufferSize,
        recv_buffer_bytes: SocketBufferSize,
        max_packet_size: usize,
    ) -> ConResult<StreamSocket> {
        let (send_socket, receive_socket): (Box<dyn SocketWriter>, Box<dyn SocketReader>) =
            match protocol {
                SocketProtocol::Udp => {
                    let socket =
                        udp::bind(port, dscp, send_buffer_bytes, recv_buffer_bytes).to_con()?;
                    let (send_socket, receive_socket) =
                        udp::connect(&socket, client_ip, port, timeout).to_con()?;

                    (Box::new(send_socket), Box::new(receive_socket))
                }
                SocketProtocol::Tcp => {
                    let (send_socket, receive_socket) = tcp::connect_to_client(
                        timeout,
                        &[client_ip],
                        port,
                        send_buffer_bytes,
                        recv_buffer_bytes,
                    )?;

                    (Box::new(send_socket), Box::new(receive_socket))
                }
            };

        Ok(StreamSocket {
            // +4 is a workaround to retain compatibilty with old protocol
            // todo: remove +4
            max_packet_size: max_packet_size + 4,
            send_socket: Arc::new(Mutex::new(send_socket)),
            receive_socket,
            shard_recv_state: None,
            stream_recv_components: HashMap::new(),

            packets_shard_instant: HashMap::new(),
            prev_packet_instant: HashMap::new(),
            bytes_received: HashMap::new(),
            shards_received: HashMap::new(),
            shards_duplicated: HashMap::new(),
            highest_packet_index: HashMap::new(),
            highest_shard_index: HashMap::new(),
        })
    }
}

struct RecvState {
    shard_length: usize, // contains prefix length itself
    stream_id: u16,
    packet_index: u32,
    shards_count: usize,
    shard_index: usize,
    packet_cursor: usize, // counts also the prefix bytes
    overwritten_data_backup: Option<[u8; SHARD_PREFIX_SIZE]>,
    should_discard: bool,
}

struct InProgressPacket {
    buffer: Vec<u8>,
    buffer_length: usize,
    received_shard_indices: HashSet<usize>,
}

struct StreamRecvComponents {
    used_buffer_sender: mpsc::Sender<Vec<u8>>,
    used_buffer_receiver: mpsc::Receiver<Vec<u8>>,
    packet_queue: mpsc::Sender<ReconstructedPacket>,
    in_progress_packets: HashMap<u32, InProgressPacket>,
    discarded_shards_sink: InProgressPacket,
}

// Note: used buffers don't *have* to be split by stream ID, but doing so improves memory usage
// todo: impose cap on number of created buffers to avoid OOM crashes
pub struct StreamSocket {
    max_packet_size: usize,
    send_socket: Arc<Mutex<Box<dyn SocketWriter>>>,
    receive_socket: Box<dyn SocketReader>,
    shard_recv_state: Option<RecvState>,
    stream_recv_components: HashMap<u16, StreamRecvComponents>,

    // Streams metrics
    packets_shard_instant: HashMap<u16, HashMap<u32, VecDeque<Instant>>>,
    prev_packet_instant: HashMap<u16, Instant>,
    bytes_received: HashMap<u16, usize>, // non-cumulative
    shards_received: HashMap<u16, usize>, // non-cumulative
    shards_duplicated: HashMap<u16, usize>, // non-cumulative
    highest_packet_index: HashMap<u16, u32>, //todo
    highest_shard_index: HashMap<u16, u32>, //todo
}

impl StreamSocket {
    pub fn request_stream<T>(&self, stream_id: u16) -> StreamSender<T> {
        StreamSender {
            inner: Arc::clone(&self.send_socket),
            stream_id,
            max_packet_size: self.max_packet_size,
            prev_packet_index: 0,
            used_buffers: vec![],
            _phantom: PhantomData,
        }
    }

    // max_concurrent_buffers: number of buffers allocated by this call which will be reused to
    // receive packets for this stream ID. If packets are not read fast enough, the shards received
    // for this particular stream will be discarded
    pub fn subscribe_to_stream<T>(
        &mut self,
        stream_id: u16,
        max_concurrent_buffers: usize,
    ) -> StreamReceiver<T> {
        let (packet_sender, packet_receiver) = mpsc::channel();
        let (used_buffer_sender, used_buffer_receiver) = mpsc::channel();

        for _ in 0..max_concurrent_buffers {
            used_buffer_sender.send(vec![]).ok();
        }

        self.stream_recv_components.insert(
            stream_id,
            StreamRecvComponents {
                used_buffer_sender: used_buffer_sender.clone(),
                used_buffer_receiver,
                packet_queue: packet_sender,
                in_progress_packets: HashMap::new(),
                discarded_shards_sink: InProgressPacket {
                    buffer: vec![],
                    buffer_length: 0,
                    received_shard_indices: HashSet::new(),
                },
            },
        );

        StreamReceiver {
            packet_receiver,
            used_buffer_queue: used_buffer_sender,
            _phantom: PhantomData,
            prev_packet_index: 0,
            packets_lost_discarded: 0,
            packets_discarded: 0,
        }
    }

    pub fn recv(&mut self) -> ConResult {
        let shard_recv_state_mut = if let Some(state) = &mut self.shard_recv_state {
            state
        } else {
            let mut bytes = [0; SHARD_PREFIX_SIZE];
            let count = self.receive_socket.peek(&mut bytes)?;
            if count < SHARD_PREFIX_SIZE {
                return alvr_common::try_again();
            }

            // todo: switch to little endian
            // todo: do not remove sizeof<u32> for packet length
            let shard_length: usize = mem::size_of::<u32>()
                + u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
            let stream_id = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
            let packet_index = u32::from_be_bytes(bytes[6..10].try_into().unwrap());
            let shards_count = u32::from_be_bytes(bytes[10..14].try_into().unwrap()) as usize;
            let shard_index = u32::from_be_bytes(bytes[14..18].try_into().unwrap()) as usize;

            let now = Instant::now();

            self.shard_recv_state.insert(RecvState {
                shard_length,
                stream_id,
                packet_index,
                shards_count,
                shard_index,
                packet_cursor: 0,
                overwritten_data_backup: None,
                should_discard: false,
            })
        };

        let Some(components) = self
            .stream_recv_components
            .get_mut(&shard_recv_state_mut.stream_id)
        else {
            debug!(
                "Received packet from stream {} before subscribing!",
                shard_recv_state_mut.stream_id
            );
            return alvr_common::try_again();
        };

        let in_progress_packet = if shard_recv_state_mut.should_discard {
            &mut components.discarded_shards_sink
        } else if let Some(packet) = components
            .in_progress_packets
            .get_mut(&shard_recv_state_mut.packet_index)
        {
            packet
        } else if let Some(buffer) = components.used_buffer_receiver.try_recv().ok().or_else(|| {
            // By default, try to dequeue a used buffer. In case none were found, recycle one of the
            // in progress packets, chances are these buffers are "dead" because one of their shards
            // has been dropped by the network.
            let idx = *components.in_progress_packets.iter().next()?.0;
            Some(components.in_progress_packets.remove(&idx).unwrap().buffer)
        }) {
            // NB: Can't use entry pattern because we want to allow bailing out on the line above
            components.in_progress_packets.insert(
                shard_recv_state_mut.packet_index,
                InProgressPacket {
                    buffer,
                    buffer_length: 0,
                    // todo: find a way to skipping this allocation
                    received_shard_indices: HashSet::with_capacity(
                        shard_recv_state_mut.shards_count,
                    ),
                },
            );
            components
                .in_progress_packets
                .get_mut(&shard_recv_state_mut.packet_index)
                .unwrap()
        } else {
            // This branch may be hit in case the thread related to the stream hangs for some reason
            shard_recv_state_mut.should_discard = true;
            shard_recv_state_mut.packet_cursor = 0; // reset cursor from old shards
                                                    // always write at the start of the packet so the buffer doesn't grow much
            shard_recv_state_mut.shard_index = 0;

            &mut components.discarded_shards_sink
        };

        let max_shard_data_size = self.max_packet_size - SHARD_PREFIX_SIZE;
        // Note: there is no prefix offset, since we want to write the prefix too.
        let packet_start_index = shard_recv_state_mut.shard_index * max_shard_data_size;

        // Prepare buffer to accomodate receiving shard
        {
            // Note: this contains the prefix offset
            in_progress_packet.buffer_length = usize::max(
                in_progress_packet.buffer_length,
                packet_start_index + shard_recv_state_mut.shard_length,
            );

            if in_progress_packet.buffer.len() < in_progress_packet.buffer_length {
                in_progress_packet
                    .buffer
                    .resize(in_progress_packet.buffer_length, 0);
            }
        }

        let sub_buffer = &mut in_progress_packet.buffer[packet_start_index..];

        // Read shard into the single contiguous buffer
        {
            // Backup the small section of bytes that will be overwritten by reading from socket.
            if shard_recv_state_mut.overwritten_data_backup.is_none() {
                shard_recv_state_mut.overwritten_data_backup =
                    Some(sub_buffer[..SHARD_PREFIX_SIZE].try_into().unwrap())
            }

            // This loop may bail out at any time if a timeout is reached. This is correctly handled by
            // the previous code.
            while shard_recv_state_mut.packet_cursor < shard_recv_state_mut.shard_length {
                let size = self.receive_socket.recv(
                    &mut sub_buffer
                        [shard_recv_state_mut.packet_cursor..shard_recv_state_mut.shard_length],
                )?;
                shard_recv_state_mut.packet_cursor += size;
            }

            // Restore backed up bytes
            // Safety: overwritten_data_backup is always set just before receiving the packet
            sub_buffer[..SHARD_PREFIX_SIZE]
                .copy_from_slice(&shard_recv_state_mut.overwritten_data_backup.take().unwrap());
        }

        let highest_packet_index = self.highest_packet_index.entry(shard_recv_state_mut.stream_id).or_insert(0);
        let highest_shard_index = self.highest_shard_index.entry(shard_recv_state_mut.stream_id).or_insert(0);

        if (*highest_packet_index == shard_recv_state_mut.packet_index) {
            if (*highest_shard_index < shard_recv_state_mut.shard_index) {
                *highest_shard_index = shard_recv_state_mut.shard_index;
            } else{

            }
        } else if (*highest_packet_index < shard_recv_state_mut.packet_index) {
            *highest_packet_index = shard_recv_state_mut.packet_index;
            *highest_shard_index = shard_recv_state_mut.shard_index;
        }

        let bytes_received = self.bytes_received.entry(shard_recv_state_mut.stream_id).or_insert(0);
        *bytes_received += shard_recv_state_mut.shard_length + SHARD_PREFIX_SIZE;

        let shards_received = self.shards_received.entry(shard_recv_state_mut.stream_id).or_insert(0);
        *shards_received += 1;
        

        if !shard_recv_state_mut.should_discard {
            if !in_progress_packet.received_shard_indices.contains(&shard_recv_state_mut.shard_index) {
                in_progress_packet.received_shard_indices.insert(shard_recv_state_mut.shard_index);

                self.packets_shard_instant.entry(shard_recv_state_mut.stream_id).
                    or_insert_with(HashMap::new).entry(shard_recv_state_mut.packet_id).
                    or_insert_with(VecDeque::new).push_back(now);
            }
            else {
                let shards_duplicated = self.shards_duplicated.entry(shard_recv_state_mut.stream_id).or_insert(0);
                *shards_duplicated += 1;
            }
        }

        // Check if packet is complete and send
        if in_progress_packet.received_shard_indices.len() == shard_recv_state_mut.shards_count {
            let mut reception_interval = Duration::from_secs(0);

            let mut packet_span = Duration::from_secs(0);
            let mut packet_shard_interval_average = Duration::from_secs(0);

            if let Some(packet_instants) = self.packets_shard_instant.entry(shard_recv_state_mut.stream_id).remove(&shard_recv_state_mut.packet_id) {
                let mut packet_shard_interval_sum = Duration::from_secs(0);
                let mut packet_shard_count = packet_instants.len();

                if shard_recv_state_mut.shards_count != packet_shard_count {
                    debug!(
                        "Shard instants for packet {} in stream {} not properly gathered.",
                        shard_recv_state_mut.packet_id, shard_recv_state_mut.stream_id
                    );
                }

                if let (Some(first_instant), Some(last_instant)) = (packet_instants.front(), packet_instants.back()) {

                    let mut prev_instant = first_instant;
                    packet_span = last_instant.duration_since(first_instant);
            
                    for &instant in packet_instants.iter().skip(1) {
                        let interval = instant.duration_since(prev_instant);
                        packet_shard_interval_sum += interval;
                        prev_instant = instant;
                    }
            
                    packet_shard_interval_average = if packet_shard_count > 1 {
                        packet_shard_interval_sum / (packet_shard_count - 1) as u32
                    };
                    let prev_packet_instant = self.prev_packet_instant.entry(shard_recv_state_mut.stream_id).or_insert(first_instant);
                    reception_interval = last_instant.duration_since(*prev_packet_instant);

                    if last_instant != now { //remove
                        debug!(
                            "Something went wrong. Last instant is not equal to now!"
                        );
                    }
            }

            let size = in_progress_packet.buffer_length;
            components
                .packet_queue
                .send(ReconstructedPacket {
                    index: shard_recv_state_mut.packet_index,
                    buffer: components
                        .in_progress_packets
                        .remove(&shard_recv_state_mut.packet_index)
                        .unwrap()
                        .buffer,
                    size,
                    packet_span,
                    packet_shard_interval_average,
                    reception_interval,
                    *bytes_received,
                    *shards_received,
                    *shards_duplicated,
                    *highest_packet_index,
                    *highest_shard_index,
                })
                .ok();
            
            *prev_packet_instant = now; // now should be equal to last_instant
            *bytes_received = 0;
            *shards_received = 0;
            *shards_duplicated = 0;

            // Keep only shards with later packet index (using wrapping logic)
            while let Some((idx, _)) = components.in_progress_packets.iter().find(|(idx, _)| {
                wrapping_cmp(**idx, shard_recv_state_mut.packet_index) == Ordering::Less
            }) {
                let idx = *idx; // fix borrow rule
                let packet = components.in_progress_packets.remove(&idx).unwrap();

                // Recycle buffer
                components.used_buffer_sender.send(packet.buffer).ok();
            }
        }

        // Mark current shard as read and allow for a new shard to be read
        self.shard_recv_state = None;

        Ok(())
    }
}
}