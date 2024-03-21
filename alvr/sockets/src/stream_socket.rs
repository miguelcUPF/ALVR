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
    anyhow::Result, parking_lot::Mutex, warn, AnyhowToCon, ConResult, HandleTryAgain, ToCon,
};
use alvr_packets::VIDEO;
use alvr_session::{DscpTos, SocketBufferSize, SocketProtocol};
use indexmap::IndexMap;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    marker::PhantomData,
    mem,
    net::{IpAddr, TcpListener, UdpSocket},
    sync::{mpsc, Arc},
    time::{Duration, Instant},
};

const SHARD_PREFIX_SIZE: usize = mem::size_of::<u32>() // shard length - field itself (4 bytes)
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
    max_shard_size: usize,
    next_packet_index: u32,

    used_buffers: Vec<Vec<u8>>,
    _phantom: PhantomData<H>,

    // Video stream metrics
    shards_bytes: Option<IndexMap<u16, usize>>, // including prefix
    shards_instant: Option<IndexMap<u16, Instant>>,
}

impl<H> StreamSender<H> {
    pub fn get_next_packet_index(&self) -> u32 {
        self.next_packet_index
    }

    pub fn get_shards_bytes(&self) -> Option<IndexMap<u16, usize>> {
        self.shards_bytes.clone()
    }

    pub fn get_shards_instant(&self) -> Option<IndexMap<u16, Instant>> {
        self.shards_instant.clone()
    }

    /// Shard and send a buffer with zero copies and zero allocations.
    /// The prefix of each shard is written over the previously sent shard to avoid reallocations.
    pub fn send(&mut self, mut buffer: Buffer<H>) -> Result<()> {
        if let (Some(shards_bytes), Some(shards_instant)) =
            (&mut self.shards_bytes, &mut self.shards_instant)
        {
            shards_bytes.clear();
            shards_instant.clear();
        }
        let max_shard_data_size = self.max_shard_size - SHARD_PREFIX_SIZE;
        let actual_buffer_size = buffer.hidden_offset + buffer.length;
        let data_size = actual_buffer_size - SHARD_PREFIX_SIZE;
        let shards_count = (data_size as f32 / max_shard_data_size as f32).ceil() as usize;

        for idx in 0..shards_count {
            // this overlaps with the previous shard, this is intended behavior and allows to
            // reduce allocations
            let shard_start_position = idx * max_shard_data_size;
            let sub_buffer = &mut buffer.inner[shard_start_position..];

            // NB: true shard length (account for last shard that is smaller)
            let shard_length = usize::min(
                self.max_shard_size,
                actual_buffer_size - shard_start_position,
            );

            // todo: switch to little endian
            // todo: do not remove sizeof<u32> for packet length
            sub_buffer[0..4]
                .copy_from_slice(&((shard_length - mem::size_of::<u32>()) as u32).to_be_bytes());
            sub_buffer[4..6].copy_from_slice(&self.stream_id.to_be_bytes());
            sub_buffer[6..10].copy_from_slice(&self.next_packet_index.to_be_bytes());
            sub_buffer[10..14].copy_from_slice(&(shards_count as u32).to_be_bytes());
            sub_buffer[14..18].copy_from_slice(&(idx as u32).to_be_bytes());

            self.inner.lock().send(&sub_buffer[..shard_length])?;

            if let (Some(shards_bytes), Some(shards_instant)) =
                (&mut self.shards_bytes, &mut self.shards_instant)
            {
                let now = Instant::now();
                shards_bytes.insert(idx as u16, shard_length);
                shards_instant.insert(idx as u16, now);
            }
        }

        self.next_packet_index = self.next_packet_index.wrapping_add(1);

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

    index: u32,
    had_packet_skip: bool,

    // Video stream metrics
    reception_interval: Option<Duration>,
    highest_packet_index: Option<u32>,
    highest_shard_index: Option<usize>,

    //// Interval metrics
    bytes_received: Option<usize>,
    shards_received: Option<usize>,
    shards_dropped: Option<usize>,
    shards_duplicated: Option<usize>,
    packets_skipped: Option<usize>,
    packets_discarded: Option<usize>,

    //// Packet metrics
    packet_span: Option<Duration>,
    shards_interval_average: Option<f32>,
    shards_jitter: Option<f32>,
}

impl<H> ReceiverData<H> {
    pub fn had_packet_skip(&self) -> bool {
        self.had_packet_skip
    }

    pub fn get_index(&self) -> u32 {
        self.index
    }

    pub fn get_highest_packet_index(&self) -> Option<u32> {
        self.highest_packet_index
    }

    pub fn get_highest_shard_index(&self) -> Option<usize> {
        self.highest_shard_index
    }

    pub fn get_reception_interval(&self) -> Option<Duration> {
        self.reception_interval
    }

    pub fn get_bytes_received(&self) -> Option<usize> {
        self.bytes_received
    }

    pub fn get_shards_received(&self) -> Option<usize> {
        self.shards_received
    }

    pub fn get_shards_dropped(&self) -> Option<usize> {
        self.shards_dropped
    }

    pub fn get_shards_duplicated(&self) -> Option<usize> {
        self.shards_duplicated
    }

    pub fn get_packets_skipped(&self) -> Option<usize> {
        self.packets_skipped
    }

    pub fn get_packets_discarded(&self) -> Option<usize> {
        self.packets_discarded
    }

    pub fn get_packet_span(&self) -> Option<Duration> {
        self.packet_span
    }

    pub fn get_shards_interval_average(&self) -> Option<f32> {
        self.shards_interval_average
    }

    pub fn get_shards_jitter(&self) -> Option<f32> {
        self.shards_jitter
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
            .send(self.buffer.take().unwrap())
            .ok();
    }
}

struct ReconstructedPacket {
    index: u32,
    buffer: Vec<u8>,
    size: usize, // including the prefix

    // Video stream metrics
    highest_packet_index: Option<u32>,
    highest_shard_index: Option<usize>,
    video_packet_metrics: Option<VideoPacketMetrics>,
}

pub struct StreamReceiver<H> {
    stream_id: u16,
    packet_receiver: mpsc::Receiver<ReconstructedPacket>,
    used_buffer_queue: mpsc::Sender<Vec<u8>>,
    next_packet_index: u32,
    _phantom: PhantomData<H>,

    // Video stream metrics
    reception_interval: Option<Duration>,

    //// Interval metrics
    bytes_received: Option<usize>,
    shards_received: Option<usize>,
    shards_dropped: Option<usize>,
    shards_duplicated: Option<usize>,
    packets_skipped: Option<usize>,
    packets_discarded: Option<usize>,

    //// Packet metrics
    packet_span: Option<Duration>,
    shards_interval_average: Option<f32>,
    shards_jitter: Option<f32>,
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

        if let Some(video_packet_metrics) = packet.video_packet_metrics {
            self.reception_interval = self
                .reception_interval
                .map(|x| x + video_packet_metrics.reception_interval);
            self.bytes_received = self
                .bytes_received
                .map(|x| x + video_packet_metrics.bytes_received);
            self.shards_received = self
                .shards_received
                .map(|x| x + video_packet_metrics.shards_received);
            self.shards_dropped = self
                .shards_dropped
                .map(|x| x + video_packet_metrics.shards_dropped);
            self.shards_duplicated = self
                .shards_duplicated
                .map(|x| x + video_packet_metrics.shards_duplicated);
            self.packet_span = Some(video_packet_metrics.packet_span);
            self.shards_interval_average = Some(video_packet_metrics.shards_interval_average);
            self.shards_jitter = Some(video_packet_metrics.shards_jitter);
        }

        let mut had_packet_skip = false;

        // Use wrapping arithmetics
        match wrapping_cmp(packet.index, self.next_packet_index) {
            Ordering::Equal => (),
            Ordering::Greater => {
                // Skipped some indices
                had_packet_skip = true;
                if self.stream_id == VIDEO {
                    self.packets_skipped = self
                        .packets_skipped
                        .map(|x| x + packet.index.wrapping_sub(self.next_packet_index) as usize);
                    warn!("Skipped {} video packets!", self.packets_skipped.unwrap());
                }
            }
            Ordering::Less => {
                // Old packet, discard
                if self.stream_id == VIDEO {
                    self.packets_discarded = self.packets_discarded.map(|x| x + 1);
                    warn!("Discarded video packet {}!", packet.index);
                }
                self.used_buffer_queue.send(packet.buffer).to_con()?;
                return alvr_common::try_again();
            }
        }

        self.next_packet_index = packet.index.wrapping_add(1);

        Ok(ReceiverData {
            buffer: Some(packet.buffer),
            size: packet.size,
            used_buffer_queue: self.used_buffer_queue.clone(),
            _phantom: PhantomData,

            had_packet_skip,
            index: packet.index,

            // Video stream metrics
            highest_packet_index: packet.highest_packet_index,
            highest_shard_index: packet.highest_shard_index,
            reception_interval: self.reception_interval,

            //// Interval metrics
            bytes_received: self.bytes_received,
            shards_received: self.shards_received,
            shards_dropped: self.shards_dropped,
            shards_duplicated: self.shards_duplicated,
            packets_skipped: self.packets_skipped,
            packets_discarded: self.packets_discarded,

            //// Packet metrics
            packet_span: self.packet_span,
            shards_interval_average: self.shards_interval_average,
            shards_jitter: self.shards_jitter,
        })
    }

    pub fn clear_video_stream_metrics(&mut self) {
        self.reception_interval = Some(Duration::ZERO);

        self.bytes_received = Some(0);
        self.shards_received = Some(0);
        self.shards_dropped = Some(0);
        self.shards_duplicated = Some(0);
        self.packets_skipped = Some(0);
        self.packets_discarded = Some(0);

        self.packet_span = Some(Duration::ZERO);
        self.shards_interval_average = Some(0.0);
        self.shards_jitter = Some(0.0);
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
        max_shard_size: usize,
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
            max_shard_size: max_shard_size + 4,
            send_socket: Arc::new(Mutex::new(send_socket)),
            receive_socket,
            shard_recv_state: None,
            stream_recv_components: HashMap::new(),

            video_stream_stats: VideoStreamStats::default(),
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
        max_shard_size: usize,
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
            max_shard_size: max_shard_size + 4,
            send_socket: Arc::new(Mutex::new(send_socket)),
            receive_socket,
            shard_recv_state: None,
            stream_recv_components: HashMap::new(),

            video_stream_stats: VideoStreamStats::default(),
        })
    }
}

struct RecvState {
    shard_length: usize, // including prefix
    stream_id: u16,
    packet_index: u32,
    shards_count: usize,
    shard_index: usize,
    bytes_cursor: usize, // counts also the prefix bytes
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

#[derive(Clone, Copy)]
pub struct VideoPacketMetrics {
    reception_interval: Duration,

    // Interval metrics
    bytes_received: usize,    // non-cumulative
    shards_received: usize,   // non-cumulative
    shards_dropped: usize,    // non-cumulative
    shards_duplicated: usize, // non-cumulative

    // Packet metrics
    packet_span: Duration,
    shards_interval_average: f32,
    shards_jitter: f32,
}

impl Default for VideoPacketMetrics {
    fn default() -> Self {
        VideoPacketMetrics {
            reception_interval: Duration::ZERO,

            // Interval metrics
            bytes_received: 0,
            shards_received: 0,
            shards_dropped: 0,
            shards_duplicated: 0,

            // Packet metrics
            packet_span: Duration::ZERO,
            shards_interval_average: 0.0,
            shards_jitter: 0.0,
        }
    }
}

pub struct VideoStreamStats {
    packet_shards_instant: HashMap<u32, VecDeque<Instant>>,
    highest_packet_index: u32,
    highest_shard_index: usize,

    prev_packet_instant: Instant,

    last_packet_metrics: VideoPacketMetrics,
}

impl Default for VideoStreamStats {
    fn default() -> Self {
        VideoStreamStats {
            packet_shards_instant: HashMap::new(),
            highest_packet_index: 0,
            highest_shard_index: 0,

            prev_packet_instant: Instant::now(),

            last_packet_metrics: VideoPacketMetrics::default(),
        }
    }
}

// Note: used buffers don't *have* to be split by stream ID, but doing so improves memory usage
// todo: impose cap on number of created buffers to avoid OOM crashes
pub struct StreamSocket {
    max_shard_size: usize,
    send_socket: Arc<Mutex<Box<dyn SocketWriter>>>,
    receive_socket: Box<dyn SocketReader>,
    shard_recv_state: Option<RecvState>,
    stream_recv_components: HashMap<u16, StreamRecvComponents>,

    video_stream_stats: VideoStreamStats,
}

impl StreamSocket {
    pub fn request_stream<T>(&self, stream_id: u16) -> StreamSender<T> {
        let (shards_bytes, shards_instant) = if stream_id == VIDEO {
            (Some(IndexMap::new()), Some(IndexMap::new()))
        } else {
            (None, None)
        };
        StreamSender {
            inner: Arc::clone(&self.send_socket),

            stream_id,
            max_shard_size: self.max_shard_size,
            next_packet_index: 0,

            used_buffers: vec![],
            _phantom: PhantomData,

            shards_bytes,
            shards_instant,
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

        let (
            reception_interval,
            bytes_received,
            shards_received,
            shards_dropped,
            shards_duplicated,
            packets_skipped,
            packets_discarded,
            packet_span,
            shards_interval_average,
            shards_jitter,
        ) = if stream_id == VIDEO {
            (
                Some(Duration::ZERO),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(Duration::ZERO),
                Some(0.0),
                Some(0.0),
            )
        } else {
            (None, None, None, None, None, None, None, None, None, None)
        };

        StreamReceiver {
            stream_id,
            packet_receiver,
            used_buffer_queue: used_buffer_sender,
            _phantom: PhantomData,
            next_packet_index: 0,

            reception_interval,

            bytes_received,
            shards_received,
            shards_dropped,
            shards_duplicated,
            packets_skipped,
            packets_discarded,

            packet_span,
            shards_interval_average,
            shards_jitter,
        }
    }

    pub fn recv(&mut self) -> ConResult {
        let now = Instant::now();

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
            let shard_length = mem::size_of::<u32>()
                + u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
            let stream_id = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
            let packet_index = u32::from_be_bytes(bytes[6..10].try_into().unwrap());
            let shards_count = u32::from_be_bytes(bytes[10..14].try_into().unwrap()) as usize;
            let shard_index = u32::from_be_bytes(bytes[14..18].try_into().unwrap()) as usize;

            if stream_id == VIDEO {
                self.video_stream_stats.last_packet_metrics.bytes_received += shard_length;

                self.video_stream_stats.last_packet_metrics.shards_received += 1;

                if self.video_stream_stats.highest_packet_index == packet_index {
                    if self.video_stream_stats.highest_shard_index < shard_index {
                        self.video_stream_stats.highest_shard_index = shard_index;
                    }
                } else if self.video_stream_stats.highest_packet_index < packet_index {
                    self.video_stream_stats.highest_packet_index = packet_index;
                    self.video_stream_stats.highest_shard_index = shard_index;
                }
            }

            self.shard_recv_state.insert(RecvState {
                shard_length,
                stream_id,
                packet_index,
                shards_count,
                shard_index,
                bytes_cursor: 0,
                overwritten_data_backup: None,
                should_discard: false,
            })
        };

        let Some(components) = self
            .stream_recv_components
            .get_mut(&shard_recv_state_mut.stream_id)
        else {
            if shard_recv_state_mut.stream_id == VIDEO {
                self.video_stream_stats.last_packet_metrics.shards_dropped += 1;
                warn!(
                    "Dropped shard {} from video packet {} because received before subscribing!",
                    shard_recv_state_mut.shard_index, shard_recv_state_mut.packet_index
                );
            }
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
            if shard_recv_state_mut.stream_id == VIDEO {
                self.video_stream_stats.last_packet_metrics.shards_dropped += 1;
                warn!(
                    "Dropped shard {} from video packet {} because the stream thread has hung!",
                    shard_recv_state_mut.shard_index, shard_recv_state_mut.packet_index
                );
            }
            &mut components.discarded_shards_sink
        };

        let max_shard_data_size = self.max_shard_size - SHARD_PREFIX_SIZE;
        // Note: there is no prefix offset, since we want to write the prefix too.
        let shard_start_index = shard_recv_state_mut.shard_index * max_shard_data_size;

        // Prepare buffer to accomodate receiving shard
        {
            // Note: this contains the prefix offset
            in_progress_packet.buffer_length = usize::max(
                in_progress_packet.buffer_length,
                shard_start_index + shard_recv_state_mut.shard_length,
            );

            if in_progress_packet.buffer.len() < in_progress_packet.buffer_length {
                in_progress_packet
                    .buffer
                    .resize(in_progress_packet.buffer_length, 0);
            }
        }

        let sub_buffer = &mut in_progress_packet.buffer[shard_start_index..];

        // Read shard into the single contiguous buffer
        {
            // Backup the small section of bytes that will be overwritten by reading from socket.
            if shard_recv_state_mut.overwritten_data_backup.is_none() {
                shard_recv_state_mut.overwritten_data_backup =
                    Some(sub_buffer[..SHARD_PREFIX_SIZE].try_into().unwrap())
            }

            // This loop may bail out at any time if a timeout is reached. This is correctly handled by
            // the previous code.
            while shard_recv_state_mut.bytes_cursor < shard_recv_state_mut.shard_length {
                let size = self.receive_socket.recv(
                    &mut sub_buffer
                        [shard_recv_state_mut.bytes_cursor..shard_recv_state_mut.shard_length],
                )?;
                shard_recv_state_mut.bytes_cursor += size;
            }

            // Restore backed up bytes
            // Safety: overwritten_data_backup is always set just before receiving the packet
            sub_buffer[..SHARD_PREFIX_SIZE]
                .copy_from_slice(&shard_recv_state_mut.overwritten_data_backup.take().unwrap());
        }

        if !shard_recv_state_mut.should_discard {
            if !in_progress_packet
                .received_shard_indices
                .contains(&shard_recv_state_mut.shard_index)
            {
                in_progress_packet
                    .received_shard_indices
                    .insert(shard_recv_state_mut.shard_index);
                if shard_recv_state_mut.stream_id == VIDEO {
                    self.video_stream_stats
                        .packet_shards_instant
                        .entry(shard_recv_state_mut.packet_index)
                        .or_insert_with(VecDeque::new)
                        .push_back(now);
                }
            } else {
                if shard_recv_state_mut.stream_id == VIDEO {
                    self.video_stream_stats
                        .last_packet_metrics
                        .shards_duplicated += 1;
                    warn!(
                        "Duplicated shard {} from video packet {}!",
                        shard_recv_state_mut.shard_index, shard_recv_state_mut.packet_index
                    );
                }
            }
        }

        // Check if packet is complete and send
        if in_progress_packet.received_shard_indices.len() == shard_recv_state_mut.shards_count {
            if shard_recv_state_mut.stream_id == VIDEO {
                let mut reception_interval = Duration::ZERO;

                let mut packet_span = Duration::ZERO;
                let mut shards_interval_average = 0.0;
                let mut shards_jitter = 0.0;

                let mut shards_intervals = Vec::new();
                let mut shards_intervals_sum = 0.0;
                let mut squared_deviation_sum = 0.0;

                if let Some(shards_instant) = self
                    .video_stream_stats
                    .packet_shards_instant
                    .remove(&shard_recv_state_mut.packet_index)
                {
                    if let (Some(first_instant), Some(last_instant)) =
                        (shards_instant.front(), shards_instant.back())
                    {
                        packet_span = (*last_instant).duration_since(*first_instant);

                        let mut prev_instant = *first_instant;
                        for &instant in shards_instant.iter().skip(1) {
                            if instant < prev_instant {
                                warn!(
                                    "Shard instants for video packet {} are not sorted increasingly!",
                                    shard_recv_state_mut.packet_index
                                );
                            }
                            let interval = instant.duration_since(prev_instant).as_secs_f32();
                            shards_intervals.push(interval);
                            shards_intervals_sum += interval;
                            prev_instant = instant;
                        }

                        let shards_intervals_count = shards_intervals.len() as f32;
                        shards_interval_average = if !shards_intervals.is_empty() {
                            shards_intervals_sum / shards_intervals_count
                        } else {
                            0.0
                        };

                        for interval in shards_intervals {
                            let deviation = (interval - shards_interval_average).abs();
                            squared_deviation_sum += deviation * deviation;
                        }
                        let mean_square_deviation = squared_deviation_sum / shards_intervals_count;
                        shards_jitter = mean_square_deviation.sqrt();

                        reception_interval = (*last_instant)
                            .saturating_duration_since(self.video_stream_stats.prev_packet_instant);
                        self.video_stream_stats.prev_packet_instant = *last_instant;
                    }
                }
                self.video_stream_stats
                    .last_packet_metrics
                    .reception_interval += reception_interval;

                self.video_stream_stats.last_packet_metrics.packet_span = packet_span;
                self.video_stream_stats
                    .last_packet_metrics
                    .shards_interval_average = shards_interval_average;
                self.video_stream_stats.last_packet_metrics.shards_jitter = shards_jitter;
            }
            let size = in_progress_packet.buffer_length;
            let (highest_packet_index, highest_shard_index, video_packet_metrics) =
                if shard_recv_state_mut.stream_id == VIDEO {
                    (
                        Some(self.video_stream_stats.highest_packet_index),
                        Some(self.video_stream_stats.highest_shard_index),
                        Some(self.video_stream_stats.last_packet_metrics),
                    )
                } else {
                    (None, None, None)
                };
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
                    highest_packet_index,
                    highest_shard_index,
                    video_packet_metrics,
                })
                .ok();
            if shard_recv_state_mut.stream_id == VIDEO {
                self.video_stream_stats.last_packet_metrics = VideoPacketMetrics::default();
            }

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
