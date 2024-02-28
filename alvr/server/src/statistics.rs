use alvr_common::{debug, SlidingWindowAverage, HEAD_ID};
use alvr_events::{EventType, GraphStatistics, NominalBitrateStats, Statistics, StatisticsSummary};
use alvr_packets::ClientStatistics;
use indexmap::IndexMap;
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

const STATS_SUMMARY_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct ServerStatistics {
    // Frame data
    pub target_timestamp: Duration,

    pub is_idr: bool,
    pub frame_index: u32,
    pub bytes: usize,
    pub shards_bytes: IndexMap<u16, usize>, // including prefix
    pub shards_instant: IndexMap<u16, Instant>,

    // Frame timing metrics
    pub frame_interval_present: Duration,
    pub frame_interval_encode: Duration,
    pub frame_interval: Duration,
}

impl Default for ServerStatistics {
    fn default() -> Self {
        Self {
            target_timestamp: Duration::ZERO,
            is_idr: false,
            frame_index: 0,
            bytes: 0,
            shards_bytes: IndexMap::new(),
            shards_instant: IndexMap::new(),
            frame_interval_present: Duration::ZERO,
            frame_interval_encode: Duration::ZERO,
            frame_interval: Duration::ZERO,
        }
    }
}

#[derive(Clone)]
pub struct HistoryFrame {
    tracking_received: Instant,
    frame_presented: Instant,
    frame_composed: Instant,
    frame_encoded: Instant,
    frame_transmitted: Instant,

    pub server_stats: ServerStatistics,
}

impl Default for HistoryFrame {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            tracking_received: now,
            frame_presented: now,
            frame_composed: now,
            frame_encoded: now,
            frame_transmitted: now,

            server_stats: Default::default(),
        }
    }
}

#[derive(Default, Clone)]
struct BatteryData {
    gauge_value: f32,
    is_plugged: bool,
}

pub struct StatisticsManager {
    history_buffer: VecDeque<HistoryFrame>,
    max_history_size: usize,

    steamvr_pipeline_latency: Duration,

    prev_presentation: Instant,
    prev_encoding: Instant,
    prev_transmission: Instant,

    prev_stats_summary_instant: Instant,
    prev_nominal_bitrate_stats: NominalBitrateStats,

    prev_vsync_instant: Instant,
    nominal_server_frame_interval: Duration,

    // Interval video statistics
    //// Support
    prev_last_shard_received_instant: Instant,
    prev_highest_frame: HistoryFrame,
    prev_highest_shard_index: usize,
    is_first_stats: bool,

    // Summary statistics
    //// Cumulative metrics
    total_bytes_sent: usize,
    total_frames_sent: usize,
    total_shards_sent: usize,
    total_bytes_received: usize,
    total_frames_received: usize,
    total_shards_received: usize,
    //// Non-cumulative metrics
    partial_sum_bytes_sent: usize,
    partial_sum_frames_sent: usize,
    partial_sum_shards_sent: usize,
    partial_sum_bytes_received: usize,
    partial_sum_frames_received: usize,
    partial_sum_shards_received: usize,
    //// Latency metrics
    total_pipeline_latency_average: SlidingWindowAverage<Duration>,
    game_delay_average: SlidingWindowAverage<Duration>,
    server_compositor_average: SlidingWindowAverage<Duration>,
    encode_delay_average: SlidingWindowAverage<Duration>,
    network_delay_average: SlidingWindowAverage<Duration>,
    decode_delay_average: SlidingWindowAverage<Duration>,
    decoder_queue_delay_average: SlidingWindowAverage<Duration>,
    client_compositor_average: SlidingWindowAverage<Duration>,
    vsync_queue_delay_average: SlidingWindowAverage<Duration>,
    //// Others
    battery_gauges: HashMap<u64, BatteryData>,
}

impl StatisticsManager {
    // history size used to calculate average total pipeline latency
    pub fn new(
        max_history_size: usize,
        nominal_server_frame_interval: Duration,
        steamvr_pipeline_frames: f32,
    ) -> Self {
        Self {
            history_buffer: VecDeque::new(),
            max_history_size,
            steamvr_pipeline_latency: Duration::from_secs_f32(
                steamvr_pipeline_frames * nominal_server_frame_interval.as_secs_f32(),
            ),
            prev_presentation: Instant::now(),
            prev_encoding: Instant::now(),
            prev_transmission: Instant::now(),
            prev_stats_summary_instant: Instant::now(),
            prev_vsync_instant: Instant::now(),
            nominal_server_frame_interval,
            prev_nominal_bitrate_stats: NominalBitrateStats::default(),
            prev_last_shard_received_instant: Instant::now(),
            prev_highest_frame: HistoryFrame::default(),
            prev_highest_shard_index: 0,
            is_first_stats: true,
            total_bytes_sent: 0,
            total_frames_sent: 0,
            total_shards_sent: 0,
            total_bytes_received: 0,
            total_frames_received: 0,
            total_shards_received: 0,
            partial_sum_bytes_sent: 0,
            partial_sum_frames_sent: 0,
            partial_sum_shards_sent: 0,
            partial_sum_bytes_received: 0,
            partial_sum_frames_received: 0,
            partial_sum_shards_received: 0,
            total_pipeline_latency_average: SlidingWindowAverage::new(
                Duration::ZERO,
                max_history_size,
            ),
            game_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            server_compositor_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            encode_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            network_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            decode_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            decoder_queue_delay_average: SlidingWindowAverage::new(
                Duration::ZERO,
                max_history_size,
            ),
            client_compositor_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            vsync_queue_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            battery_gauges: HashMap::new(),
        }
    }

    pub fn report_tracking_received(&mut self, target_timestamp: Duration) {
        if !self
            .history_buffer
            .iter()
            .any(|frame| frame.server_stats.target_timestamp == target_timestamp)
        {
            self.history_buffer.push_front(HistoryFrame {
                tracking_received: Instant::now(),
                server_stats: ServerStatistics {
                    target_timestamp,
                    ..Default::default() // Initialize other fields with default values
                },
                ..Default::default()
            });
        }

        if self.history_buffer.len() > self.max_history_size {
            self.history_buffer.pop_back();
        }
    }

    pub fn report_frame_presented(&mut self, target_timestamp: Duration, offset: Duration) {
        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.server_stats.target_timestamp == target_timestamp)
        {
            let now = Instant::now() - offset;

            frame.frame_presented = now;

            frame.server_stats.frame_interval_present =
                now.saturating_duration_since(self.prev_presentation);

            self.prev_presentation = now;
        }
    }

    pub fn report_frame_composed(&mut self, target_timestamp: Duration, offset: Duration) {
        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.server_stats.target_timestamp == target_timestamp)
        {
            let now = Instant::now() - offset;

            frame.frame_composed = now;
        }
    }
    // returns encoding delay
    pub fn report_frame_encoded(
        &mut self,
        target_timestamp: Duration,
        bytes: usize,
        is_idr: bool,
    ) -> Duration {
        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.server_stats.target_timestamp == target_timestamp)
        {
            let now = Instant::now();

            frame.frame_encoded = now;

            frame.server_stats.is_idr = is_idr;

            frame.server_stats.bytes = bytes;

            let frame_interval_encode = now.saturating_duration_since(self.prev_encoding);
            frame.server_stats.frame_interval_encode = frame_interval_encode;

            self.prev_encoding = now;

            frame_interval_encode
        } else {
            Duration::ZERO
        }
    }

    pub fn report_frame_transmitted(
        &mut self,
        target_timestamp: Duration,
        packet_index: u32,
        shards_bytes: IndexMap<u16, usize>,
        shards_instant: IndexMap<u16, Instant>,
    ) {
        self.total_frames_sent += 1;
        self.total_bytes_sent += shards_bytes.values().sum::<usize>();
        self.total_shards_sent += shards_bytes.values().count();

        self.partial_sum_frames_sent += 1;
        self.partial_sum_bytes_sent += shards_bytes.values().sum::<usize>();
        self.partial_sum_shards_sent += shards_bytes.values().count();

        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.server_stats.target_timestamp == target_timestamp)
        {
            let last_instant = *shards_instant.values().last().unwrap();

            frame.frame_transmitted = last_instant;

            frame.server_stats.frame_interval =
                last_instant.saturating_duration_since(self.prev_transmission);

            self.prev_transmission = last_instant;

            debug!(
                "Frame {} encoded instant {}",
                packet_index,
                format!("{:?}", frame.frame_encoded)
            ); // remove
            debug!(
                "Frame {} sent instant {}",
                packet_index,
                format!("{:?}", last_instant)
            ); // remove

            frame.server_stats.frame_index = packet_index;
            frame.server_stats.shards_bytes = shards_bytes;
            frame.server_stats.shards_instant = shards_instant;
        }
    }

    pub fn report_battery(&mut self, device_id: u64, gauge_value: f32, is_plugged: bool) {
        *self.battery_gauges.entry(device_id).or_default() = BatteryData {
            gauge_value,
            is_plugged,
        };
    }

    pub fn report_nominal_bitrate_stats(&mut self, stats: NominalBitrateStats) {
        self.prev_nominal_bitrate_stats = stats;
    }

    pub fn report_statistics_summary(&mut self) {
        let interval_secs = STATS_SUMMARY_INTERVAL.as_secs_f32();

        alvr_events::send_event(EventType::StatisticsSummary(StatisticsSummary {
            total_frames_sent: self.total_frames_sent,
            frames_sent_per_sec: (self.partial_sum_frames_sent as f32 / interval_secs) as _,

            total_frames_received: self.total_frames_received,
            frames_received_per_sec: (self.partial_sum_frames_received as f32 / interval_secs) as _,

            total_mbits_sent: (self.total_bytes_sent as f32 / (1e6)) as usize,
            mbits_sent_per_sec: self.partial_sum_bytes_sent as f32 * 8. / (1e6) / interval_secs,

            total_mbits_received: (self.total_bytes_received as f32 / (1e6)) as usize,
            mbits_received_per_sec: self.partial_sum_bytes_received as f32 * 8.
                / (1e6)
                / interval_secs,

            total_shards_sent: self.total_shards_sent,
            shards_sent_per_sec: (self.partial_sum_shards_sent as f32 / interval_secs) as _,

            total_shards_received: self.total_shards_received,
            shards_received_per_sec: (self.partial_sum_shards_received as f32 / interval_secs) as _,

            total_pipeline_latency_average_ms: self
                .total_pipeline_latency_average
                .get_average()
                .as_secs_f32()
                * 1000.,
            game_delay_average_ms: self.game_delay_average.get_average().as_secs_f32() * 1000.,
            server_compositor_delay_average_ms: self
                .server_compositor_average
                .get_average()
                .as_secs_f32()
                * 1000.,
            encode_delay_average_ms: self.encode_delay_average.get_average().as_secs_f32() * 1000.,
            network_delay_average_ms: self.network_delay_average.get_average().as_secs_f32()
                * 1000.,
            decode_delay_average_ms: self.decode_delay_average.get_average().as_secs_f32() * 1000.,
            decoder_queue_delay_average_ms: self
                .decoder_queue_delay_average
                .get_average()
                .as_secs_f32()
                * 1000.,
            client_compositor_average_ms: self
                .client_compositor_average
                .get_average()
                .as_secs_f32()
                * 1000.,
            vsync_queue_delay_average_ms: self
                .vsync_queue_delay_average
                .get_average()
                .as_secs_f32()
                * 1000.,

            battery_hmd: (self
                .battery_gauges
                .get(&HEAD_ID)
                .cloned()
                .unwrap_or_default()
                .gauge_value
                * 100.) as u32,
            hmd_plugged: self
                .battery_gauges
                .get(&HEAD_ID)
                .cloned()
                .unwrap_or_default()
                .is_plugged,
        }));

        self.partial_sum_frames_sent = 0;
        self.partial_sum_bytes_sent = 0;
        self.partial_sum_shards_sent = 0;
        self.partial_sum_frames_received = 0;
        self.partial_sum_bytes_received = 0;
        self.partial_sum_shards_received = 0;
    }

    // Called every frame, returns network delay
    pub fn report_statistics(&mut self, client_stats: ClientStatistics) -> Duration {
        let next_summary_time = self.prev_stats_summary_instant + STATS_SUMMARY_INTERVAL;
        if next_summary_time <= Instant::now() {
            self.prev_stats_summary_instant = Instant::now();
            self.report_statistics_summary();
        }

        let frame = match self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.server_stats.target_timestamp == client_stats.target_timestamp)
        {
            Some(frame_searched) => frame_searched.clone(),
            None => {
                debug!("Frame not found in history buffer!");
                return Duration::ZERO;
            }
        };

        let highest_frame_received = if frame.server_stats.frame_index
            == client_stats.highest_frame_index
        {
            frame.clone()
        } else {
            self.history_buffer
                .iter_mut()
                .find(|frame_| frame_.server_stats.frame_index == client_stats.highest_frame_index)
                .map_or_else(|| frame.clone(), |frame_searched| frame_searched.clone())
        };

        // Frame statistics
        //// Latency metrics
        let game_delay = frame
            .frame_presented
            .saturating_duration_since(frame.tracking_received);
        let rendering = frame
            .frame_composed
            .saturating_duration_since(frame.frame_presented);
        let video_encode = frame
            .frame_encoded
            .saturating_duration_since(frame.frame_composed);

        ////// The network latency cannot be estiamed directly. It is what's left of the total pipeline
        ////// latency after subtracting all other latency intervals. In particular it contains the
        ////// transport latency of the tracking packet and the interval between the first video
        ////// shard is sent and the last video shard is received for a specific frame.
        let network = client_stats.total_pipeline_latency.saturating_sub(
            game_delay
                + rendering
                + video_encode
                + client_stats.video_decode
                + client_stats.video_decoder_queue
                + client_stats.rendering
                + client_stats.vsync_queue,
        );

        //// Frame data
        let is_idr = frame.server_stats.is_idr;
        let frame_index = frame.server_stats.frame_index;
        let frame_bytes = frame.server_stats.bytes;
        let shards_count = frame.server_stats.shards_instant.len();
        let frame_bytes_sent = frame.server_stats.shards_bytes.values().sum();

        //// Frame rate metrics
        let server_fps_present = 1.0
            / frame
                .server_stats
                .frame_interval_present
                .max(Duration::from_millis(1))
                .as_secs_f32();

        let server_fps_encode = 1.0
            / frame
                .server_stats
                .frame_interval_encode
                .max(Duration::from_millis(1))
                .as_secs_f32();

        let server_fps = 1.0
            / frame
                .server_stats
                .frame_interval
                .max(Duration::from_millis(1))
                .as_secs_f32();

        let client_fps = 1.0
            / client_stats
                .frame_interval
                .max(Duration::from_millis(1))
                .as_secs_f32();

        let client_fps_decode = 1.0
            / client_stats
                .frame_interval_decode
                .max(Duration::from_millis(1))
                .as_secs_f32();
        let client_fps_vsync = 1.0
            / client_stats
                .frame_interval_vsync
                .max(Duration::from_millis(1))
                .as_secs_f32();

        //// Frame timing metrics
        let shards_instant = frame.server_stats.shards_instant;
        let first_instant = shards_instant.values().min().unwrap();
        let last_instant = shards_instant.values().max().unwrap();

        let frame_span = (*last_instant).saturating_duration_since(*first_instant);
        let client_frame_span = client_stats.frame_span;

        let mut frame_shard_interval_sum = Duration::ZERO;
        let mut prev_instant = *first_instant;
        for &instant in shards_instant.values().skip(1) {
            let interval = instant.saturating_duration_since(prev_instant);
            frame_shard_interval_sum += interval;
            prev_instant = instant;
        }

        let frame_shard_interval_average = if shards_count > 1 {
            frame_shard_interval_sum / (shards_count - 1) as u32
        } else {
            Duration::ZERO
        };
        let client_frame_shard_interval_average = client_stats.frame_shard_interval_average;

        let frame_shard_jitter = client_stats.frame_shard_jitter;

        // Interval video statistics
        //// Given that the server and client are not synchronized, we use the last frame
        //// received (highest_frame_index) and its last shard recieved (highest_shard_index)
        //// and the client as an indicator of the amount of frames and shards sent
        let prev_idx = self.prev_highest_frame.server_stats.frame_index;

        let highest_idx = highest_frame_received.server_stats.frame_index;

        let frames_sent;

        if self.is_first_stats {
            let lowest_frame_temp = self
                .history_buffer
                .iter_mut()
                .min_by_key(|frame_| frame_.server_stats.frame_index);
            self.prev_last_shard_received_instant = *lowest_frame_temp
                .unwrap()
                .server_stats
                .shards_instant
                .values()
                .min()
                .unwrap();

            frames_sent = highest_idx.wrapping_sub(prev_idx) + 1;

            self.is_first_stats = false;
        } else {
            frames_sent = highest_idx.wrapping_sub(prev_idx);
        }

        let frames_discarded = client_stats.frames_discarded;

        let frames_lost = client_stats.frames_lost_discarded - frames_discarded;

        let frames_dropped = client_stats.frames_dropped;

        let mut frames_received = 0;

        if frames_sent >= frames_lost as u32 {
            frames_received = frames_sent - frames_lost as u32;
        }

        let mut last_shard_received_instant = Instant::now();

        if let Some(instant) = highest_frame_received
            .server_stats
            .shards_instant
            .get(&(client_stats.highest_shard_index as u16))
        {
            last_shard_received_instant = *instant;
        }

        let transmission_interval = last_shard_received_instant
            .saturating_duration_since(self.prev_last_shard_received_instant);

        let shards_sent;
        let bytes_sent;

        if highest_idx == prev_idx {
            shards_sent = client_stats
                .highest_shard_index
                .saturating_sub(self.prev_highest_shard_index);

            bytes_sent = highest_frame_received
                .server_stats
                .shards_bytes
                .iter()
                .filter(|&(shard_index, _)| {
                    *shard_index > self.prev_highest_shard_index as u16
                        && *shard_index <= client_stats.highest_shard_index as u16
                })
                .map(|(_, &bytes)| bytes)
                .sum::<usize>();
        } else {
            // Previous highest frame received
            let shards_from_prev = self
                .prev_highest_frame
                .server_stats
                .shards_bytes
                .iter()
                .filter(|&(shard_index, _)| *shard_index > self.prev_highest_shard_index as u16)
                .count();
            let bytes_from_prev = self
                .prev_highest_frame
                .server_stats
                .shards_bytes
                .iter()
                .filter(|&(shard_index, _)| *shard_index > self.prev_highest_shard_index as u16)
                .map(|(_, &bytes)| bytes)
                .sum::<usize>();

            //From frames in between
            let shards_in_between = self
                .history_buffer
                .iter_mut()
                .filter(|frame| {
                    let frame_idx = frame.server_stats.frame_index;

                    if highest_idx < 257 && prev_idx > std::u32::MAX - 257 {
                        (frame_idx > prev_idx && frame_idx <= std::u32::MAX)
                            || (frame_idx <= highest_idx)
                    } else {
                        frame_idx > prev_idx && frame_idx <= highest_idx
                    }
                })
                .flat_map(|frame| frame.server_stats.shards_bytes.values())
                .count();
            let bytes_in_between = self
                .history_buffer
                .iter_mut()
                .filter(|frame| {
                    let frame_idx = frame.server_stats.frame_index;

                    if highest_idx < 257 && prev_idx > std::u32::MAX - 257 {
                        (frame_idx > prev_idx && frame_idx <= std::u32::MAX)
                            || (frame_idx <= highest_idx)
                    } else {
                        frame_idx > prev_idx && frame_idx <= highest_idx
                    }
                })
                .flat_map(|frame| frame.server_stats.shards_bytes.values())
                .sum::<usize>();

            //From highest frame received
            let shards_from_last = client_stats.highest_shard_index + 1;
            let bytes_from_last = highest_frame_received
                .server_stats
                .shards_bytes
                .iter()
                .filter(|&(shard_index, _)| *shard_index <= client_stats.highest_shard_index as u16)
                .map(|(_, &bytes)| bytes)
                .sum::<usize>();

            shards_sent = shards_from_prev + shards_in_between + shards_from_last;

            bytes_sent = bytes_from_prev + bytes_in_between + bytes_from_last;
        }

        let shards_lost = shards_sent - client_stats.shards_received;

        let shards_duplicated = client_stats.shards_duplicated;

        let shards_received = client_stats.shards_received;

        let bytes_received = client_stats.bytes_received;

        let mbits_sent_per_sec = (bytes_sent as f32 * 8.)
            / (1e6)
            / transmission_interval
                .max(Duration::from_millis(1))
                .as_secs_f32();
        let mbits_received_per_sec = (bytes_received as f32 * 8.)
            / (1e6)
            / client_stats
                .frame_interval
                .max(Duration::from_millis(1))
                .as_secs_f32();

        self.prev_last_shard_received_instant = last_shard_received_instant;
        self.prev_highest_frame = highest_frame_received.clone();
        self.prev_highest_shard_index = client_stats.highest_shard_index;

        self.total_frames_received += frames_received as usize;
        self.total_bytes_received += bytes_received as usize;
        self.total_shards_received += shards_received;

        self.partial_sum_frames_received += frames_received as usize;
        self.partial_sum_bytes_received += bytes_received;
        self.partial_sum_shards_received += shards_received;

        // todo: use target timestamp in nanoseconds. the dashboard needs to use the first
        // timestamp as the graph time origin.
        alvr_events::send_event(EventType::Statistics(Statistics {
            // Frame data
            is_idr,
            frame_index,
            frame_bytes,
            shards_count,
            frame_bytes_sent, // including prefix
            // Frame timing metrics
            frame_span_s: frame_span.as_secs_f32(),
            client_frame_span_s: client_frame_span.as_secs_f32(),
            frame_shard_interval_average_s: frame_shard_interval_average.as_secs_f32(),
            client_frame_shard_interval_average_s: client_frame_shard_interval_average
                .as_secs_f32(),
            frame_shard_jitter_s: frame_shard_jitter,
            // Interval video statistics
            bytes_sent,     // including prefix
            bytes_received, // including prefix

            shards_sent,
            shards_received,
            shards_lost,
            shards_duplicated,

            frames_sent: frames_sent as usize,
            frames_received: frames_received as usize,
            frames_lost,
            frames_discarded,
            frames_dropped,
        }));
        alvr_events::send_event(EventType::GraphStatistics(GraphStatistics {
            // Latency metrics
            total_pipeline_latency_s: client_stats.total_pipeline_latency.as_secs_f32(),
            game_time_s: game_delay.as_secs_f32(),
            server_compositor_s: rendering.as_secs_f32(),
            encoder_s: video_encode.as_secs_f32(),
            network_s: network.as_secs_f32(),
            decoder_s: client_stats.video_decode.as_secs_f32(),
            decoder_queue_s: client_stats.video_decoder_queue.as_secs_f32(),
            client_compositor_s: client_stats.rendering.as_secs_f32(),
            vsync_queue_s: client_stats.vsync_queue.as_secs_f32(),
            // Frame rate metrics
            server_fps_present,
            server_fps_encode,
            server_fps,
            client_fps,
            client_fps_decode,
            client_fps_vsync,
            // Interval video statistics
            mbits_sent_per_sec,     // including prefix
            mbits_received_per_sec, // including prefix
            nominal_bitrate: self.prev_nominal_bitrate_stats.clone(), // TODO: check
        }));
        network
    }

    pub fn video_pipeline_latency_average(&self) -> Duration {
        self.total_pipeline_latency_average.get_average()
    }

    pub fn tracker_pose_time_offset(&self) -> Duration {
        // This is the opposite of the client's StatisticsManager::tracker_prediction_offset().
        self.steamvr_pipeline_latency
            .saturating_sub(self.total_pipeline_latency_average.get_average())
    }

    // NB: this call is non-blocking, waiting should be done externally
    pub fn duration_until_next_vsync(&mut self) -> Duration {
        let now = Instant::now();

        // update the last vsync if it's too old
        while self.prev_vsync_instant + self.nominal_server_frame_interval < now {
            self.prev_vsync_instant += self.nominal_server_frame_interval;
        }

        (self.prev_vsync_instant + self.nominal_server_frame_interval)
            .saturating_duration_since(now)
    }
}
