use crate::connection::VideoStatistics;
use alvr_common::{warn, SlidingWindowAverage};
use alvr_packets::ClientStatistics;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

#[derive(Clone)]
struct HistoryFrame {
    input_acquired: Instant,
    frame_received: Instant,
    frame_decoded: Instant,
    frame_composed: Instant,
    frame_displayed: Instant,

    is_received: bool, // to avoid overwritting the latency metrics if several frames are received with the same target_timestamp
    is_decoded: bool,
    is_composed: bool,
    is_displayed: bool,

    client_stats: ClientStatistics,
}
pub struct StatisticsManager {
    history_buffer: VecDeque<HistoryFrame>,
    stats_buffer: VecDeque<ClientStatistics>, // client statistics for every frame

    max_history_size: usize,

    prev_reception: Instant,
    prev_decoding: Instant,
    prev_vsync: Instant,

    total_pipeline_latency_average: SlidingWindowAverage<Duration>,
    steamvr_pipeline_latency: Duration,
}

impl StatisticsManager {
    pub fn new(
        max_history_size: usize,
        nominal_server_frame_interval: Duration,
        steamvr_pipeline_frames: f32,
    ) -> Self {
        Self {
            max_history_size,
            history_buffer: VecDeque::new(),
            stats_buffer: VecDeque::new(),
            prev_reception: Instant::now(),
            prev_decoding: Instant::now(),
            prev_vsync: Instant::now(),
            total_pipeline_latency_average: SlidingWindowAverage::new(
                Duration::ZERO,
                max_history_size,
            ),
            steamvr_pipeline_latency: Duration::from_secs_f32(
                steamvr_pipeline_frames * nominal_server_frame_interval.as_secs_f32(),
            ),
        }
    }

    pub fn report_input_acquired(&mut self, target_timestamp: Duration) {
        if !self
            .history_buffer
            .iter()
            .any(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            self.history_buffer.push_front(HistoryFrame {
                input_acquired: Instant::now(),
                frame_received: Instant::now(),
                frame_decoded: Instant::now(),
                frame_composed: Instant::now(),
                frame_displayed: Instant::now(),
                is_received: false,
                is_decoded: false,
                is_composed: false,
                is_displayed: false,
                client_stats: ClientStatistics {
                    target_timestamp,
                    ..Default::default()
                },
            });
        }

        if self.history_buffer.len() > self.max_history_size {
            self.history_buffer.pop_back();
        }
        if self.stats_buffer.len() > self.max_history_size {
            self.stats_buffer.pop_front();
        }
    }

    pub fn report_frame_received(&mut self, target_timestamp: Duration) {
        let now = Instant::now();

        if let Some(frame) = self.history_buffer.iter_mut().find(|frame| {
            frame.client_stats.target_timestamp == target_timestamp && !frame.is_received
        }) {
            frame.frame_received = now;

            frame.client_stats.frame_interval = now.saturating_duration_since(self.prev_reception);
        }
        self.prev_reception = now;
    }

    pub fn report_video_statistics(&mut self, target_timestamp: Duration, stats: VideoStatistics) {
        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            frame.client_stats.packet_index = stats.packet_index as i32;

            frame.client_stats.frame_span = stats.frame_span;
            frame.client_stats.shards_interval_average = stats.shards_interval_average;
            frame.client_stats.shards_jitter = stats.shards_jitter;

            frame.client_stats.reception_interval = stats.reception_interval;

            frame.client_stats.bytes_received = stats.bytes_received;
            frame.client_stats.shards_received = stats.shards_received;

            frame.client_stats.frames_skipped = stats.frames_skipped;
            frame.client_stats.frames_discarded = stats.frames_discarded;
            frame.client_stats.frames_dropped = stats.frames_dropped;

            frame.client_stats.shards_dropped = stats.shards_dropped;
            frame.client_stats.shards_duplicated = stats.shards_duplicated;

            frame.client_stats.highest_frame_index = stats.highest_frame_index;
            frame.client_stats.highest_shard_index = stats.highest_shard_index;

            self.stats_buffer.push_back(frame.client_stats.clone()) // to keep the statistics for frames with same target_timestamp
        }
    }

    pub fn report_frame_decoded(&mut self, target_timestamp: Duration) {
        let now = Instant::now();

        if let Some(frame) = self.history_buffer.iter_mut().find(|frame| {
            frame.client_stats.target_timestamp == target_timestamp && !frame.is_decoded
        }) {
            frame.frame_decoded = now;

            frame.client_stats.video_decode = now.saturating_duration_since(frame.frame_received);

            frame.client_stats.frame_interval_decode =
                now.saturating_duration_since(self.prev_decoding);
        }
        self.prev_decoding = now;
    }

    pub fn report_compositor_start(&mut self, target_timestamp: Duration) {
        let now = Instant::now();

        if let Some(frame) = self.history_buffer.iter_mut().find(|frame| {
            frame.client_stats.target_timestamp == target_timestamp && !frame.is_composed
        }) {
            frame.frame_composed = now;

            frame.client_stats.video_decoder_queue = now
                .saturating_duration_since(frame.frame_received + frame.client_stats.video_decode);
        }
    }

    // vsync_queue is the latency between this call and the vsync. it cannot be measured by ALVR and
    // should be reported by the VR runtime
    pub fn report_submit(&mut self, target_timestamp: Duration, vsync_queue: Duration) {
        let now = Instant::now();
        let vsync = now + vsync_queue;

        if let Some(frame) = self.history_buffer.iter_mut().find(|frame| {
            frame.client_stats.target_timestamp == target_timestamp && !frame.is_displayed
        }) {
            frame.client_stats.rendering = now.saturating_duration_since(
                frame.frame_received
                    + frame.client_stats.video_decode
                    + frame.client_stats.video_decoder_queue,
            );
            frame.client_stats.vsync_queue = vsync_queue;
            frame.client_stats.total_pipeline_latency =
                now.saturating_duration_since(frame.input_acquired) + vsync_queue;
            self.total_pipeline_latency_average
                .submit_sample(frame.client_stats.total_pipeline_latency);

            frame.frame_displayed = vsync;

            frame.client_stats.frame_interval_vsync =
                vsync.saturating_duration_since(self.prev_vsync);
        }
        self.prev_vsync = vsync;
    }

    pub fn summary(&mut self, target_timestamp: Duration) -> Option<ClientStatistics> {
        if let Some(index) = self
            .stats_buffer
            .iter()
            .position(|client_stats| client_stats.target_timestamp == target_timestamp)
        {
            let client_stats = self.stats_buffer.remove(index);
            warn!("summary sent for frame {}", client_stats.clone().unwrap().packet_index ); // remove
            client_stats
        } else {
            None
        }
    }

    // latency used for head prediction
    pub fn average_total_pipeline_latency(&self) -> Duration {
        self.total_pipeline_latency_average.get_average()
    }

    // latency used for controllers/trackers prediction
    pub fn tracker_prediction_offset(&self) -> Duration {
        self.total_pipeline_latency_average
            .get_average()
            .saturating_sub(self.steamvr_pipeline_latency)
    }
}
