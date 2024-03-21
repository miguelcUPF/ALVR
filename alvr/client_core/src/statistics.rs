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
    stats_hist_buffer: VecDeque<HistoryFrame>, // since there are several frames with same timestamp

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
            stats_hist_buffer: VecDeque::new(),
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
        if self.stats_hist_buffer.len() > self.max_history_size {
            self.stats_hist_buffer.pop_front();
        }
    }

    pub fn report_frame_received(&mut self, target_timestamp: Duration) {
        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            let now = Instant::now();

            if !frame.is_received {
                frame.is_received = true;
                frame.frame_received = now;

                frame.client_stats.frame_interval =
                    now.saturating_duration_since(self.prev_reception);
            }
            self.prev_reception = now;
        }
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

            self.stats_hist_buffer.push_back(frame.clone()) // to keep the statistics for frames with same target_timestamp
        }
    }

    pub fn report_frame_decoded(&mut self, target_timestamp: Duration) {
        let now = Instant::now();
        let prev_decode = self.prev_decoding;

        let mut frame_decoded = Instant::now();
        let mut video_decode = Duration::ZERO;
        let mut frame_interval_decode = Duration::ZERO;

        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            // this is to keep the same latency statistics for frames with same timestamp
            if !frame.is_decoded {
                frame.is_decoded = true;

                frame_decoded = now;
                frame.frame_decoded = frame_decoded;

                video_decode = now.saturating_duration_since(frame.frame_received);
                frame.client_stats.video_decode = video_decode;

                frame_interval_decode = now.saturating_duration_since(prev_decode);
                frame.client_stats.frame_interval_decode = frame_interval_decode;
                warn!(
                    "1_frame decoded statistics updated for frame {} with prev_decode {:?}",
                    target_timestamp.as_secs_f64(),
                    prev_decode
                ); // remove
            } else {
                frame_decoded = frame.frame_decoded;
                video_decode = frame.client_stats.video_decode;
                frame_interval_decode = frame.client_stats.frame_interval_decode;
            }
        } else {
            warn!(
                "1_frame_not found timestamp {}",
                target_timestamp.as_secs_f64()
            ); // remove
        }

        // stats buffer instead of history buffer since this happens after report_video_statisticcs()
        let mut count = 0; // remove
        for frame in self
            .stats_hist_buffer
            .iter_mut()
            .filter(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            count += 1;
            if !frame.is_decoded {
                frame.is_decoded = true;

                frame.frame_decoded = frame_decoded;
                frame.client_stats.video_decode = video_decode;
                frame.client_stats.frame_interval_decode = frame_interval_decode;

                warn!(
                    "2_frame {} timestamp {} interval decode is {}",
                    frame.client_stats.packet_index,
                    target_timestamp.as_secs_f64(),
                    frame_interval_decode.as_secs_f64()
                ); // remove
            }
            self.prev_decoding = now;
            warn!(
                "2_prev decoding updated now {:?} cosdiering frame {}",
                now, frame.client_stats.packet_index,
            ); // remove
        }
        if count > 1 {
            warn!(
                "2_more than one frame with timestamp {}",
                target_timestamp.as_secs_f64()
            ); // remove
        }
        if count == 0 {
            warn!("2_not found timestamp {}", target_timestamp.as_secs_f64()); // remove
        }
    }

    pub fn report_compositor_start(&mut self, target_timestamp: Duration) {
        let now = Instant::now();

        let mut frame_composed = Instant::now();
        let mut video_decoder_queue = Duration::ZERO;

        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            // this is to keep the same latency statistics for frames with same timestamp
            if !frame.is_composed {
                frame.is_composed = true;

                frame_composed = now;
                frame.frame_composed = now;

                video_decoder_queue = now.saturating_duration_since(
                    frame.frame_received + frame.client_stats.video_decode,
                );
                frame.client_stats.video_decoder_queue = video_decoder_queue;
            } else {
                frame_composed = frame.frame_composed;
                video_decoder_queue = frame.client_stats.video_decoder_queue;
            }
        }

        // stats buffer instead of history buffer since this happens after report_video_statisticcs()
        for frame in self.stats_hist_buffer.iter_mut().filter(|frame| {
            frame.client_stats.target_timestamp == target_timestamp && !frame.is_composed
        }) {
            frame.is_composed = true;

            frame.frame_composed = frame_composed;
            frame.client_stats.video_decoder_queue = video_decoder_queue;
        }
    }

    // vsync_queue is the latency between this call and the vsync. it cannot be measured by ALVR and
    // should be reported by the VR runtime
    pub fn report_submit(&mut self, target_timestamp: Duration, vsync_queue: Duration) {
        let now = Instant::now();
        let vsync = now + vsync_queue;
        let prev_vsync = self.prev_vsync;

        let mut frame_vsync_queue = Duration::ZERO;
        let mut rendering = Duration::ZERO;
        let mut total_pipeline_latency = Duration::ZERO;
        let mut frame_displayed = Instant::now();
        let mut frame_interval_vsync = Duration::ZERO;

        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            // this is to keep the same latency statistics for frames with same timestamp
            if !frame.is_displayed {
                frame.is_displayed = true;

                rendering = now.saturating_duration_since(
                    frame.frame_received
                        + frame.client_stats.video_decode
                        + frame.client_stats.video_decoder_queue,
                );
                frame.client_stats.rendering = rendering;

                frame_vsync_queue = vsync_queue;
                frame.client_stats.vsync_queue = frame_vsync_queue;

                total_pipeline_latency =
                    now.saturating_duration_since(frame.input_acquired) + vsync_queue;
                frame.client_stats.total_pipeline_latency = total_pipeline_latency;

                self.total_pipeline_latency_average
                    .submit_sample(frame.client_stats.total_pipeline_latency);

                frame_displayed = vsync;
                frame.frame_displayed = frame_displayed;

                frame_interval_vsync = vsync.saturating_duration_since(prev_vsync);
                frame.client_stats.frame_interval_vsync = frame_interval_vsync;

                warn!(
                    "3_frame submit statistics updated for frame {} with prev_vsync {:?}",
                    target_timestamp.as_secs_f64(),
                    prev_vsync
                ); // remove
            } else {
                frame_vsync_queue = frame.client_stats.vsync_queue;
                rendering = frame.client_stats.rendering;
                total_pipeline_latency = frame.client_stats.total_pipeline_latency;
                frame_displayed = frame.frame_displayed;
                frame_interval_vsync = frame.client_stats.frame_interval_vsync;
            }
        }

        // stats buffer instead of history buffer since this happens after report_video_statisticcs()
        for frame in self
            .stats_hist_buffer
            .iter_mut()
            .filter(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            if !frame.is_displayed {
                frame.is_displayed = true;

                frame.client_stats.vsync_queue = frame_vsync_queue;
                frame.client_stats.rendering = rendering;
                frame.client_stats.total_pipeline_latency = total_pipeline_latency;
                frame.frame_displayed = frame_displayed;
                frame.client_stats.frame_interval_vsync = frame_interval_vsync;
                warn!(
                    "3_frame {} timestamp {} interval vsync is {}",
                    frame.client_stats.packet_index,
                    target_timestamp.as_secs_f64(),
                    frame_interval_vsync.as_secs_f64()
                ); // remove
            }
            self.prev_vsync = vsync;
            warn!(
                "4_prev vsync updated now {:?} cosdiering frame {}",
                now, frame.client_stats.packet_index,
            ); // remove
        }
    }

    pub fn summary(&mut self, target_timestamp: Duration) -> Option<ClientStatistics> {
        if let Some(index) = self
            .stats_hist_buffer
            .iter()
            .position(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            if let Some(frame) = self.stats_hist_buffer.remove(index) {
                let client_stats = frame.client_stats;
                Some(client_stats)
            } else {
                None
            }
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
