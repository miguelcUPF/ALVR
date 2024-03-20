use alvr_common::{info, DeviceMotion, LogEntry, Pose};
use alvr_packets::{AudioDevicesList, ButtonValue};
use alvr_session::SessionConfig;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Statistics {
    // Frame data
    pub is_idr: bool,
    pub frame_index: u32,
    pub frame_bytes: usize,
    pub shards_count: usize,
    pub frame_bytes_sent: usize,
    // Frame timing metrics
    pub frame_span_s: f32,
    pub client_frame_span_s: f32,
    pub server_shards_interval_average_s: f32,
    pub client_shards_interval_average_s: f32,
    pub shards_jitter_s: f32,
    // Interval video statistics
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub shards_sent: usize,
    pub shards_received: usize,
    pub shards_lost: f32,
    pub shards_dropped: usize,
    pub shards_duplicated: usize,
    pub frames_sent: usize,
    pub frames_received: usize,
    pub frames_skipped: usize,
    pub frames_discarded: usize,
    pub frames_dropped: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StatisticsSummary {
    pub total_frames_sent: usize,
    pub frames_sent_per_sec: f32,
    pub total_frames_received: usize,
    pub frames_received_per_sec: f32,
    pub total_mbits_sent: usize,
    pub mbits_sent_per_sec: f32,
    pub total_mbits_received: usize,
    pub mbits_received_per_sec: f32,
    pub total_shards_sent: usize,
    pub shards_sent_per_sec: f32,
    pub total_shards_received: usize,
    pub shards_received_per_sec: f32,
    pub total_pipeline_latency_average_ms: f32,
    pub game_delay_average_ms: f32,
    pub server_compositor_delay_average_ms: f32,
    pub encode_delay_average_ms: f32,
    pub network_delay_average_ms: f32,
    pub decode_delay_average_ms: f32,
    pub decoder_queue_delay_average_ms: f32,
    pub client_compositor_average_ms: f32,
    pub vsync_queue_delay_average_ms: f32,
    pub battery_hmd: u32,
    pub hmd_plugged: bool,
}

// Bitrate statistics minus the empirical output value
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct NominalBitrateStats {
    pub scaled_calculated_bps: Option<f32>,
    pub decoder_latency_limiter_bps: Option<f32>,
    pub network_latency_limiter_bps: Option<f32>,
    pub encoder_latency_limiter_bps: Option<f32>,
    pub manual_max_bps: Option<f32>,
    pub manual_min_bps: Option<f32>,
    pub requested_bps: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GraphStatistics {
    // Latency metrics
    pub total_pipeline_latency_s: f32,
    pub game_time_s: f32,
    pub server_compositor_s: f32,
    pub encoder_s: f32,
    pub network_s: f32,
    pub decoder_s: f32,
    pub decoder_queue_s: f32,
    pub client_compositor_s: f32,
    pub vsync_queue_s: f32,
    // Frame rate metrics
    pub server_fps_present: f32,
    pub server_fps_encode: f32,
    pub server_fps: f32,
    pub client_fps: f32,
    pub client_fps_decode: f32,
    pub client_fps_vsync: f32,
    // Interval video statistics
    pub mbits_sent_per_sec: f32,
    pub mbits_received_per_sec: f32,
    pub nominal_bitrate: NominalBitrateStats,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TrackingEvent {
    pub device_motions: Vec<(String, DeviceMotion)>,
    pub hand_skeletons: [Option<[Pose; 26]>; 2],
    pub eye_gazes: [Option<Pose>; 2],
    pub fb_face_expression: Option<Vec<f32>>,
    pub htc_eye_expression: Option<Vec<f32>>,
    pub htc_lip_expression: Option<Vec<f32>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ButtonEvent {
    pub path: String,
    pub value: ButtonValue,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HapticsEvent {
    pub path: String,
    pub duration: Duration,
    pub frequency: f32,
    pub amplitude: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "id", content = "data")]
pub enum EventType {
    Log(LogEntry),
    Session(Box<SessionConfig>),
    StatisticsSummary(StatisticsSummary),
    Statistics(Statistics),
    GraphStatistics(GraphStatistics),
    Tracking(Box<TrackingEvent>),
    Buttons(Vec<ButtonEvent>),
    Haptics(HapticsEvent),
    AudioDevices(AudioDevicesList),
    DriversList(Vec<PathBuf>),
    ServerRequestsSelfRestart,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Event {
    pub timestamp: String,
    pub event_type: EventType,
}

pub fn send_event(event_type: EventType) {
    info!("{}", serde_json::to_string(&event_type).unwrap());
}
