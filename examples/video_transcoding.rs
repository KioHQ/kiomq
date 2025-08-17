use deadpool_redis::{Config, Connection};
use kio_mq::{
    fetch_redis_pass, frame, framed, get_job_metrics, EventParameters, Job, KioError, KioResult,
    Queue, Worker, WorkerOpts,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{mpsc::Sender, Notify};
type BoxedError = Box<dyn std::error::Error + Send>;
use ffmpeg_sidecar::{
    command::FfmpegCommand,
    event::{FfmpegEvent, LogLevel},
    log_parser::parse_time_str,
};
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct ProcessData {
    path: PathBuf,
    size: Size,
}
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
struct Size {
    width: u32,
    height: u32,
}
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct ReturnData {
    output_path: PathBuf,
    processed_size: Size,
}
#[derive(Clone, Debug, Serialize, Deserialize, Copy, Default)]
struct Progress {
    percentage: f64,
    current_duration: Option<f64>,
    size_kb: u32,
    fps: f32,
    bitrate_kbps: f32,
}

#[tokio::main]
#[framed]
async fn main() -> KioResult<()> {
    let input_path = "sampleFHD.mp4";
    let mut config = Config::default();
    if let Some(cfg) = config.connection.as_mut() {
        cfg.redis.password = fetch_redis_pass();
    }
    let queue: Queue<ProcessData, ReturnData, Progress> =
        Queue::new(None, "video-processing", &config).await?;
    let processor = |con: _, job: _| process_callback(con, job);
    if !Path::new(input_path).exists() {
        tokio::task::spawn_blocking(|| create_h265_source(input_path)).await?;
    }

    // create the compressed folder if its doesn't exist too;
    if !Path::new("compressed").exists() {
        fs::create_dir("compressed").await?;
    }
    let sizes = [(1280, 720), (640, 480)];
    for (height, width) in sizes {
        let size = Size { height, width };
        let data = ProcessData {
            size,
            path: input_path.into(),
        };
        queue
            .add_job(height.to_string().as_str(), data, None)
            .await?;
    }
    let opts = WorkerOpts {
        concurrency: sizes.len(),
        ..Default::default()
    };
    let worker = Worker::new(&queue, processor, Some(opts));
    let cancel_worker = worker.cancellation_token.clone();
    let notifier = Arc::new(Notify::new());
    let last_job_id = queue.job_count.load(std::sync::atomic::Ordering::Relaxed);
    let notifier_clone = notifier.clone();
    let prefix = queue.prefix.clone();
    let name = queue.name.clone();
    let mut conn = queue.get_connection().await?;
    tokio::spawn(async move {
        while !cancel_worker.is_cancelled() {
            notifier_clone.notified().await;
            let metrics = get_job_metrics(&prefix, &name, &mut conn).await?;
            if metrics.all_jobs_completed() {
                cancel_worker.cancel();
            }
        }
        Ok::<(), KioError>(())
    });

    worker
        .on_all_events(move |event| {
            let notifier = notifier.clone();
            {
                async move {
                    if let EventParameters::Completed {
                        job,
                        prev_state: _,
                        result: _,
                    } = event
                    {
                        let id = job.id.unwrap();
                        let completed_in = (job.finished_on.unwrap() - job.processed_on.unwrap())
                            .num_milliseconds();
                        println!(" completed job {id} in {completed_in} mills");
                        notifier.notify_one();
                        if last_job_id.to_string() == id {
                            //   cancel.cancel();
                        }
                    }
                }
            }
        })
        .await;
    worker.run().await?;
    while worker.is_running() {}

    Ok(())
}

enum Payload {
    Progress(Progress),
    Log(String),
}
#[framed]
async fn process_callback(
    mut conn: Connection,
    mut job: Job<ProcessData, ReturnData, Progress>,
) -> KioResult<ReturnData> {
    let data = job.data.clone();
    let (sender, mut reciever) = tokio::sync::mpsc::channel(1000000000000);
    // task that recieves progress
    tokio::spawn(async move {
        while let Some(payload) = reciever.recv().await {
            match payload {
                Payload::Progress(ffmpeg_progress) => {
                    job.update_progress(ffmpeg_progress, &mut conn).await?;
                }
                Payload::Log(ref _log) => {
                    // TODO: do something with the logs here
                }
            }
        }
        Ok::<(), KioError>(())
    });
    let task = frame!(tokio::task::spawn_blocking(move || transcode_video(
        data, sender
    )));
    task.await?
}

#[framed]
fn transcode_video(
    data: Option<ProcessData>,
    payload_sender: Sender<Payload>,
) -> KioResult<ReturnData> {
    use uuid::Uuid;
    let data = data.unwrap_or_default();
    let input_path = data.path.to_str().expect("failed to extract");

    let size = data.size;
    let random = Uuid::new_v4();
    let output_path = format!(
        "compressed/{}x{}-{random}-output.mp4",
        data.size.height, data.size.width
    );
    let expected_path = output_path.clone();
    let mut cmd = FfmpegCommand::new()
        .input(input_path)
        .size(size.height, size.width)
        .output(expected_path)
        .print_command()
        .spawn()?;
    let mut total_duration = 1.0;

    let ffmpeg_iter = cmd.iter().map_err(BoxedError::from)?;
    for event in ffmpeg_iter {
        match event {
            FfmpegEvent::Progress(progress) => {
                let parsed_duration = parse_time_str(&progress.time);
                let mut current_progress = Progress {
                    size_kb: progress.size_kb,
                    bitrate_kbps: progress.bitrate_kbps,
                    ..Default::default()
                };

                if let Some(time) = parsed_duration {
                    let percent = (time / total_duration) * 100.0;
                    if percent.is_sign_positive() {
                        current_progress.percentage = percent;
                        current_progress.current_duration = parsed_duration;
                    }
                }

                payload_sender
                    .blocking_send(Payload::Progress(current_progress))
                    .map_err(std::io::Error::other)?;
            }

            FfmpegEvent::Log(log_level, msg) => {
                if matches!(log_level, LogLevel::Error | LogLevel::Fatal) {
                    return Err(std::io::Error::other(msg).into());
                }
                if !msg.is_empty() {
                    let msg = msg.trim_ascii();
                    let log = format!("{log_level:?}: {msg}");
                    payload_sender
                        .blocking_send(Payload::Log(log))
                        .map_err(std::io::Error::other)?;
                }
            }

            FfmpegEvent::Error(failed_reason) => {
                if failed_reason != "No streams found" {
                    return Err(std::io::Error::other(failed_reason).into());
                }
            }
            FfmpegEvent::ParsedDuration(duration) => {
                total_duration = duration.duration;
            }

            FfmpegEvent::LogEOF => {
                return Ok(ReturnData {
                    output_path: output_path.into(),
                    processed_size: size,
                });
            }

            _ => {}
        }
    }
    Err(std::io::Error::other("failed to process video").into())
}
/// create a H265 source video from scratch
fn create_h265_source(path_str: &str) {
    println!("Creating H265 source video: {path_str}");
    FfmpegCommand::new()
        .args("-f lavfi -i testsrc=size=1920x1080:rate=30:duration=15 -c:v libx265".split(' '))
        .arg(path_str)
        .spawn()
        .expect("failed to spawn")
        .iter()
        .expect("failed to get iter")
        .for_each(|e| match e {
            FfmpegEvent::Log(LogLevel::Error, e) => println!("Error: {e}"),
            FfmpegEvent::Progress(p) => println!("Progress: {} / 00:00:15", p.time),
            _ => {}
        });
    println!("Created H265 source video: {path_str}");
}
