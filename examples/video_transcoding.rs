use deadpool_redis::Config;
use kio_mq::{
    fetch_redis_pass, framed, EventParameters, Job, KioResult, Queue, RedisStore, Store, Worker,
    WorkerOpts,
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;
type BoxedError = Box<dyn std::error::Error + Send>;
use ffmpeg_sidecar::{
    command::FfmpegCommand,
    download::auto_download,
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
    pub processed_size: Size,
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
    console_subscriber::init();
    let input_path = "sampleFHD.mp4";
    let mut config = Config::default();
    if let Some(cfg) = config.connection.as_mut() {
        cfg.redis.password = fetch_redis_pass();
    }
    let store = RedisStore::new(None, "video-processing", &config).await?;
    let queue = Queue::new(store, None).await?;
    let processor = |con: _, job: _| process_callback(con, job);
    // auto download ffmpeg if it's not installed;
    tokio::task::spawn_blocking(auto_download)
        .await?
        .map_err(std::io::Error::other)?;

    if !Path::new(input_path).exists() {
        tokio::task::spawn_blocking(|| create_h265_source(input_path)).await?;
    }
    // create the compressed folder if its doesn't exist too;
    if !Path::new("compressed").exists() {
        fs::create_dir("compressed").await?;
    }
    let sizes = [(1280, 720), (640, 480), (1920, 1080), (3840, 2160)];
    let iter = sizes.into_iter().map(|(height, width)| {
        let size = Size { height, width };
        let data = ProcessData {
            size,
            path: input_path.into(),
        };
        (height.to_string().to_lowercase(), None, data)
    });

    let opts = WorkerOpts {
        concurrency: sizes.len(),
        lock_duration: 120000,
        stalled_interval: 120000,

        ..Default::default()
    };
    let worker = Worker::new_sync(&queue, processor, Some(opts))?;
    let updating_metrics = queue.current_metrics.clone();

    worker.on_all_events(move |event| async move {
        if let EventParameters::Completed {
            job,
            prev_state: _,
            result,
        } = event
        {
            let id = job.id.unwrap();
            let completed_in =
                (job.finished_on.unwrap() - job.processed_on.unwrap()).num_milliseconds();
            let size = result.processed_size;
            println!(" completed job {id}  for {size:?} in {completed_in} mills");
        }
    });

    worker.run()?;
    queue.bulk_add_only(iter).await?;

    while !updating_metrics.all_jobs_completed() {}
    worker.close(true);
    if worker.closed() {
        queue.obliterate().await?;
    }

    Ok(())
}

#[framed]
fn process_callback(
    store: Arc<RedisStore>,
    job: Job<ProcessData, ReturnData, Progress>,
) -> KioResult<ReturnData> {
    transcode_video(store, job)
}

#[framed]
fn transcode_video(
    store: Arc<RedisStore>,
    mut job: Job<ProcessData, ReturnData, Progress>,
) -> KioResult<ReturnData> {
    use uuid::Uuid;
    let data = job.data.clone().unwrap_or_default();
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
                        current_progress.percentage = percent.round();
                        current_progress.current_duration = parsed_duration;
                    }
                }
                store.update_job_progress(&mut job, current_progress)?;
            }

            FfmpegEvent::Log(log_level, msg) => {
                if matches!(log_level, LogLevel::Error | LogLevel::Fatal) {
                    return Err(std::io::Error::other(msg).into());
                }
                if !msg.is_empty() {
                    //let msg = msg.trim_ascii();
                    //let log = format!("{log_level:?}: {msg}");
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
