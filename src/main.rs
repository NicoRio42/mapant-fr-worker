mod lidar;
mod pyramid;
mod render;
mod utils;

use clap::Parser;
use dotenv::dotenv;
use lidar::lidar_step;
use log::{error, info};
use pyramid::pyramid_step;
use render::render_step;
use reqwest::{self};
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::OpenOptions,
    sync::Mutex,
    thread::{self, sleep, spawn, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

// Update the docs when modifying
#[derive(Parser, Debug)]
#[command(version, about = "A worker node for the mapant.fr map generation")]
pub struct Args {
    #[arg(
        long,
        short,
        help = "Number of threads to parallelize the work",
        default_value = "3"
    )]
    threads: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "data")]
enum Job {
    Lidar {
        tile_id: String,
        tile_url: String,
    },
    Render {
        tile_id: String,
        neigbhoring_tiles_ids: Vec<String>,
    },
    Pyramid {
        x: i32,
        y: i32,
        z: i32,
        base_zoom_level_tile_id: Option<String>,
        area_id: String,
    },
    NoJobLeft,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = format!(
        "{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );

    let log_file_name = format!("app_{}.log", timestamp);

    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&log_file_name)
        .expect("Unable to open log file");

    // Wrap the file in a Mutex to allow safe concurrent access
    let log_file = Mutex::new(log_file);

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(move |buf, record| {
            use std::io::Write;
            let ts = buf.timestamp_seconds();
            let level_style = buf.default_level_style(record.level());

            // Write to console
            buf.write_all(
                format!(
                    "[{} {:?} {level_style}{}{level_style:#}] {}\n",
                    ts,
                    thread::current().id(),
                    record.level(),
                    record.args()
                )
                .as_bytes(),
            )
            .unwrap();

            // Write to the file
            let mut file = log_file.lock().unwrap();
            file.write_all(
                format!(
                    "[{} {:?} {}] {}\n",
                    ts,
                    thread::current().id(),
                    record.level(),
                    record.args()
                )
                .as_bytes(),
            )
            .unwrap();

            Ok(())
        })
        .init();

    dotenv().ok();

    let mapant_api_worker_id = env::var("MAPANT_API_WORKER_ID")
        .expect("MAPANT_API_WORKER_ID environment variable not set.");
    let mapant_api_token =
        env::var("MAPANT_API_TOKEN").expect("MAPANT_API_TOKEN environment variable not set.");
    let mapant_api_base_url =
        env::var("MAPANT_API_BASE_URL").unwrap_or_else(|_| "https://mapant.fr".to_string());

    let args = Args::parse();
    let threads = args.threads.unwrap_or(3);

    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(threads);

    for _ in 0..threads {
        let worker_id = mapant_api_worker_id.clone();
        let token = mapant_api_token.clone();
        let base_url = mapant_api_base_url.clone();

        let spawned_thread = spawn(move || loop {
            match get_and_handle_next_job(&worker_id, &token, &base_url) {
                Ok(_) => {
                    sleep(Duration::from_millis(1));
                }
                Err(error) => {
                    error!("Error: {}. Restarting the thread...", error);
                    sleep(Duration::from_secs(1));
                }
            }
        });

        handles.push(spawned_thread);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    return Ok(());
}

fn get_and_handle_next_job(
    worker_id: &str,
    token: &str,
    base_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::blocking::Client::new();
    let url = format!("{}/api/map-generation/next-job", base_url);

    let res = client
        .post(&url)
        .header("Authorization", format!("Bearer {}.{}", worker_id, token))
        .send()?;

    if !res.status().is_success() {
        error!(
            "Failed to call mapant generation 'next-job' endpoint. Status: {}",
            res.status()
        );

        return Err("Failed to call endpoint".into());
    }

    let text = res.text()?;
    let job: Job = serde_json::from_str(&text)?;

    match job {
        Job::Lidar { tile_id, tile_url } => {
            info!("Handle Lidar job for tile {}", tile_id);
            let start = Instant::now();

            lidar_step(&tile_id, &tile_url, worker_id, token, base_url)?;

            let duration = start.elapsed();
            info!("Lidar job for tile {} done in {:.1?}", &tile_id, duration);

            get_and_handle_next_job(worker_id, token, base_url)?;
        }
        Job::Render {
            tile_id,
            neigbhoring_tiles_ids,
        } => {
            info!("Handle Render job for tile {}", tile_id);
            let start = Instant::now();

            render_step(&tile_id, &neigbhoring_tiles_ids, worker_id, token, base_url)?;

            let duration = start.elapsed();
            info!("Render job for tile {} done in {:.1?}", &tile_id, duration);

            get_and_handle_next_job(worker_id, token, base_url)?;
        }
        Job::Pyramid {
            x,
            y,
            z,
            base_zoom_level_tile_id,
            area_id,
        } => {
            info!("Handle Pyramid job x={}, y={}, z={}", x, y, z);
            let start = Instant::now();

            pyramid_step(
                x,
                y,
                z,
                base_zoom_level_tile_id,
                area_id,
                worker_id,
                token,
                base_url,
            )?;

            let duration = start.elapsed();

            info!(
                "Pyramid job x={}, y={}, z={} done in {:.1?}",
                x, y, z, duration
            );

            get_and_handle_next_job(worker_id, token, base_url)?;
        }
        Job::NoJobLeft => {
            info!("No job left, retrying in 30 seconds");
            std::thread::sleep(std::time::Duration::from_secs(30));
            get_and_handle_next_job(worker_id, token, base_url)?;
        }
    }

    Ok(())
}
