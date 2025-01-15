mod lidar;
mod utils;

use clap::Parser;
use dotenv::dotenv;
use lidar::lidar_step;
use reqwest;
use serde::{Deserialize, Serialize};
use std::{
    env,
    thread::{sleep, spawn, JoinHandle},
    time::Duration,
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
    Lidar { x: i32, y: i32, tile_url: String },
    Render { x: i32, y: i32 },
    Pyramid { x: i32, y: i32, z: i32 },
    NoJobLeft,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let mapant_api_worker_id = env::var("MAPANT_API_WORKER_ID")
        .expect("MAPANT_API_WORKER_ID environment variable not set.");
    let mapant_api_token =
        env::var("MAPANT_API_TOKEN").expect("MAPANT_API_TOKEN environment variable not set.");
    let mapant_api_base_url =
        env::var("MAPANT_API_BASE_URL").unwrap_or_else(|_| "https://mapant.fr/api".to_string());

    let args = Args::parse();
    let threads = args.threads.unwrap_or(3);

    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(threads);

    for thread_index in 0..threads {
        let worker_id = mapant_api_worker_id.clone();
        let token = mapant_api_token.clone();
        let base_url = mapant_api_base_url.clone();

        let spawned_thread = spawn(move || {
            get_and_handle_next_job(&worker_id, &token, &base_url, thread_index);
            sleep(Duration::from_millis(1));
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
    thread_index: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Thread {}", thread_index);
    let client = reqwest::blocking::Client::new();
    let url = format!("{}/map-generation/next-job", base_url);

    let res = client
        .post(&url)
        .header("Authorization", format!("Bearer {}.{}", worker_id, token))
        .send()?;

    if !res.status().is_success() {
        println!("Failed to call mapant generation 'next-job' endpoint");
        return Err("Failed to call endpoint".into());
    }

    let text = res.text()?;
    let job: Job = serde_json::from_str(&text)?;

    match job {
        Job::Lidar { x, y, tile_url } => {
            println!("Handle Lidar job: x={}, y={}, url={}", x, y, tile_url);
            lidar_step(x, y, tile_url, worker_id, token, base_url)?;
            get_and_handle_next_job(worker_id, token, base_url, thread_index)?;
        }
        Job::Render { x, y } => {
            // Handle Render job
            println!("Handle Render job: x={}, y={}", x, y);
            // Implement further logic as needed
        }
        Job::Pyramid { x, y, z } => {
            // Handle Pyramid job
            println!("Handle Pyramid job: x={}, y={}, z={}", x, y, z);
            // Implement further logic as needed
        }
        Job::NoJobLeft => {
            println!("No job left, retrying in 2 minutes");
            std::thread::sleep(std::time::Duration::from_secs(120));
            // Retry or end as needed
        }
    }

    Ok(())
}
