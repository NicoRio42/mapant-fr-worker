use cassini::{get_extent_from_lidar_dir_path, process_single_tile_render_step};
use log::{error, info};
use reqwest::header::{HeaderMap, HeaderValue};
use std::{
    fs::{self, create_dir_all, remove_dir_all},
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    time::Instant,
};

use crate::utils::{compress_directory, decompress_archive, download_file, upload_files};

const SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING: i64 = 20;

pub fn render_step(
    tile_id: &str,
    neigbhoring_tiles_ids: &Vec<String>,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let lidar_step_base_dir_path = Path::new("lidar-step");

    if !lidar_step_base_dir_path.exists() {
        create_dir_all(lidar_step_base_dir_path)?;
    }

    // Downloading lidar step files for the tile if not already on disk
    let lidar_step_tile_dir_path = lidar_step_base_dir_path.join(tile_id);

    download_and_decompress_lidar_step_files_if_not_on_disk(
        tile_id,
        worker_id,
        token,
        base_api_url,
        lidar_step_base_dir_path,
        &lidar_step_tile_dir_path,
    )?;

    let mut neighbor_tiles_lidar_step_dir_paths: Vec<PathBuf> = vec![];

    // Downloading lidar step files for the neigbhoring tiles if not already on disk
    for neigbhoring_tile_id in neigbhoring_tiles_ids {
        let neigbhoring_tile_lidar_step_dir_path =
            lidar_step_base_dir_path.join(neigbhoring_tile_id);

        download_and_decompress_lidar_step_files_if_not_on_disk(
            neigbhoring_tile_id,
            worker_id,
            token,
            base_api_url,
            lidar_step_base_dir_path,
            &neigbhoring_tile_lidar_step_dir_path,
        )?;

        neighbor_tiles_lidar_step_dir_paths.push(neigbhoring_tile_lidar_step_dir_path);
    }

    let render_step_path = Path::new("render-step");

    if !render_step_path.exists() {
        create_dir_all(render_step_path)?;
    }

    let output_dir_path = render_step_path.join(&tile_id);

    info!("Processing render step for tile {}", &tile_id);
    let start = Instant::now();

    process_single_tile_render_step(
        &lidar_step_tile_dir_path,
        &output_dir_path,
        neighbor_tiles_lidar_step_dir_paths,
        false,
    );

    let duration = start.elapsed();

    info!(
        "Render step for tile {} processed in {:.1?}",
        &tile_id, duration
    );

    // Crop tiff images
    let rasters_path = output_dir_path.join("rasters");
    create_dir_all(&rasters_path)?;
    let tile_extent = get_extent_from_lidar_dir_path(&lidar_step_tile_dir_path);

    crop_tiff_image(
        &output_dir_path.join("dem-with-buffer.tif"),
        &rasters_path.join("dem.tif"),
        tile_extent,
    )?;

    crop_tiff_image(
        &output_dir_path.join("dem-low-resolution-with-buffer.tif"),
        &rasters_path.join("dem-low-resolution.tif"),
        tile_extent,
    )?;

    crop_tiff_image(
        &output_dir_path.join("high-vegetation-with-buffer.tif"),
        &rasters_path.join("high-vegetation.tif"),
        tile_extent,
    )?;

    crop_tiff_image(
        &output_dir_path.join("medium-vegetation-with-buffer.tif"),
        &rasters_path.join("medium-vegetation.tif"),
        tile_extent,
    )?;

    crop_tiff_image(
        &output_dir_path.join("slopes.tif"),
        &rasters_path.join("slopes.tif"),
        tile_extent,
    )?;

    fs::copy(
        &lidar_step_tile_dir_path.join("extent.txt"),
        &rasters_path.join("extent.txt"),
    )?;

    fs::copy(
        &lidar_step_tile_dir_path.join("pipeline.json"),
        &rasters_path.join("pipeline.json"),
    )?;

    // Compress tiff images
    let rasters_archive_file_name = format!("rasters_{}.tar.xz", &tile_id);
    let rasters_archive_path = output_dir_path.join(&rasters_archive_file_name);
    compress_directory(&rasters_path, &rasters_archive_path)?;

    // Crop shapes
    let shapefiles_path = output_dir_path.join("shapefiles");
    let vectors_path = shapefiles_path.join("vectors");
    let contours_path = shapefiles_path.join("contours");
    let contours_raw_path = shapefiles_path.join("contours-raw");
    let formlines_path = shapefiles_path.join("formlines");
    create_dir_all(&vectors_path)?;
    create_dir_all(&contours_path)?;
    create_dir_all(&contours_raw_path)?;
    create_dir_all(&formlines_path)?;

    clip_shapefiles_with_small_buffer(
        &output_dir_path.join("shapes").join("lines.shp"),
        &vectors_path.join("lines.shp"),
        tile_extent,
    )?;

    clip_shapefiles_with_small_buffer(
        &output_dir_path.join("shapes").join("multilinestrings.shp"),
        &vectors_path.join("multilinestrings.shp"),
        tile_extent,
    )?;

    clip_shapefiles_with_small_buffer(
        &output_dir_path.join("shapes").join("multipolygons.shp"),
        &vectors_path.join("multipolygons.shp"),
        tile_extent,
    )?;

    clip_shapefiles_with_small_buffer(
        &output_dir_path.join("shapes").join("points.shp"),
        &vectors_path.join("points.shp"),
        tile_extent,
    )?;

    clip_shapefiles_with_small_buffer(
        &output_dir_path.join("contours").join("contours.shp"),
        &contours_path.join("contours.shp"),
        tile_extent,
    )?;

    clip_shapefiles_with_small_buffer(
        &output_dir_path
            .join("contours-raw")
            .join("contours-raw.shp"),
        &contours_raw_path.join("contours-raw.shp"),
        tile_extent,
    )?;

    clip_shapefiles_with_small_buffer(
        &output_dir_path.join("formlines").join("formlines.shp"),
        &formlines_path.join("formlines.shp"),
        tile_extent,
    )?;

    // Compress shapes
    let shapefiles_archive_file_name = format!("shapefiles_{}.tar.xz", &tile_id);
    let shapefiles_archive_path = output_dir_path.join(&shapefiles_archive_file_name);
    compress_directory(&shapefiles_path, &shapefiles_archive_path)?;

    // Copy pngs in the same directory
    let pngs_path = output_dir_path.join("pngs");
    create_dir_all(&pngs_path)?;

    fs::copy(
        &output_dir_path.join("cliffs.png"),
        &pngs_path.join("cliffs.png"),
    )?;

    fs::copy(
        &output_dir_path.join("contours.png"),
        &pngs_path.join("contours.png"),
    )?;

    fs::copy(
        &output_dir_path.join("vectors.png"),
        &pngs_path.join("vectors.png"),
    )?;

    fs::copy(
        &output_dir_path.join("vegetation.png"),
        &pngs_path.join("vegetation.png"),
    )?;

    // Compress pngs
    let pngs_archive_file_name = format!("pngs_{}.tar.xz", &tile_id);
    let pngs_archive_path = output_dir_path.join(&pngs_archive_file_name);
    compress_directory(&pngs_path, &pngs_archive_path)?;

    // Upload files
    let url = format!(
        "{}/api/map-generation/render-steps/{}",
        base_api_url, &tile_id
    );

    upload_files(
        worker_id,
        token,
        url,
        base_api_url,
        vec![
            (
                rasters_archive_file_name,
                "rasters".to_string(),
                rasters_archive_path,
                "application/x-bzip2".to_string(),
            ),
            (
                shapefiles_archive_file_name,
                "shapefiles".to_string(),
                shapefiles_archive_path,
                "application/x-bzip2".to_string(),
            ),
            (
                pngs_archive_file_name,
                "pngs".to_string(),
                pngs_archive_path,
                "application/x-bzip2".to_string(),
            ),
            (
                "full-map.png".to_string(),
                "full-map".to_string(),
                output_dir_path.join("full-map.png"),
                "image/png".to_string(),
            ),
        ],
    )?;

    Ok(())
}

fn download_and_decompress_lidar_step_files_if_not_on_disk(
    tile_id: &str,
    worker_id: &str,
    token: &str,
    base_api_url: &str,
    lidar_step_base_dir_path: &Path,
    lidar_step_tile_dir_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    if lidar_step_tile_dir_path.exists() && !lidar_step_tile_dir_path.join("extent.txt").exists() {
        info!(
            "Files from LiDAR step for tile {} already on disk but corrupted. Cleaning",
            &tile_id
        );

        remove_dir_all(lidar_step_tile_dir_path)?;
    }

    if !lidar_step_tile_dir_path.exists() {
        info!("Downloading files from LiDAR step for tile {}", &tile_id);
        let start = Instant::now();

        create_dir_all(lidar_step_tile_dir_path)?;

        let lidar_step_archive_url = format!(
            "{}/api/map-generation/lidar-steps/{}",
            base_api_url, tile_id
        );

        let lidar_step_archive_path = lidar_step_base_dir_path.join(format!("{}.tar.xz", tile_id));

        let mut headers = HeaderMap::new();

        headers.append(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}.{}", worker_id, token))?,
        );

        download_file(
            &lidar_step_archive_url,
            &lidar_step_archive_path,
            Some(headers),
        )?;

        let duration = start.elapsed();

        info!(
            "Files from LiDAR step for tile {} downloaded in {:.1?}",
            &tile_id, duration
        );

        info!("Decompressing files from LiDAR step for tile {}", &tile_id);
        let start = Instant::now();

        decompress_archive(&lidar_step_archive_path, lidar_step_tile_dir_path)?;

        let duration = start.elapsed();

        info!(
            "Files from LiDAR step for tile {} decompressed in {:.1?}",
            &tile_id, duration
        );
    } else {
        info!(
            "Files from LiDAR step for tile {} already on disk.",
            &tile_id
        );
    };

    Ok(())
}

fn crop_tiff_image(
    input_file_path: &PathBuf,
    output_file_path: &PathBuf,
    (min_x, min_y, max_x, max_y): (i64, i64, i64, i64),
) -> Result<(), Box<dyn std::error::Error>> {
    let gdal_translate_output = Command::new("gdal_translate")
        .args([
            "-projwin",
            &(min_x).to_string(),
            &(max_y).to_string(),
            &(max_x).to_string(),
            &(min_y).to_string(),
        ])
        .args(["-of", "GTiff"])
        .arg(input_file_path.to_str().unwrap())
        .arg(output_file_path.to_str().unwrap())
        .arg("--quiet")
        .output()
        .expect("failed to execute gdal_translate command");

    if !ExitStatus::success(&gdal_translate_output.status) {
        error!(
            "Tile min_x={} min_y={} max_x={} max_y={}. Gdal_translate command failed {:?}",
            min_x,
            min_y,
            max_x,
            max_y,
            String::from_utf8(gdal_translate_output.stderr).unwrap()
        );
    }

    Ok(())
}

fn clip_shapefiles_with_small_buffer(
    input_file_path: &PathBuf,
    output_file_path: &PathBuf,
    (min_x, min_y, max_x, max_y): (i64, i64, i64, i64),
) -> Result<(), Box<dyn std::error::Error>> {
    let ogr2ogr_output = Command::new("ogr2ogr")
        .arg("-f")
        .arg("ESRI Shapefile")
        .arg(output_file_path.to_str().unwrap())
        .arg(input_file_path.to_str().unwrap())
        .arg("-clipsrc")
        .args([
            &(min_x - SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING).to_string(),
            &(min_y - SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING).to_string(),
            &(max_x + SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING).to_string(),
            &(max_y + SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING).to_string(),
        ])
        .output()
        .expect("failed to execute ogr2ogr command");

    if !ExitStatus::success(&ogr2ogr_output.status) {
        error!(
            "Tile min_x={} min_y={} max_x={} max_y={}. Ogr2ogr command failed {:?}",
            min_x,
            min_y,
            max_x,
            max_y,
            String::from_utf8(ogr2ogr_output.stderr).unwrap()
        );
    }

    Ok(())
}
