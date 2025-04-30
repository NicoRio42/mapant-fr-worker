import { join } from "@std/path";
import { RenderJob } from "./next-job-schema.ts";
import {
  HIGH_QUALITY_TILE_PIXEL_SIZE,
  LIDAR_STEP_DIR_NAME,
  RENDER_STEP_DIR_NAME,
  RENDER_STEP_ENDPOINT_PATH,
  SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING,
} from "./constants.ts";
import { ensureDir, exists } from "@std/fs";
import { Extent, JobHandlingAdditionnalArguments } from "./models.ts";
import {
  compressDirectory,
  executeCommand,
  fetchWithRetryAndTimeout,
  log,
  removeIfExists,
} from "./utils.ts";
import sharp from "sharp";

export async function handleRenderJob(
  { tileId, neigbhoringTilesIds }: RenderJob["data"],
  options: JobHandlingAdditionnalArguments,
) {
  const { threadNumber, mapantApiBaseUrl, mapantApiToken, mapantApiWorkerId } = options;

  log(`Tile ${tileId} | Downloading LiDAR step assets for tile and neigbhors.`, {
    level: "info",
    threadNumber,
  });

  const tileRenderStepOutputDirPath = join(RENDER_STEP_DIR_NAME, tileId);

  try {
    // We want to throw if one download fails
    await Promise.all(
      [...neigbhoringTilesIds, tileId].map((neigbhoringTileId) =>
        downloadAndDecompressLidarStepArchive(neigbhoringTileId, options)
      ),
    );

    log(`Tile ${tileId} | LiDAR step assets for tile and neigbhors downloaded.`, {
      level: "info",
      threadNumber,
    });

    log(`Tile ${tileId} | Executing Cassini render step`, { level: "info", threadNumber });
    await ensureDir(RENDER_STEP_DIR_NAME);

    await executeCommand(
      "cassini",
      "render",
      "-o",
      tileRenderStepOutputDirPath,
      ...neigbhoringTilesIds.flatMap((
        neigbhoringTileId,
      ) => ["-n", join(LIDAR_STEP_DIR_NAME, neigbhoringTileId)]),
      "--skip-520",
      join(LIDAR_STEP_DIR_NAME, tileId),
    );

    log(`Tile ${tileId} | Cassini render step done`, { level: "info", threadNumber });

    log(`Tile ${tileId} | Clipping and compressing render step result files`, {
      level: "info",
      threadNumber,
    });
    const lidarStepTileDirPath = join(LIDAR_STEP_DIR_NAME, tileId);
    const tileExtent = await getExtentFromLidarDirPath(lidarStepTileDirPath);

    const rastersPath = join(tileRenderStepOutputDirPath, "rasters");
    await ensureDir(rastersPath);

    await clipAndCompressRasters({
      lidarStepTileDirPath,
      tileExtent,
      rastersPath,
      tileRenderStepOutputDirPath,
    });

    const rastersArchiveFileName = `rasters_${tileId}.tar.xz`;
    const rastersArchivePath = join(tileRenderStepOutputDirPath, rastersArchiveFileName);
    await compressDirectory(rastersPath, rastersArchivePath);

    const shapefilesPath = join(tileRenderStepOutputDirPath, "shapefiles");
    await clipAndCompressShapefiles({ tileRenderStepOutputDirPath, tileExtent, shapefilesPath });
    const shapefilesArchiveName = `shapefiles_${tileId}.tar.xz`;
    const shapefilesArchivePath = join(tileRenderStepOutputDirPath, shapefilesArchiveName);
    await compressDirectory(shapefilesPath, shapefilesArchivePath);

    const pngsPath = join(tileRenderStepOutputDirPath, "pngs");
    await ensureDir(pngsPath);

    await resizeOrCopyPngs({ tileExtent, tileId, tileRenderStepOutputDirPath, pngsPath });

    const pngsArchiveFileName = `pngs_${tileId}.tar.xz`;
    const pngsArchivePath = join(tileRenderStepOutputDirPath, pngsArchiveFileName);
    await compressDirectory(pngsPath, pngsArchivePath);

    log(`Tile ${tileId} | Clipping and compressing done`, { level: "info", threadNumber });

    log(`Tile ${tileId} | Uploading render step result files`, { level: "info", threadNumber });

    const formData = new FormData();

    formData.append(
      "rasters",
      new Blob([await Deno.readFile(rastersArchivePath)], { type: "application/x-bzip2" }),
      rastersArchiveFileName,
    );

    formData.append(
      "shapefiles",
      new Blob([await Deno.readFile(shapefilesArchivePath)], { type: "application/x-bzip2" }),
      shapefilesArchiveName,
    );

    formData.append(
      "pngs",
      new Blob([await Deno.readFile(pngsArchivePath)], { type: "application/x-bzip2" }),
      pngsArchiveFileName,
    );

    formData.append(
      "full-map",
      new Blob([await Deno.readFile(join(tileRenderStepOutputDirPath, "full-map.png"))], {
        type: "image/png",
      }),
      "full-map.png",
    );

    await fetchWithRetryAndTimeout(`${mapantApiBaseUrl}${RENDER_STEP_ENDPOINT_PATH}/${tileId}`, {
      method: "POST",
      body: formData,
      headers: {
        "Origin": mapantApiBaseUrl,
        "Authorization": `Bearer ${mapantApiWorkerId}.${mapantApiToken}`,
      },
    });

    log(`Tile ${tileId} | Uploading render step result files done`, {
      level: "info",
      threadNumber,
    });
  } catch (e) {
    await removeIfExists(tileRenderStepOutputDirPath, { recursive: true });

    throw e;
  }
}

const downloadJobs: Map<string, Promise<void>> = new Map();

async function downloadAndDecompressLidarStepArchive(
  tileId: string,
  options: JobHandlingAdditionnalArguments,
): Promise<void> {
  const { threadNumber, mapantApiBaseUrl, mapantApiToken, mapantApiWorkerId } = options;
  const lidarStepTileDirPath = join(LIDAR_STEP_DIR_NAME, tileId);

  if (await exists(lidarStepTileDirPath)) {
    log(`Tile ${tileId} | LiDAR step assets already on disk`, { level: "info", threadNumber });
    return;
  }

  const lidarStepTileArchivePath = join(LIDAR_STEP_DIR_NAME, `${tileId}.tar.xz`);
  await ensureDir(LIDAR_STEP_DIR_NAME);

  const onGoingJob = downloadJobs.get(tileId);

  if (onGoingJob !== undefined) {
    log(`Tile ${tileId} | LiDAR step assets already being downloaded`, {
      level: "info",
      threadNumber,
    });
    return onGoingJob;
  }

  const url = `${mapantApiBaseUrl}/api/map-generation/lidar-steps/${tileId}`;

  log(`Tile ${tileId} | Downloading LiDAR step assets`, { level: "info", threadNumber });

  const job = fetch(url, {
    headers: {
      "Authorization": `Bearer ${mapantApiWorkerId}.${mapantApiToken}`,
    },
  })
    .then(async (response) => {
      if (!response.ok || response.body === null) {
        throw new Error(
          `Tile ${tileId} | Could not fetch LiDAR step assets, Status: ${response.status}, ${await response
            .text()}`,
        );
      }

      const file = await Deno.open(lidarStepTileArchivePath, { write: true, create: true });
      return response.body.pipeTo(file.writable);
    })
    .then(() => Deno.mkdir(lidarStepTileDirPath))
    .then(() => executeCommand("tar", "-xvf", lidarStepTileArchivePath, "-C", lidarStepTileDirPath))
    .catch(async (e) => {
      await removeIfExists(lidarStepTileDirPath);
      log(e, { level: "error", threadNumber });
      return e;
    }).finally(async () => {
      await removeIfExists(lidarStepTileArchivePath);
      return downloadJobs.delete(tileId);
    });

  downloadJobs.set(tileId, job);

  return job;
}

async function clipAndCompressRasters(
  { lidarStepTileDirPath, tileRenderStepOutputDirPath, tileExtent, rastersPath }: {
    lidarStepTileDirPath: string;
    tileRenderStepOutputDirPath: string;
    tileExtent: Extent;
    rastersPath: string;
  },
) {
  await Promise.allSettled([
    cropTiffImage(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "dem-with-buffer.tif"),
        outputFilePath: join(rastersPath, "dem.tif"),
        tileExtent,
      },
    ),
    cropTiffImage(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "dem-low-resolution-with-buffer.tif"),
        outputFilePath: join(rastersPath, "dem-low-resolution.tif"),
        tileExtent,
      },
    ),
    cropTiffImage(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "high-vegetation-with-buffer.tif"),
        outputFilePath: join(rastersPath, "high-vegetation.tif"),
        tileExtent,
      },
    ),
    cropTiffImage(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "medium-vegetation-with-buffer.tif"),
        outputFilePath: join(rastersPath, "medium-vegetation.tif"),
        tileExtent,
      },
    ),
    cropTiffImage(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "slopes.tif"),
        outputFilePath: join(rastersPath, "slopes.tif"),
        tileExtent,
      },
    ),
  ]);

  await Promise.allSettled([
    Deno.copyFile(
      join(lidarStepTileDirPath, "extent.txt"),
      join(rastersPath, "extent.txt"),
    ),

    Deno.copyFile(
      join(lidarStepTileDirPath, "pipeline.json"),
      join(rastersPath, "pipeline.json"),
    ),
  ]);
}

async function clipAndCompressShapefiles(
  { tileRenderStepOutputDirPath, tileExtent, shapefilesPath }: {
    tileRenderStepOutputDirPath: string;
    tileExtent: Extent;
    shapefilesPath: string;
  },
) {
  const vectorsPath = join(shapefilesPath, "vectors");
  const contoursPath = join(shapefilesPath, "contours");
  const contoursRawPath = join(shapefilesPath, "contours-raw");
  const formlinesPath = join(shapefilesPath, "formlines");

  await Promise.allSettled([
    ensureDir(vectorsPath),
    ensureDir(contoursPath),
    ensureDir(contoursRawPath),
    ensureDir(formlinesPath),
  ]);

  await Promise.allSettled([
    clipShapefilesWithSmallBuffer(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "shapes", "lines.shp"),
        outputFilePath: join(vectorsPath, "lines.shp"),
        tileExtent,
      },
    ),
    clipShapefilesWithSmallBuffer(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "shapes", "multipolygons.shp"),
        outputFilePath: join(vectorsPath, "multipolygons.shp"),
        tileExtent,
      },
    ),
    clipShapefilesWithSmallBuffer(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "contours", "contours.shp"),
        outputFilePath: join(contoursPath, "contours.shp"),
        tileExtent,
      },
    ),
    clipShapefilesWithSmallBuffer(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "contours-raw", "contours-raw.shp"),
        outputFilePath: join(contoursRawPath, "contours-raw.shp"),
        tileExtent,
      },
    ),
    clipShapefilesWithSmallBuffer(
      {
        inputFilePath: join(tileRenderStepOutputDirPath, "formlines", "formlines.shp"),
        outputFilePath: join(formlinesPath, "formlines.shp"),
        tileExtent,
      },
    ),
  ]);
}

async function cropTiffImage(
  { inputFilePath, outputFilePath, tileExtent: { minX, minY, maxX, maxY } }: {
    inputFilePath: string;
    outputFilePath: string;
    tileExtent: Extent;
  },
) {
  return executeCommand(
    "gdal_translate",
    "-projwin",
    minX.toString(),
    maxY.toString(),
    maxX.toString(),
    minY.toString(),
    "-of",
    "GTiff",
    inputFilePath,
    outputFilePath,
    "--quiet",
  );
}

async function getExtentFromLidarDirPath(lidarDirPath: string): Promise<Extent> {
  const content = await Deno.readTextFile(join(lidarDirPath, "extent.txt"));
  const parts = content.split("|").map((part) => parseInt(part, 10));

  if (parts.length !== 4 || parts.some(isNaN)) {
    throw new Error("The extent.txt file is corrupted");
  }

  return { minX: parts[0], minY: parts[1], maxX: parts[2], maxY: parts[3] };
}

async function clipShapefilesWithSmallBuffer(
  { inputFilePath, outputFilePath, tileExtent: { minX, minY, maxX, maxY } }: {
    inputFilePath: string;
    outputFilePath: string;
    tileExtent: Extent;
  },
) {
  return executeCommand(
    "ogr2ogr",
    "-f",
    "ESRI Shapefile",
    outputFilePath,
    inputFilePath,
    "-clipsrc",
    (minX - SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING).toString(),
    (minY - SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING).toString(),
    (maxX + SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING).toString(),
    (maxY + SMALL_BUFFER_FOR_SHAPEFILES_CLIPPING).toString(),
  );
}

function getExtentFromTileId(tile_id: string): Extent {
  const parts = tile_id
    .trim()
    .split("_")
    .map((p) => parseInt(p, 10));

  if (parts.length !== 2) {
    throw new Error("Problem parsing extent from tile id");
  }

  return { minX: parts[0], minY: parts[1], maxX: parts[0] + 1000, maxY: parts[1] + 1000 };
}

async function resizePngToHighQualitySquare(
  { extent: { minX, minY, maxX, maxY }, imageToResizePath, outputPath, realMaxY, realMinX }: {
    imageToResizePath: string;
    outputPath: string;
    extent: Extent;
    realMinX: number;
    realMaxY: number;
  },
) {
  const left = HIGH_QUALITY_TILE_PIXEL_SIZE * (realMinX - minX) / (maxX - minX);
  const top = HIGH_QUALITY_TILE_PIXEL_SIZE * (maxY - realMaxY) / (maxY - minY);

  return sharp({
    create: {
      width: HIGH_QUALITY_TILE_PIXEL_SIZE,
      height: HIGH_QUALITY_TILE_PIXEL_SIZE,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  }).composite([{
    input: imageToResizePath,
    left,
    top,
  }]).toFile(outputPath);
}

async function resizeOrCopyPngs(
  { tileRenderStepOutputDirPath, pngsPath, tileExtent, tileId }: {
    tileExtent: Extent;
    tileId: string;
    tileRenderStepOutputDirPath: string;
    pngsPath: string;
  },
) {
  const { minX: realMinX, minY: realMinY, maxX: realMaxX, maxY: realMaxY } = tileExtent;
  const extent = getExtentFromTileId(tileId);
  const { minX, minY, maxX, maxY } = extent;

  if (realMinX !== minX || realMinY !== minY || realMaxX !== maxX || realMaxY !== maxY) {
    await Promise.allSettled([
      resizePngToHighQualitySquare(
        {
          imageToResizePath: join(tileRenderStepOutputDirPath, "cliffs.png"),
          outputPath: join(pngsPath, "cliffs.png"),
          extent,
          realMinX,
          realMaxY,
        },
      ),
      resizePngToHighQualitySquare(
        {
          imageToResizePath: join(tileRenderStepOutputDirPath, "contours.png"),
          outputPath: join(pngsPath, "contours.png"),
          extent,
          realMinX,
          realMaxY,
        },
      ),
      resizePngToHighQualitySquare(
        {
          imageToResizePath: join(tileRenderStepOutputDirPath, "vegetation.png"),
          outputPath: join(pngsPath, "vegetation.png"),
          extent,
          realMinX,
          realMaxY,
        },
      ),
      resizePngToHighQualitySquare(
        {
          imageToResizePath: join(tileRenderStepOutputDirPath, "full-map.png"),
          outputPath: join(tileRenderStepOutputDirPath, "full-map.png"),
          extent,
          realMinX,
          realMaxY,
        },
      ),
    ]);
  } else {
    await Promise.allSettled([
      Deno.copyFile(join(tileRenderStepOutputDirPath, "cliffs.png"), join(pngsPath, "cliffs.png")),
      Deno.copyFile(
        join(tileRenderStepOutputDirPath, "contours.png"),
        join(pngsPath, "contours.png"),
      ),
      Deno.copyFile(
        join(tileRenderStepOutputDirPath, "vegetation.png"),
        join(pngsPath, "vegetation.png"),
      ),
    ]);
  }
}
