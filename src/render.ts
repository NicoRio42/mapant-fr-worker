import { join } from "jsr:@std/path";
import { RenderJob } from "./next-job-schema.ts";
import { LIDAR_STEP_DIR_NAME, RENDER_STEP_DIR_NAME } from "./constants.ts";
import { ensureDir, exists } from "@std/fs";
import { Extent, JobHandlingAdditionnalArguments } from "./models.ts";
import { compressDirectory, executeCommand, log } from "./utils.ts";
import { string } from "zod";

export async function handleRenderJob(
  { tileId, neigbhoringTilesIds }: RenderJob["data"],
  options: JobHandlingAdditionnalArguments,
) {
  const { threadNumber, mapantApiBaseUrl, mapantApiToken, mapantApiWorkerId } = options;

  log(`Tile ${tileId} | Downloading LiDAR step assets for tile and neigbhors.`, {
    level: "info",
    threadNumber,
  });

  await Promise.allSettled([
    downloadAndDecompressLidarStepArchive(tileId, options),
    ...neigbhoringTilesIds.map((neigbhoringTileId) =>
      downloadAndDecompressLidarStepArchive(neigbhoringTileId, options)
    ),
  ]);

  await ensureDir(RENDER_STEP_DIR_NAME);
  const tileRenderStepOutputDirPath = join(RENDER_STEP_DIR_NAME, tileId);
  log(`Tile ${tileId} | Executing Cassini render step`, { level: "info", threadNumber });

  await executeCommand(
    "cassini",
    "render",
    "-o",
    tileRenderStepOutputDirPath,
    "-n",
    ...neigbhoringTilesIds.map((neigbhoringTileId) => join(LIDAR_STEP_DIR_NAME, neigbhoringTileId)),
    "--skip-520",
  );

  log(`Tile ${tileId} | Cassini render step done`, { level: "info", threadNumber });

  const lidarStepTileDirPath = join(LIDAR_STEP_DIR_NAME, tileId);
  const tileExtent = await getExtentFromLidarDirPath(lidarStepTileDirPath);

  await clipAndCompressRasters({
    lidarStepTileDirPath,
    tileExtent,
    tileId,
    tileRenderStepOutputDirPath,
  });
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

  const onGoingJob = downloadJobs.get(tileId);

  if (onGoingJob !== undefined) {
    log(`Tile ${tileId} | LiDAR step assets already being downloaded`, {
      level: "info",
      threadNumber,
    });
    return onGoingJob;
  }

  const url = `"${mapantApiBaseUrl}/api/map-generation/lidar-steps/${tileId}"`;

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
    .then(() => {
      downloadJobs.delete(tileId);
    });

  downloadJobs.set(tileId, job);

  return job;
}

async function clipAndCompressRasters(
  { lidarStepTileDirPath, tileRenderStepOutputDirPath, tileExtent, tileId }: {
    lidarStepTileDirPath: string;
    tileRenderStepOutputDirPath: string;
    tileExtent: Extent;
    tileId: string;
  },
) {
  let rastersPath = join(tileRenderStepOutputDirPath, "rasters");
  await ensureDir(rastersPath);

  cropTiffImage(
    {
      inputFilePath: join(tileRenderStepOutputDirPath, "dem-with-buffer.tif"),
      outputFilePath: join(rastersPath, "dem.tif"),
      tileExtent,
    },
  );

  cropTiffImage(
    {
      inputFilePath: join(tileRenderStepOutputDirPath, "dem-low-resolution-with-buffer.tif"),
      outputFilePath: join(rastersPath, "dem-low-resolution.tif"),
      tileExtent,
    },
  );

  cropTiffImage(
    {
      inputFilePath: join(tileRenderStepOutputDirPath, "high-vegetation-with-buffer.tif"),
      outputFilePath: join(rastersPath, "high-vegetation.tif"),
      tileExtent,
    },
  );

  cropTiffImage(
    {
      inputFilePath: join(tileRenderStepOutputDirPath, "medium-vegetation-with-buffer.tif"),
      outputFilePath: join(rastersPath, "medium-vegetation.tif"),
      tileExtent,
    },
  );

  cropTiffImage(
    {
      inputFilePath: join(tileRenderStepOutputDirPath, "slopes.tif"),
      outputFilePath: join(rastersPath, "slopes.tif"),
      tileExtent,
    },
  );

  Deno.copyFile(
    join(lidarStepTileDirPath, "extent.txt"),
    join(rastersPath, "extent.txt"),
  );

  Deno.copyFile(
    join(lidarStepTileDirPath, "pipeline.json"),
    join(rastersPath, "pipeline.json"),
  );

  let rastersArchiveFileName = `rasters_${tileId}.tar.xz`;
  let rastersArchivePath = join(tileRenderStepOutputDirPath, rastersArchiveFileName);
  return compressDirectory(rastersPath, rastersArchivePath);
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
