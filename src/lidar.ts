import { ensureDir, exists } from "@std/fs";
import { join } from "@std/path";
import {
  LIDAR_FILES_DIR_NAME,
  LIDAR_STEP_DIR_NAME,
  LIDAR_STEP_ENDPOINT_PATH,
} from "./constants.ts";
import { LidarJob } from "./next-job-schema.ts";
import { compressDirectory, executeCommand, log } from "./utils.ts";
import { JobHandlingAdditionnalArguments } from "./models.ts";

export async function handleLidarJob(
  { tileId, tileUrl }: LidarJob["data"],
  { threadNumber, mapantApiWorkerId, mapantApiToken, mapantApiBaseUrl }:
    JobHandlingAdditionnalArguments,
) {
  const lidarFilePath = join(LIDAR_FILES_DIR_NAME, `${tileId}.laz`);
  const lidarStepOutputDirPath = join(LIDAR_STEP_DIR_NAME, tileId);
  const lidarStepArchiveFileName = `${tileId}.tar.xz`;
  const lidarStepArchivePath = join(LIDAR_STEP_DIR_NAME, lidarStepArchiveFileName);

  try {
    await ensureDir(LIDAR_FILES_DIR_NAME);

    log(`Tile ${tileId} | Downloading LiDAR file`, { level: "info", threadNumber });
    const fileResponse = await fetch(tileUrl);

    if (!fileResponse.ok || !fileResponse.body) {
      throw new Error(
        `Could not download LiDAR file.\nResponse status: ${fileResponse.status}.\n${await fileResponse
          .text()}`,
      );
    }

    const file = await Deno.open(lidarFilePath, { write: true, create: true });
    await fileResponse.body.pipeTo(file.writable);

    log(`Tile ${tileId} | LiDAR file downloaded`, { level: "info", threadNumber });

    await ensureDir(lidarStepOutputDirPath);

    log(`Tile ${tileId} | Running cassini lidar step`, { level: "info", threadNumber });
    await executeCommand("cassini", "lidar", lidarFilePath, "-o", lidarStepOutputDirPath);
    await checkIfCassiniExecutionWentOk(lidarStepOutputDirPath);
    log(`Tile ${tileId} | Cassini lidar step done`, { level: "info", threadNumber });

    log(`Tile ${tileId} | Compressing lidar step`, { level: "info", threadNumber });
    await compressDirectory(lidarStepOutputDirPath, lidarStepArchivePath);
    log(`Tile ${tileId} | Compressing lidar step done`, { level: "info", threadNumber });

    log(`Tile ${tileId} | Uploading lidar step`, { level: "info", threadNumber });
    const archiveFile = await Deno.readFile(lidarStepArchivePath);
    const formData = new FormData();

    formData.append(
      "file",
      new Blob([archiveFile], { type: "application/x-bzip2" }),
      lidarStepArchiveFileName,
    );

    const uploadResponse = await fetch(`${mapantApiBaseUrl}${LIDAR_STEP_ENDPOINT_PATH}/${tileId}`, {
      method: "POST",
      body: formData,
      headers: {
        "Origin": mapantApiBaseUrl,
        "Authorization": `Bearer ${mapantApiWorkerId}.${mapantApiToken}`,
      },
    });

    if (!uploadResponse.ok) {
      throw new Error(
        `Could not upload LiDAR step result.\nResponse status: ${uploadResponse.status}.\n${await uploadResponse
          .text()}`,
      );
    }

    log(`Tile ${tileId} | Lidar step result upload done`, { level: "info", threadNumber });
  } catch (e) {
    if (await exists(lidarFilePath)) Deno.remove(lidarFilePath);
    if (await exists(lidarStepArchivePath)) Deno.remove(lidarStepArchivePath);

    if (await exists(lidarStepOutputDirPath)) {
      Deno.remove(lidarStepOutputDirPath, { recursive: true });
    }

    throw e;
  }

  Deno.remove(lidarFilePath);
  Deno.remove(lidarStepArchivePath);
}

async function checkIfCassiniExecutionWentOk(lidarStepOutputDirPath: string) {
  if (
    !(await exists(join(lidarStepOutputDirPath, "dem.tif"))) ||
    !(await exists(join(lidarStepOutputDirPath, "dem-low-resolution.tif"))) ||
    !(await exists(join(lidarStepOutputDirPath, "high-vegetation.tif"))) ||
    !(await exists(join(lidarStepOutputDirPath, "medium-vegetation.tif"))) ||
    !(await exists(join(lidarStepOutputDirPath, "extent.txt"))) ||
    !(await exists(join(lidarStepOutputDirPath, "pipeline.json")))
  ) {
    throw new Error("An error occured while executing cassini lidar command");
  }
}
