import { ensureDir, exists } from "@std/fs";
import { join } from "jsr:@std/path";
import {
  LIDAR_FILES_DIR_NAME,
  LIDAR_STEP_DIR_NAME,
  LIDAR_STEP_ENDPOINT_PATH,
  MAPANT_API_BASE_URL,
} from "./constants.ts";
import { LidarJob } from "./next-job-schema.ts";
import { log } from "./utils.ts";

export async function handleLidarJob(
  { tileId, tileUrl }: LidarJob["data"],
  { threadNumber, mapantApiWorkerId, mapantApiToken, mapantApiBaseUrl }: {
    threadNumber: number;
    mapantApiWorkerId: string;
    mapantApiToken: string;
    mapantApiBaseUrl: string;
  },
) {
  const lidarFilePath = join(LIDAR_FILES_DIR_NAME, `${tileId}.laz`);
  const lidarStepOutputDirPath = join(LIDAR_STEP_DIR_NAME, tileId);
  const lidarStepArchiveFileName = `${tileId}.tar.xz`;
  const lidarStepArchivePath = join(LIDAR_STEP_DIR_NAME, lidarStepArchiveFileName);

  try {
    await ensureDir(LIDAR_FILES_DIR_NAME);

    log(`Downloading ${tileUrl}`, { level: "info", threadNumber });
    const fileResponse = await fetch(tileUrl);

    if (!fileResponse.ok || !fileResponse.body) {
      throw new Error(
        `Could not download LiDAR file.\nResponse status: ${fileResponse.status}.\n${await fileResponse
          .text()}`,
      );
    }

    const file = await Deno.open(lidarFilePath, { write: true, create: true });
    await fileResponse.body.pipeTo(file.writable);

    log(`File ${tileUrl} downloaded`, { level: "info", threadNumber });

    await ensureDir(lidarStepOutputDirPath);

    log(`Running cassini lidar step for tile ${tileId}`, { level: "info", threadNumber });

    {
      const { success, stderr } = await new Deno.Command("cassini", {
        args: ["lidar", lidarFilePath, "-o", lidarStepOutputDirPath],
      }).output();

      if (!success) {
        throw new Error(new TextDecoder().decode(stderr));
      }
    }

    await checkIfCassiniExecutionWentOk(lidarStepOutputDirPath);

    log(`Cassini lidar step for tile ${tileId} done`, { level: "info", threadNumber });
    log(`Compressing lidar step result for tile ${tileId}`, { level: "info", threadNumber });

    {
      const { success, stderr } = await new Deno.Command("tar", {
        args: ["-cJf", lidarStepArchivePath, "-C", lidarStepOutputDirPath, "."],
      }).output();

      if (!success) {
        throw new Error(new TextDecoder().decode(stderr));
      }
    }

    log(`Compressing lidar step result for tile ${tileId} done`, { level: "info", threadNumber });
    log(`Uploading lidar step result for tile ${tileId}`, { level: "info", threadNumber });

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

    log(`Lidar step result for tile ${tileId} upload done`, { level: "info", threadNumber });
  } catch (e) {
    if (await exists(lidarFilePath)) Deno.remove(lidarFilePath);
    if (await exists(lidarStepArchivePath)) Deno.remove(lidarStepArchivePath);

    if (await exists(lidarStepOutputDirPath)) {
      Deno.remove(lidarStepOutputDirPath, { recursive: true });
    }

    throw e;
  }

  Deno.remove(lidarFilePath);
  // Deno.remove(lidarStepArchivePath);
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
