import { nextJobApiEndpointResponseSchema } from "./schemas.ts";
import { join } from "jsr:@std/path";

const mapantApiWorkerId = Deno.env.get("MAPANT_API_WORKER_ID");

if (mapantApiWorkerId === undefined) {
  throw new Error("MAPANT_API_WORKER_ID environment variable not set.");
}

const mapantApiToken = Deno.env.get("MAPANT_API_TOKEN");

if (mapantApiToken === undefined) {
  throw new Error("MAPANT_API_TOKEN environment variable not set.");
}

const mapantApiBaseUrl = Deno.env.get("MAPANT_API_BASE_URL") ??
  "https://mapant.fr/api";

await getAndHandleNextJob();

async function getAndHandleNextJob() {
  const nextJobResponse = await fetch(
    `${mapantApiBaseUrl}/map-generation/next-job`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${mapantApiWorkerId}.${mapantApiToken}`,
      },
    },
  );

  if (!nextJobResponse.ok) {
    console.error('Failed to call mapant generation "next-job" endpoint');
    console.error(nextJobResponse.status);
    console.error(nextJobResponse.statusText);
    return;
  }

  const rawJsonResponse = await nextJobResponse.json();

  const parsedJsonResponse = nextJobApiEndpointResponseSchema.safeParse(
    rawJsonResponse,
  );

  if (!parsedJsonResponse.success) {
    console.error("Not expected next-job endpoint response");
    return;
  }

  const validResponse = parsedJsonResponse.data;

  if (validResponse.type === "no-job-left") {
    console.warn("No job left, retrying in 2 minutes");
    setTimeout(() => getAndHandleNextJob(), 2 * 60 * 1000);
  }

  if (validResponse.type === "lidar") {
    await createDirIfItDoenstExist("lidar-files");

    const outDirPath = join(
      "lidar-step",
      `${validResponse.data.x}_${validResponse.data.y}`,
    );

    await createDirIfItDoenstExist("lidar-step");
    const fileName = validResponse.data.tileUrl.split("/").reverse()[0];
    const filePath = join("lidar-files", fileName);

    console.log(`Downloading lidar file: ${fileName}`);
    await downloadFileToDisk(validResponse.data.tileUrl, filePath);

    console.log("Running cassini lidar subcommand");
    const exitCode = await runCassiniInDocker(
      "lidar",
      filePath,
      "-o",
      outDirPath,
    );

    if (exitCode !== 0) return;

    console.log("Compressing lidar step files");
    const [archivePath, compressExitCode] = await compressDir(
      outDirPath,
      "lidar-step",
      `${validResponse.data.x}_${validResponse.data.y}`,
    );

    if (compressExitCode !== 0) return;
  }

  if (validResponse.type === "render") {
  }

  if (validResponse.type === "pyramid") {
  }
}

async function runCassiniInDocker(...args: string[]): Promise<number> {
  return executeCommand(
    "docker",
    "run",
    "--rm",
    "-v",
    `${Deno.cwd()}:/app`,
    "nicorio42/cassini",
    ...args,
  );
}

async function compressDir(
  inputDirectoryPath: string,
  outputDirectoryPath: string,
  fileName: string,
) {
  const archiveFileName = fileName + ".tar.bz2";
  const archivePath = join(outputDirectoryPath, archiveFileName);

  return [
    archivePath,
    await executeCommand("tar", "-cjvf", archivePath, inputDirectoryPath),
  ];
}

async function executeCommand(command: string, ...args: string[]) {
  const cmd = new Deno.Command(command, { args });
  const { code, stdout, stderr } = await cmd.output();

  if (code !== 0) {
    console.log(new TextDecoder().decode(stderr));
  } else {
    console.log(new TextDecoder().decode(stdout));
  }

  return code;
}

async function downloadFileToDisk(url: string, filePath: string) {
  const lifarFileResponse = await fetch(url);

  if (!lifarFileResponse.ok || lifarFileResponse.body === null) {
    throw new Error(`Failed to fetch file: ${url}`);
  }

  using file = await Deno.open(filePath, {
    create: true,
    write: true,
  });

  await lifarFileResponse.body.pipeTo(file.writable);
}

async function createDirIfItDoenstExist(dirPath: string) {
  try {
    await Deno.lstat(dirPath);
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      await Deno.mkdir(dirPath, { recursive: true });
    } else {
      console.error(error);
    }
  }
}
