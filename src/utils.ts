import { exists } from "@std/fs";

type LogLevel = "info" | "warn" | "error";

export function log(
  content: any,
  args: { threadNumber?: number; level: LogLevel },
) {
  const level = args.level;
  const threadNumber = args?.threadNumber?.toString() ?? "main";

  const fun = console[level === "error" ? "error" : level === "warn" ? "warn" : "log"];
  const color = level === "error" ? "red" : level === "warn" ? "orange" : "blue";

  fun(
    `%c[${level.toUpperCase()} ${new Date().toISOString()} Thread(${threadNumber})] ${content}`,
    color !== undefined ? `color: ${color}` : "",
  );
}

export async function isGdalAvailable(): Promise<boolean> {
  try {
    const command = new Deno.Command("gdalinfo", { args: ["--version"] });
    const { success } = await command.output();
    return success;
  } catch {
    return false;
  }
}

export async function isPdalAvailable(): Promise<boolean> {
  try {
    const command = new Deno.Command("pdal", { args: ["--version"] });
    const { success } = await command.output();
    return success;
  } catch {
    return false;
  }
}

export async function isCassiniAvailable(): Promise<boolean> {
  try {
    const command = new Deno.Command("cassini", { args: ["--version"] });
    const { success } = await command.output();
    return success;
  } catch {
    return false;
  }
}

export async function executeCommand(command: string, ...args: string[]) {
  const { success, stderr, stdout } = await new Deno.Command(command, { args }).output();
  if (!success) throw new Error(new TextDecoder().decode(stderr));
  return stdout;
}

export async function compressDirectory(directoryPath: string, archivePath: string) {
  return executeCommand("tar", "-cJf", archivePath, "-C", directoryPath, ".");
}

export async function removeIfExists(path: string, options?: { recursive: boolean }) {
  if (await exists(path)) return Deno.remove(path, options);
}

const DEFAULT_TIMEOUT = 60 * 1000; // seconds
const DEFAULT_NUMBER_OF_RETRY = 5;

export async function fetchWithRetryAndTimeout(
  input: RequestInfo | URL,
  init?: RequestInit & { timeout?: number; numberOfRetry?: number },
): Promise<Response> {
  const timeout = init?.timeout ?? DEFAULT_TIMEOUT;
  const numberOfRetry = init?.numberOfRetry ?? DEFAULT_NUMBER_OF_RETRY;

  for (let attempt = 0; attempt < numberOfRetry; attempt++) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      const response = await fetch(input, { ...init, signal: controller.signal });
      clearTimeout(timeoutId);
      if (!response.ok) throw new Error(`HTTP error! Status: ${response.status}`);
      return response;
    } catch (error) {
      clearTimeout(timeoutId);

      if (attempt === numberOfRetry - 1 || error instanceof Error && error.name === "AbortError") {
        throw error;
      }
    }
  }

  throw new Error("Fetch failed after retries");
}
