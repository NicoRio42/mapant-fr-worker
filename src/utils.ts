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
  const { success, stderr } = await new Deno.Command(command, { args }).output();
  if (!success) throw new Error(new TextDecoder().decode(stderr));
}

export async function compressDirectory(directoryPath: string, archivePath: string) {
  return executeCommand("tar", "-cJf", archivePath, "-C", directoryPath, ".");
}
