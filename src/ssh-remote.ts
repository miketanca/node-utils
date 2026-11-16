import { NodeSSH } from "node-ssh";

const ssh = new NodeSSH();

export async function runRemoteCommand(command: string): Promise<string> {
  try {
    // 1. Connect to the SSH server
    await ssh.connect({
      host: "localhost",
      username: "mtan",
      // Use privateKeyPath (recommended) OR password:
      // privateKeyPath: '/path/to/your/private_key',
      password: "Test*123",
    });

    // 2. Run the command and get the response
    const result = await ssh.execCommand(command, { cwd: "." });

    return result.stdout;
  } catch (error) {
    console.error("SSH Connection Error:", error);
    return ""; // Return an empty string in case of error
  } finally {
    // 3. Always close the connection when done
    ssh.dispose();
  }
}
