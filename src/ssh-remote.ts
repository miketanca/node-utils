import { NodeSSH } from "node-ssh";

const ssh = new NodeSSH();

export async function runRemoteCommand() {
  try {
    // 1. Connect to the SSH server
    await ssh.connect({
      host: "test.rebex.net",
      username: "demo",
      // Use privateKeyPath (recommended) OR password:
      // privateKeyPath: '/path/to/your/private_key',
      password: "password",
    });

    // 2. Run the command and get the response
    const result = await ssh.execCommand("ls -la", { cwd: "." });

    console.log("STDOUT:", result.stdout);
    console.log("STDERR:", result.stderr);
    console.log("EXIT CODE:", result.code);

    await ssh.getFile("/Temp/readme.txt", "readme.txt");

    console.log("File downloaded successfully.");
  } catch (error) {
    console.error("SSH Connection Error:", error);
  } finally {
    // 3. Always close the connection when done
    ssh.dispose();
  }
}
