import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function testGetBuckets() {
  // Path to the MCP server relative to this test file
  const serverPath = path.join(__dirname, "../../mcp-server-xml.js");
  
  const transport = new StdioClientTransport({
    command: "node",
    args: [serverPath],
  });

  const client = new Client(
    { name: "test-client", version: "1.0.0" },
    { capabilities: {} }
  );

  try {
    await client.connect(transport);
    const result = await client.callTool({
      name: "GetBuckets",
      arguments: {},
    });

    if (result.isError) {
      console.error("Tool execution failed:", result.content);
      process.exit(1);
    } else {
      console.log("Response from tool:");
      console.log(result.content[0].text);
      
      if (result.content[0].text.includes("<Bucket>")) {
        console.log("\nSUCCESS: Successfully retrieved buckets from BP via MCP server!");
      } else {
        console.log("\nWARNING: Unexpected response format.");
        process.exit(1);
      }
    }
  } catch (error) {
    console.error("Test failed:", error);
    process.exit(1);
  } finally {
    process.exit(0);
  }
}

testGetBuckets();
