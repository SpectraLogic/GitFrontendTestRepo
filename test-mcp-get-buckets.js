import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function testGetBuckets() {
  const serverPath = path.join(__dirname, "mcp-server-xml.js");
  
  console.log("Starting MCP server and connecting...");
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
    console.log("Connected to MCP server.");

    console.log("Calling tool: GetBuckets...");
    const result = await client.callTool({
      name: "GetBuckets",
      arguments: {},
    });

    console.log("\n--- TEST RESULT ---");
    if (result.isError) {
      console.error("Tool execution failed:", result.content);
    } else {
      console.log("Response from tool:");
      console.log(result.content[0].text);
      
      if (result.content[0].text.includes("<Bucket>")) {
        console.log("\nSUCCESS: Successfully retrieved buckets from BP via MCP server!");
      } else {
        console.log("\nWARNING: Unexpected response format.");
      }
    }
    
  } catch (error) {
    console.error("Test failed:", error);
  } finally {
    process.exit(0);
  }
}

testGetBuckets();
