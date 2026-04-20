#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from 'url';
import xml2js from "xml2js";

// Get the directory of the current script
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Resolve XML path relative to this script
const XML_PATH = path.join(__dirname, "server/src/main/resources/request-handlers.xml");

const server = new Server(
  {
    name: "spectra-s3-xml-server",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

let tools = [];

async function loadTools() {
  try {
    if (!fs.existsSync(XML_PATH)) {
        // Log to stderr so it shows up in client logs but doesn't break JSON-RPC on stdout
        console.error(`[MCP Error] XML file not found at: ${XML_PATH}`);
        console.error(`[MCP Info] Current script dir: ${__dirname}`);
        return;
    }
    
    const xml = fs.readFileSync(XML_PATH, "utf-8");
    const parser = new xml2js.Parser();
    const result = await parser.parseStringPromise(xml);

    if (!result.Data || !result.Data.RequestHandler) {
      console.error("[MCP Error] Invalid XML structure: Missing Data.RequestHandler");
      return;
    }

    tools = result.Data.RequestHandler.map((handler) => {
      const nameFull = handler.$.Name;
      const nameShort = nameFull.split(".").pop().replace("RequestHandler", "");
      const doc = handler.Documentation ? handler.Documentation[0] : "No description";
      
      let requiredParams = [];
      let optionalParams = [];
      
      if (handler.RequestRequirements) {
        handler.RequestRequirements.forEach(req => {
            const text = typeof req === 'string' ? req : req._ || "";
            
            if (text.includes("Query Parameters Required:")) {
                 const reqMatch = text.match(/Required: \[(.*?)\]/);
                 if (reqMatch && reqMatch[1]) {
                     requiredParams = reqMatch[1].split(", ").filter(s => s);
                 }
                 const optMatch = text.match(/Optional: \[(.*?)\]/);
                 if (optMatch && optMatch[1]) {
                     optionalParams = optMatch[1].split(", ").filter(s => s);
                 }
            }
        });
      }

      const properties = {};
      [...requiredParams, ...optionalParams].forEach(p => {
          properties[p] = { type: "string" };
      });

      return {
        name: nameShort,
        description: doc,
        inputSchema: {
          type: "object",
          properties: properties,
          required: requiredParams,
        },
      };
    });
    
    console.error(`[MCP Info] Successfully loaded ${tools.length} tools from XML.`);
  } catch (error) {
    console.error("[MCP Error] Error loading XML:", error);
  }
}

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: tools,
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const toolName = request.params.name;
  const args = request.params.arguments;

  return {
    content: [
      {
        type: "text",
        text: `Tool ${toolName} called with arguments: ${JSON.stringify(args)}. Execution is not yet implemented for this provider.`,
      },
    ],
  };
});

async function run() {
  await loadTools();
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("[MCP Info] Spectra S3 XML MCP Server running on stdio");
}

run().catch((error) => {
  console.error("[MCP Fatal] Fatal error:", error);
  process.exit(1);
});
