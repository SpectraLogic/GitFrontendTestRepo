import fs from "fs";
import path from "path";
import { fileURLToPath } from 'url';
import xml2js from "xml2js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const XML_PATH = path.join(__dirname, "server/src/main/resources/request-handlers.xml");

async function testLoad() {
  console.log(`Testing XML loading from: ${XML_PATH}`);
  
  if (!fs.existsSync(XML_PATH)) {
      console.error("ERROR: File does not exist!");
      return;
  }

  try {
    const xml = fs.readFileSync(XML_PATH, "utf-8");
    const parser = new xml2js.Parser();
    const result = await parser.parseStringPromise(xml);

    if (!result.Data || !result.Data.RequestHandler) {
      console.error("ERROR: Invalid XML structure");
      return;
    }

    const tools = result.Data.RequestHandler.map((handler) => {
        const nameFull = handler.$.Name;
        return nameFull.split(".").pop().replace("RequestHandler", "");
    });

    console.log(`SUCCESS: Found ${tools.length} tools.`);
    console.log("First 5 tools:", tools.slice(0, 5));

  } catch (error) {
    console.error("ERROR parsing XML:", error);
  }
}

testLoad();
