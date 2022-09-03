// swagger-cli bundle -o Orchestration.json Orchestration.yaml -r

const { exec } = require("node:child_process");
const fs = require("fs");

exec(
  "swagger-cli bundle -o ../Orchestration.json ../Orchestration.yaml -r",
  (error, stdout, stderr) => {
    if (error) {
      console.error(`exec error: ${error}`);
      return;
    }
    console.log(`stdout: ${stdout}`);
    var jsonContent = require("../Orchestration.json");
    const jsonSchemas = jsonContent["components"].schemas;
    const dirJsonSchemaGenerated = "../src/models/json-schema";
    if (!fs.existsSync(dirJsonSchemaGenerated)) {
      fs.mkdirSync(dirJsonSchemaGenerated, { recursive: true });
    }
    Object.keys(jsonSchemas).forEach((key) => {
      fs.writeFileSync(
        `../src/models/json-schema/${key}.json`,
        JSON.stringify(jsonSchemas[key])
      );
    });
    exec("rm -f ../Orchestration.json", (error, stdout, stderr) => {
      if (error) {
        console.error(`Remove Orchestration.json ERROR: ${error}`);
        return;
      }
    })
    exec("./oapi_generator ../ Orchestration.yaml ../src/models",(error, stdout, stderr) => {
      if (error) {
        console.error(`rust model generate ERROR: ${error}`);
        return;
      }
      console.log(`rust model generate: ${stdout}`);
    })
  }
);
