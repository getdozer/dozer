// swagger-cli bundle -o api.json api.yaml -r

const { exec } = require("node:child_process");
const fs = require("fs");

exec(
  "swagger-cli bundle -o ../api.json ../api.yaml -r && swagger-cli bundle -o ../api-raw.json ../api.yaml",
  (error, stdout, stderr) => {
    if (error) {
      console.error(`exec error: ${error}`);
      return;
    }
    console.log(`stdout: ${stdout}`);
    var jsonContent = require("../api.json");
    const jsonSchemas = jsonContent["components"].schemas;
    const dirJsonSchemaGenerated = "../src/models/json-schema";
    if (!fs.existsSync(dirJsonSchemaGenerated)) {
      fs.mkdirSync(dirJsonSchemaGenerated, { recursive: true });
    }
    Object.keys(jsonSchemas).forEach((key) => {
      delete jsonSchemas[key]['x-examples']
      fs.writeFileSync(
        `../src/models/json-schema/${key}.json`,
        JSON.stringify(jsonSchemas[key])
      );
    });

    const jsonRaw = require("../api-raw.json");
    const jsonRawSchemas = { ...jsonRaw["components"].schemas }
    const jsonDerefSchemas = { ...jsonContent["components"].schemas }
    Object.keys(jsonDerefSchemas).forEach(key => {
      if (jsonDerefSchemas[key].allOf) {
        const allOf = jsonDerefSchemas[key].allOf
        const allProperties = allOf.flatMap(e => e.properties).reduce((prev, curr) => {
          return { ...prev, ...curr }
        }, {})

        Object.keys(allProperties).forEach(propsKey => {
          if (allProperties[propsKey].title) {
            allProperties[propsKey] = {
              "$ref": `#/components/schemas/${allProperties[propsKey].title}`
            }
          }
        })
        const allRequired = allOf.flatMap(e => e.required)
        jsonRawSchemas[key] = {
          "type": "object",
          "title": key,
          properties: allProperties,
          required: allRequired
        }
      }
    })
    jsonRaw["components"].schemas = jsonRawSchemas
    fs.writeFileSync(
      `../api-raw.json`,
      JSON.stringify(jsonRaw)
    );


    exec("./oapi_generator ../ api-raw.json ../src/models", (error, stdout, stderr) => {
      if (error) {
        console.error(`rust model generate ERROR: ${error}`);
        return;
      }
      console.log(`rust model generate: ${stdout}`);
      exec("rm -f ../api-raw.json", (error, stdout, stderr) => {
        if (error) {
          console.error(`Remove api-raw.json ERROR: ${error}`);
          return;
        }
      })
    })

    exec("rm -f ../api.json", (error, stdout, stderr) => {
      if (error) {
        console.error(`Remove api.json ERROR: ${error}`);
        return;
      }
    })

  }
);
