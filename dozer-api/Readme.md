## Everytime we update our api definition - generate new model

1. Update YAML file definition: located in `Orchestration.yaml`
2.
2. run this command

```node
cd generator
node .
```

3. Result will be:
   1. A folder `src/models` is generated
   2. All `JsonSchema` according to YAML is generated in `src/models/json-schema/`
   3. All Rust models file under `src/models`
