### Reset Database with diesel

1. Run this command

   ```bash
   rm -i dozer-admin/src/db/dozer.db && rm -i dozer-admin/src/db/schema.rs && diesel setup && diesel migration run
   ```
### update bin from dozer orchestrator

```bash
cargo build -p dozer-orchestrator && cp target/debug/dozer dozer-admin/dozer-bin
```