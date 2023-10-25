# Postgres requirements

### Version
At least **v10**.  To verify it, you can run
```sql
SHOW server_version;
```

### WAL Level
**Logical**. It can be verified with 
```sql
SHOW wal_level;
```

You can change wal_level with this query. After changing you must restart your server.
```sql
ALTER SYSTEM SET wal_level = logical;
```

In AWS RDS you need to use custom parameters group. https://aws.amazon.com/premiumsupport/knowledge-center/rds-postgresql-use-logical-replication/
> To turn on logical replication in RDS for PostgreSQL, modify a custom parameter group to set rds.logical_replication to 1 and attach rds.logical_replication to the DB instance. Update the parameter group to set rds.logical_replication to 1 if a custom parameter group is attached to a DB instance. The rds.logical_replication parameter is a static parameter that requires a DB instance reboot to take effect. When the DB instance reboots, the wal_level parameter is set to logical.
>
> -- <cite>[AWS tutorial][1]<cite>
 
### Replication slots
Database should have at least one available replication slot.

```sql
-- Fetch max replication slots
SHOW max_replication_slots;

--- Get current used replication slots count
SELECT COUNT(*) FROM pg_replication_slots;

-- If there is no empty slot, you can drop any unused slot with
SELECT * FROM pg_drop_replication_slot('slot_name');
```

### User
To use replication postgres database user should have replication permission - `userepl`.<br/>
Permission can be checked with this query
```sql
SELECT usename, userepl FROM pg_user WHERE usename = "current_user"()
```
If it is not enabled, you can grant permission with this query
```sql
ALTER USER <user-name> WITH REPLICATION;
```

[1]: https://aws.amazon.com/premiumsupport/knowledge-center/rds-postgresql-use-logical-replication/