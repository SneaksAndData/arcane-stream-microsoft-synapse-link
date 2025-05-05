# Backfilling and schema evolution data with the Synapse Link Arcane plugin

## Schema evolution

Data produced by the Synapse Link in `Incremental CSV` mode generates files prefixes based on timestamp of the batch start. Schema change handling in this case is similar to other Arcane plugins, but has a few specifics, outlined below.


1. In streaming mode, each batch folder will contain a `model.json` file holding the schema for all entities in the batch. Plugin will use this, for example `2025-01-01T00.00.00Z/model.json` to determine the schema of the change set for each entitiy.
 
2. The extracted schema is used to parse CSV file for each entity.
 
3. Plugin groups rows by their schema and writes each group to a separate staging table.
 
4. Before merging from the staging table to the final table, the plugin checks the schema of the target table and compares it
   with the schema of the staging table. Changes are resolved the following way:
     - If present in staging, but missing in target (regardless if actually missing or renamed) - ADD
     - If missing in staging, but present in target - IGNORE (Iceberg will auto-resolve this to inserting a null value)
 
5. Once the schema has been updated, the plugin merges the data from the staging table to the target table.
 
6. Archiving process follows steps 3-5.


## Backfilling

Since the `MicrosoftSynapseLink` Arcane plugin automatically evolves schema of the target table, it **does not need
to initiate the backfilling process on the schema change event**.

Nevertheless, the plugin can be configured to backfill the data from the very first folder in the storage container.
It can be helpful if a stream runner was offline for a long time, if some of the CSV files were missed due to code bugs,
or if the a new table was added.

The job templates used for backfilling process should have more resources than a regular job template and use public IPv4 address for the host, since Azure Storage does not support IPv6 at the time of writing.

> [!IMPORTANT]  
> The backfill reads the data from beginning of the storage account, not the source table in D365.
> If you need to **replace** target table, make sure that the export of the source table was reinitialized **BEFORE** starting
> the backfill process.

### Backfill behavior

The backfill can work in two modes:

- Create a temporary target table and then replace the data in the live one with a SQL `CREATE OR REPLACE` statement.
- Create a new temporary target table and update the data in the live one with a SQL `MERGE` statement.

The mode is defined by the `.spec.backfillBehavior` parameter in the stream definition that can be set to `ovewrite` or `merge`.

> [!IMPORTANT]  
> The backfill reads the data from beginning of the storage account, not the source table in D365.
> If you need to **replace** target table, make sure that the export of the source table was reinitialized **BEFORE** starting
> the backfill process.

## Arcane time intervals

The Arcane stream definition has a set of parameters that define the time intervals for the streaming and backfilling
processes.

1. If the stream is running in backfill mode, it always reads the folders starting from the date defined in
   `.spec.backfillStartDate` until the end of the container.
 
2. When a job is started in streaming mode, in reads the content of the `Changelog/changelog.info` file and subtracts the
   `.spec.lookBackIntervalSeconds` from the timestamp of the last change. Then it reads the data starting from this
   timestamp until the date in the `Changelog/changelog.info` file.
   > [!WARNING]  
   > If the stream runner was offline more then the `.spec.lookBackIntervalSeconds`, the data will be lost.
   > 
   > This can be fixed with the backfill process with the `.spec.backfillBehavior` set to `merge`.
 
3. Every `.spec.changeCaputureIntervalSeconds` the stream reads the data starting from the previous iteration 
   until the timestamp recorded in the `Changelog/changelog.info` file.

4. The most recent folder is **always skipped** to avoid reading the incomplete data.
