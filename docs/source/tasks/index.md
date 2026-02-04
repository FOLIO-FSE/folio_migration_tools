# Migration Tasks

The folio_migration_tools are built around the concept of migration tasks that either **transform**, **post** (load), or **migrate** data from a legacy system into FOLIO.

## Task Categories

### Transform Tasks
Transform tasks read data from source files and convert them into FOLIO-compatible JSON objects:

| Task | Description | Source Data |
|------|-------------|-------------|
| [BibsTransformer](bibs_transformer) | Transform MARC bibliographic records to FOLIO Instances | MARC21 (.mrc) |
| [HoldingsMarcTransformer](holdings_marc_transformer) | Transform MFHD records to FOLIO Holdings | MARC21 (.mrc) |
| [HoldingsCsvTransformer](holdings_csv_transformer) | Transform delimited data to FOLIO Holdings | CSV/TSV |
| [ItemsTransformer](items_transformer) | Transform delimited data to FOLIO Items | CSV/TSV |
| [UserTransformer](user_transformer) | Transform delimited data to FOLIO Users | CSV/TSV |
| [OrganizationTransformer](organization_transformer) | Transform delimited data to FOLIO Organizations | CSV/TSV |
| [OrdersTransformer](orders_transformer) | Transform delimited data to FOLIO Orders | CSV/TSV |
| [ManualFeeFinesTransformer](manual_fee_fines_transformer) | Transform delimited data to FOLIO Manual Fees/Fines | CSV/TSV |

### Post Tasks
Post tasks load transformed data into FOLIO via APIs:

| Task | Description | Use Case |
|------|-------------|----------|
| [BatchPoster](batch_poster) | Post FOLIO objects via batch APIs | General-purpose posting |
| [InventoryBatchPoster](inventory_batch_poster) | Enhanced posting for inventory objects | Upsert, field preservation |
| [MARCImportTask](marc_import) | Import MARC records via Data Import APIs | Loading MARC to SRS |
| [UserImportTask](user_import) | Import users with enhanced upsert support | User loading with field protection |

### Migrate Tasks
Migrate tasks handle transactional data that requires real-time API interactions:

| Task | Description | Source Data |
|------|-------------|-------------|
| [CoursesMigrator](courses_migrator) | Migrate course reserve courses | JSON |
| [ReservesMigrator](reserves_migrator) | Migrate course reserve items | CSV/TSV |
| [LoansMigrator](loans_migrator) | Migrate open loans via circulation APIs | CSV/TSV |
| [RequestsMigrator](requests_migrator) | Migrate open requests via circulation APIs | CSV/TSV |

## How Tasks Work Together

A typical migration workflow involves running tasks in sequence:

```
1. BibsTransformer          →  Creates Instances + MARC file for Data Import
2. HoldingsMarcTransformer  →  Creates Holdings (linked to Instances)
   or HoldingsCsvTransformer
3. ItemsTransformer         →  Creates Items (linked to Holdings)
4. UserTransformer          →  Creates Users
5. InventoryBatchPoster     →  Posts Instances, Holdings, Items to FOLIO
   or BatchPoster
6. MARCImportTask           →  Loads MARC records to SRS via Data Import
7. UserImportTask           →  Posts Users to FOLIO
   or BatchPoster
8. LoansMigrator            →  Creates open loans
9. RequestsMigrator         →  Creates open requests
```

## Running a Task

Tasks are run using the `folio-migration-tools` command:

```shell
folio-migration-tools <configuration_file> <task_name> --base_folder <path>
```

Example:
```shell
folio-migration-tools mapping_files/config.json transform_bibs --base_folder ./
```

The `<task_name>` must match the `"name"` property in the task configuration within your configuration file.

## Task Configuration

Each task is configured in the main configuration JSON file. A configuration file contains a `libraryConfiguration` object and a `tasks` array:

```json
{
    "libraryConfiguration": {
        "gateway_url": "https://folio-snapshot-okapi.dev.folio.org",
        "tenant_id": "diku",
        "folio_username": "diku_admin",
        "folio_password": "admin",
        "base_folder": ".",
        "library_name": "My Library",
        "folio_release": "sunflower",
        "iteration_identifier": "test_iteration"
    },
    "tasks": [
        {
            "name": "transform_bibs",
            "migrationTaskType": "BibsTransformer"
        },
        {
            "name": "post_instances",
            "migrationTaskType": "InventoryBatchPoster"
        }
    ]
}
```

Note: Task configurations have additional properties beyond `name` and `migrationTaskType`. See individual task documentation for complete configuration options.

```{toctree}
:maxdepth: 1
:caption: Transform Tasks

bibs_transformer
holdings_marc_transformer
holdings_csv_transformer
items_transformer
user_transformer
organization_transformer
orders_transformer
manual_fee_fines_transformer
```

```{toctree}
:maxdepth: 1
:caption: Post Tasks

batch_poster
inventory_batch_poster
marc_import
user_import
```

```{toctree}
:maxdepth: 1
:caption: Migrate Tasks

courses_migrator
reserves_migrator
loans_migrator
requests_migrator
```
