# Configuration files

Every run of FOLIO Migration Tools is driven by a single JSON configuration file. It tells the tools which FOLIO tenant to talk to, where your files live, and what work to do. You pass it on the command line together with the name of the task you want to run:

```shell
uv run folio-migration-tools mapping_files/exampleConfiguration.json --base_folder_path ./ transform_bibs
```

In a repository created from [migration_repo_template](https://github.com/FOLIO-FSE/migration_repo_template), configuration files live in `mapping_files/`.

## Anatomy of a configuration file

A configuration file has two top-level objects:

* **`libraryInformation`** — tenant-level settings that apply to every task: connection details, the iteration to work in, error thresholds, logging.
* **`migrationTasks`** — an array of task definitions. Each entry is one runnable task, identified by its `name`.

Keys are written in **camelCase**. Internally the tools convert them to snake_case, so `gatewayUrl` and `gateway_url` both work, but camelCase is the convention used throughout the docs and the template repo.

```json
{
    "libraryInformation": {
        "gatewayUrl": "https://folio-etesting-snapshot-kong.ci.folio.org",
        "tenantId": "diku",
        "folioUsername": "diku_admin",
        "libraryName": "Example University Library",
        "folioRelease": "ramsons",
        "iterationIdentifier": "test_iteration",
        "multiFieldDelimiter": "<delimiter>",
        "logLevelDebug": false
    },
    "migrationTasks": [
        {
            "name": "transform_bibs",
            "migrationTaskType": "BibsTransformer",
            "ilsFlavour": "sierra",
            "files": [
                { "fileName": "bibs.mrc" }
            ]
        },
        {
            "name": "post_instances",
            "migrationTaskType": "InventoryBatchPoster",
            "objectType": "Instances",
            "batchSize": 250,
            "files": [
                { "fileName": "folio_instances_transform_bibs.json" }
            ]
        }
    ]
}
```

Both tasks above are defined in the same file. The task name you pass on the command line (`transform_bibs`) selects which one runs.

## `libraryInformation`

| Key | Required | Default | Description |
|---|---|---|---|
| `gatewayUrl` | yes | — | URL of the FOLIO API gateway. Found under Settings → Software versions → API gateway services. |
| `tenantId` | yes | — | The FOLIO tenant ID. In an ECS environment, this is always the **central** tenant. |
| `folioUsername` | yes | — | Username of the FOLIO account performing the migration. Needs full admin permissions/roles. |
| `folioPassword` | yes | — | Password for that account. Prefer supplying it at runtime — see [Credentials](#credentials). |
| `libraryName` | yes | — | Name of the library being migrated. Used in reports. |
| `folioRelease` | yes | — | Target FOLIO release: `ramsons`, `sunflower`, `trillium`, or `umbrellaleaf`. |
| `iterationIdentifier` | yes | — | The folder under `base_folder/iterations/` this run reads from and writes to. |
| `ecsTenantId` | no | `""` | In ECS environments, the data (member) tenant this configuration targets. Setting it implies `isEcs: true`. |
| `ecsCentralIterationIdentifier` | no | `""` | The central tenant's `iterationIdentifier` matching this one, used to reach the central `instances_id_map`. Required when `ecsTenantId` is set. |
| `isEcs` | no | `false` | Marks the tenant as running ECS FOLIO. Forced to `true` when `ecsTenantId` is set. |
| `multiFieldDelimiter` | no | `"<delimiter>"` | Separator for multiple values inside one delimited-text field. |
| `logLevelDebug` | no | `false` | Enables DEBUG logging. Also raises all error thresholds so a debugging run does not shut itself down. See [Logging](logging.md). |
| `failedRecordsThreshold` | no | `5000` | Number of failed records before the run aborts. |
| `failedPercentageThreshold` | no | `20` | Percentage of failed records before the run aborts. |
| `genericExceptionThreshold` | no | `50` | Number of unexpected exceptions before the run aborts. |
| `addTimeStampToFileNames` | no | `false` | Adds a timestamp to output file names. |
| `useGatewayUrlForUuids` | no | `false` | Generate deterministic UUIDs from the gateway URL instead of the tenant ID. Only change this if you know you need it — it changes every UUID the tools produce. |

`baseFolder` is also part of the library configuration, but you do not set it in the file: it comes from `--base_folder_path` on the command line.

```{note}
The legacy key names `okapiUrl`, `okapiUsername`, and `okapiPassword` are still accepted as fallbacks for `gatewayUrl`, `folioUsername`, and `folioPassword`.
```

## `migrationTasks`

Every task entry shares three keys:

| Key | Required | Description |
|---|---|---|
| `name` | yes | The name you pass on the command line. Must be unique within the file. |
| `migrationTaskType` | yes | The class of task to run, e.g. `BibsTransformer`, `InventoryBatchPoster`. See [Migration Tasks](tasks/index.md) for the full list. |
| `ecsTenantId` | no | Overrides the library-level ECS tenant for this task only. |

Everything else depends on the task type — mapping file names, batch sizes, HRID handling, and so on. Each task has its own page under [Migration Tasks](tasks/index.md) documenting its options.

Most tasks also take a `files` array telling them which source files to process. Files are looked up relative to the iteration's `source_data` subfolder for the object type being migrated:

| Key | Default | Description |
|---|---|---|
| `fileName` | `""` | Name of the file to process. |
| `discoverySuppressed` | `false` | Suppress the resulting records from discovery. |
| `staffSuppressed` | `false` | Suppress the resulting records from staff view. |
| `statisticalCode` | `""` | Statistical code (by code, not UUID) to apply to inventory records from this file. Use `multiFieldDelimiter` for multiple codes. |
| `servicePointId` | `""` | Service point for transactions created from this file (loans only). |
| `createSourceRecords` | `true` | MARC transformations only: whether to create SRS source records. |
| `dataImportMarc` | `true` | MARC transformations only: whether successfully processed records are written to the MARC file for Data Import. |

## How the configuration relates to the folder structure

`--base_folder_path` plus `iterationIdentifier` determine where everything is read and written:

```
<base_folder_path>/
├── mapping_files/                  # configuration files and mapping files
└── iterations/
    └── <iterationIdentifier>/
        ├── source_data/            # your legacy data, in per-object-type subfolders
        │   ├── instances/
        │   ├── holdings/
        │   ├── items/
        │   └── users/
        ├── results/                # transformed records, id maps, failed records
        └── reports/                # logs, migration reports, data issue files
```

Missing folders under the iteration are created for you; `--base_folder_path` itself must already exist.

## Credentials

Keep passwords out of your migration repository. Rather than putting `folioPassword` in the configuration file, supply it at runtime:

```shell
uv run folio-migration-tools mapping_files/exampleConfiguration.json --base_folder_path ./ transform_bibs --folio_password <password>
```

If you omit `--folio_password` you will be prompted for it, unless it is set in the environment. Any of the CLI arguments can be supplied as an environment variable instead:

| Environment variable | Equivalent |
|---|---|
| `FOLIO_MIGRATION_TOOLS_CONFIGURATION_PATH` | the configuration file argument |
| `FOLIO_MIGRATION_TOOLS_TASK_NAME` | the task name argument |
| `FOLIO_MIGRATION_TOOLS_FOLIO_PASSWORD` (or `..._OKAPI_PASSWORD`) | `--folio_password` |
| `FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH` | `--base_folder_path` |
| `FOLIO_MIGRATION_TOOLS_REPORT_LANGUAGE` | `--report_language` |

A password given in the configuration file takes precedence: the command-line value is only applied when neither `folioPassword` nor `okapiPassword` is present in `libraryInformation`.

## Validation

Configuration files are validated before any work starts:

* Invalid JSON stops the run with the offending document printed out.
* Missing or malformed settings produce a list of validation messages with camelCased paths to the offending keys, so `libraryInformation, folioRelease` points you straight at the problem.
* An unknown task name, or a `migrationTaskType` that does not exist, prints the list of valid options.
* **Unrecognized keys are ignored rather than rejected.** A misspelled setting will not raise an error; it will silently do nothing.

## Inheriting from other configuration files

A configuration file can inherit from one or more other configuration files. This lets you keep a single, shared definition of your `libraryInformation` and `migrationTasks` and layer small files on top of it — one per tenant, per iteration, or per variation of a task.

Inheritance is triggered by a single top-level key: `source`.

```json
{
    "source": "baseConfiguration.json",
    "libraryInformation": {
        "iterationIdentifier": "test_iteration_2"
    }
}
```

* `source` may be a **string** (one parent file) or an **array of strings** (several parent files).
* Paths are resolved **relative to the file that declares them**, not to your working directory. Absolute paths also work.
* Parents may themselves declare a `source`, so inheritance can be chained to any depth.
* Only the **top-level** `source` key is honored. A `source` key inside `libraryInformation` or inside an individual task is treated as ordinary data and ignored.

You always pass the *child* (most specific) file on the command line; the tools resolve the whole chain before validating anything.

### Precedence

Files are merged from most general to most specific:

1. Each file listed in `source` is loaded, recursively, **in the order it appears**. Later entries win over earlier ones.
2. The contents of the file that declares `source` are merged **on top** of all its parents.

So with:

```json
{
    "source": ["defaults.json", "tenantDefaults.json"],
    "libraryInformation": { "logLevelDebug": true }
}
```

the effective precedence is `defaults.json` → `tenantDefaults.json` → this file, with this file winning any conflict.

### Merge rules

The merge is a deep merge, not a whole-key replacement:

| Value in the child file | Behavior |
|---|---|
| Object (parent value is also an object) | Merged key by key, recursively |
| Array (parent value is also an array) | Merged item by item — see below |
| String, number, boolean | Replaces the parent value |
| `null` | The key is **deleted** from the merged result |

Array items that are objects are matched against the parent's items on the first key found among **`name`**, **`fileName`**, or **`file_name`**, in that order:

* If a parent item has the same value for that key, the two items are deep-merged.
* If there is no match — or the item has none of those keys, or is not an object — the item is **appended**.

That is what makes task-level inheritance useful in practice: `migrationTasks` entries are matched on `name` and `files` entries on `fileName`, so you can override a single field of a single task without restating the task.

### Example: one base configuration, several iterations

`mapping_files/baseConfiguration.json` holds everything that does not change — the `libraryInformation` and `migrationTasks` from the [example above](#anatomy-of-a-configuration-file).

`mapping_files/iteration2.json` changes only the iteration and turns on debug logging:

```json
{
    "source": "baseConfiguration.json",
    "libraryInformation": {
        "iterationIdentifier": "test_iteration_2",
        "logLevelDebug": true
    }
}
```

Everything else — the gateway URL, both tasks, their files — is inherited unchanged.

### Example: overriding one field of one task

Because `migrationTasks` items are matched on `name`, you can reach into a single task:

```json
{
    "source": "baseConfiguration.json",
    "migrationTasks": [
        {
            "name": "post_instances",
            "batchSize": 50
        }
    ]
}
```

The merged `post_instances` task keeps its `migrationTaskType`, `objectType`, and `files` from the base file, with only `batchSize` changed. `transform_bibs` is untouched.

```{note}
`name` is required in an overriding task entry — it is the key the merge matches on. An entry without a `name` cannot be matched to a parent task, so it is appended as a new (and almost certainly invalid) task.
```

### Example: adding a task, and adding a file to an existing task

Unmatched items are appended, so the same mechanism adds new content:

```json
{
    "source": "baseConfiguration.json",
    "migrationTasks": [
        {
            "name": "transform_bibs",
            "files": [
                { "fileName": "bibs_second_batch.mrc", "discoverySuppressed": true }
            ]
        },
        {
            "name": "transform_items",
            "migrationTaskType": "ItemsTransformer",
            "itemsMappingFileName": "item_mapping.json",
            "locationMapFileName": "locations.tsv",
            "files": [
                { "fileName": "items.tsv" }
            ]
        }
    ]
}
```

`transform_bibs` ends up with both `bibs.mrc` and `bibs_second_batch.mrc`, and `transform_items` is added as a third task.

### Example: separating tenant settings from task definitions

A common pattern is one file per target tenant, one file per workflow, and a thin child file combining them:

```
mapping_files/
├── tenants/
│   ├── snapshot.json          # gatewayUrl, tenantId, folioUsername, folioRelease
│   └── bugfest.json
├── workflows/
│   └── inventory.json         # the migrationTasks for the inventory workflow
├── snapshotInventory.json
└── bugfestInventory.json
```

`mapping_files/snapshotInventory.json`:

```json
{
    "source": ["tenants/snapshot.json", "workflows/inventory.json"]
}
```

Its `source` paths are relative to `mapping_files/`, while any `source` inside `tenants/snapshot.json` would be relative to `mapping_files/tenants/`.

### Example: removing an inherited setting

Setting a key to `null` deletes it, which is how you fall back to a task's own default:

```json
{
    "source": "baseConfiguration.json",
    "migrationTasks": [
        {
            "name": "post_instances",
            "batchSize": null
        }
    ]
}
```

The merged `post_instances` task has no `batchSize` key at all, so `InventoryBatchPoster`'s default of 100 applies instead of the base file's 250.

```{warning}
`null` always means *delete*. There is no way to inherit a key and give it a literal JSON `null` value.
```

### Things to watch out for

**You cannot replace an array in a single file.** Arrays are always merged, never overwritten, and JSON does not let you set the same key twice. Clearing and redefining takes two levels of inheritance:

```json
// step1.json — drop the inherited files array
{
    "source": "baseConfiguration.json",
    "migrationTasks": [{ "name": "transform_bibs", "files": null }]
}
```

```json
// step2.json — define a fresh one
{
    "source": "step1.json",
    "migrationTasks": [{ "name": "transform_bibs", "files": [{ "fileName": "only_this.mrc" }] }]
}
```

**Arrays of plain values accumulate.** Items that are not objects are appended with no duplicate check, so a parent's `["a", "b"]` plus a child's `["b"]` becomes `["a", "b", "b"]`.

**Diamond inheritance can duplicate array items.** If two parents both inherit from the same grandparent, that grandparent is loaded twice. Object items keyed on `name`/`fileName` still merge cleanly, so `migrationTasks` and `files` are safe — but arrays of plain values will contain their items twice. Prefer a linear chain where you can.

**Keep array items consistent.** Matching reads the merge key directly from each parent item. If a child item has a `name` but a parent item in the same array does not — or the parent array mixes objects with plain values — the load fails with a `KeyError` or `TypeError` instead of a helpful validation message.

**Circular `source` references are not detected.** Two files that inherit from each other produce a `RecursionError`.

**The `source` key survives into the merged configuration.** This is harmless — only `libraryInformation` and `migrationTasks` are read — but it will show up if you dump the resolved configuration.

### Checking the merged result

Inheritance is resolved by `folio_migration_tools.config_file_load.merge_load`, so you can print exactly what the tools will see before running a task:

```shell
uv run python -c "import json; from folio_migration_tools.config_file_load import merge_load; print(json.dumps(merge_load('mapping_files/iteration2.json'), indent=4))"
```

This is the fastest way to confirm that precedence and array merging came out the way you intended.
