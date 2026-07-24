# OrdersTransformer

Transform delimited (CSV/TSV) data into FOLIO Purchase Orders and Purchase Order Lines (POL) with support for acquisition methods, funds, expense classes, and vendor linking.

## When to Use This Task

- Migrating purchase orders from legacy acquisitions systems
- Creating ongoing orders for subscriptions or standing orders
- Linking orders to existing organizations (vendors) and instances (titles)

## Configuration

```json
{
    "name": "transform_orders",
    "migrationTaskType": "OrdersTransformer",
    "ordersMappingFileName": "orders_mapping.json",
    "organizationsCodeMapFileName": "org_codes.tsv",
    "acquisitionMethodMapFileName": "acquisition_methods.tsv",
    "paymentStatusMapFileName": "payment_status.tsv",
    "receiptStatusMapFileName": "receipt_status.tsv",
    "workflowStatusMapFileName": "workflow_status.tsv",
    "locationMapFileName": "locations.tsv",
    "fundsMapFileName": "funds.tsv",
    "fundsExpenseClassMapFileName": "expense_classes.tsv",
    "files": [
        {
            "file_name": "orders.tsv"
        }
    ]
}
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | The name of this task. |
| `migrationTaskType` | string | Yes | Must be `"OrdersTransformer"` |
| `ordersMappingFileName` | string | Yes | JSON mapping file for order and POL fields |
| `organizationsCodeMapFileName` | string | Yes | TSV file mapping legacy vendor codes to FOLIO organization codes |
| `acquisitionMethodMapFileName` | string | Yes | TSV file mapping acquisition methods |
| `paymentStatusMapFileName` | string | No | TSV file mapping payment statuses |
| `receiptStatusMapFileName` | string | No | TSV file mapping receipt statuses |
| `workflowStatusMapFileName` | string | No | TSV file mapping workflow statuses |
| `locationMapFileName` | string | No | TSV file mapping locations |
| `fundsMapFileName` | string | No | TSV file mapping funds (required if POLs have fund distributions) |
| `fundsExpenseClassMapFileName` | string | No | TSV file mapping expense classes |
| `files` | array | Yes | List of source data files to process |

## Source Data Requirements

- **Location**: Place CSV/TSV files in `iterations/<iteration>/source_data/orders/`
- **Format**: Tab-separated (TSV) or comma-separated (CSV) with header row
- **Prerequisites**:
  - Run [BibsTransformer](bibs_transformer) if linking to instances
  - Run [OrganizationTransformer](organization_transformer) and post organizations if linking to vendors

### Order Mapping File

Orders in FOLIO consist of a Purchase Order (header) with embedded Purchase Order Lines. The mapping file must include fields for both:

```json
{
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "ORDER_ID",
            "description": "Legacy order identifier"
        },
        {
            "folio_field": "poNumber",
            "legacy_field": "PO_NUMBER",
            "description": "Purchase order number"
        },
        {
            "folio_field": "vendor",
            "legacy_field": "VENDOR_CODE",
            "description": "Mapped to organization UUID via organizationsCodeMapFileName"
        },
        {
            "folio_field": "orderType",
            "legacy_field": "ORDER_TYPE",
            "description": "One-Time or Ongoing"
        },
        {
            "folio_field": "workflowStatus",
            "legacy_field": "STATUS",
            "description": "Open, Pending, Closed"
        },
        {
            "folio_field": "compositePoLines[0].titleOrPackage",
            "legacy_field": "TITLE"
        },
        {
            "folio_field": "compositePoLines[0].instanceId",
            "legacy_field": "BIB_ID",
            "description": "Linked to instance via instance_id_map"
        },
        {
            "folio_field": "compositePoLines[0].acquisitionMethod",
            "legacy_field": "ACQ_METHOD",
            "description": "Mapped via acquisitionMethodMapFileName"
        },
        {
            "folio_field": "compositePoLines[0].orderFormat",
            "legacy_field": "FORMAT",
            "description": "Physical Resource, Electronic Resource, etc."
        },
        {
            "folio_field": "compositePoLines[0].cost.listUnitPrice",
            "legacy_field": "PRICE"
        },
        {
            "folio_field": "compositePoLines[0].cost.currency",
            "legacy_field": "",
            "value": "USD"
        },
        {
            "folio_field": "compositePoLines[0].fundDistribution[0].fundId",
            "legacy_field": "FUND_CODE",
            "description": "Mapped via fundsMapFileName"
        },
        {
            "folio_field": "compositePoLines[0].fundDistribution[0].distributionType",
            "legacy_field": "",
            "value": "percentage"
        },
        {
            "folio_field": "compositePoLines[0].fundDistribution[0].value",
            "legacy_field": "",
            "value": 100
        }
    ]
}
```

## billTo and shipTo Address Mapping

Orders support name-based mapping for `billTo` and `shipTo`.

- In your orders field mapping file, map `billTo` and `shipTo` to legacy source fields that contain address names.
- During transformation, the mapper resolves those names against tenant address settings from `/configurations/entries` where `module=="TENANT"` and `configName=="tenant.addresses"`.
- The resulting FOLIO value written to `billTo`/`shipTo` is the address UUID from the configuration entry `id`.

Example mapping entries:

```json
{
    "folio_field": "billTo",
    "legacy_field": "BILL_TO_NAME",
    "value": ""
},
{
    "folio_field": "shipTo",
    "legacy_field": "SHIP_TO_NAME",
    "value": ""
}
```

Expected tenant configuration shape:

```json
{
    "id": "e8532c88-ab56-461a-8ba8-ed18bf5e5c22",
    "module": "TENANT",
    "configName": "tenant.addresses",
    "value": "{\"name\":\"Acquisitions\",\"address\":\"...\"}"
}
```

In this example, if source data contains `Acquisitions`, the mapped UUID is `e8532c88-ab56-461a-8ba8-ed18bf5e5c22`.

```{important}
Address name matching is case-insensitive and trims surrounding whitespace. If a name cannot be resolved, the record fails with a data issue.
```

### Validation and Error Reporting for billTo/shipTo

Both `value` and `fallback_value` are supported for name-based address mapping.

- If `value` or `fallback_value` contains a UUID, it is accepted as-is.
- If `value` or `fallback_value` contains a name, it is resolved against `tenant.addresses`.

Validation happens at two stages:

- **Startup validation (process-level):** hardcoded `value` and `fallback_value` entries for `billTo`/`shipTo` are validated when the mapper initializes. If any configured value is invalid, the task reports a transformation process error and halts.
- **Per-record validation (record-level):** names coming from source data (for example from `legacy_field`) are validated during record transformation. If a name cannot be resolved, that record is marked as failed and logged as a data issue.

The migration report includes record-level failures, and processing continues unless failure thresholds are exceeded by task configuration.

### Reference Data Mapping Files

Reference data mapping files connect values from your legacy data to FOLIO reference data. See [Reference Data Mapping](../reference_data_mapping) for detailed documentation on how these files work.

| Mapping File | FOLIO Column | Maps To |
|--------------|--------------|---------|
| `organizationsCodeMapFileName` | `folio_code` | Organization code |
| `acquisitionMethodMapFileName` | `folio_value` | Acquisition method value |
| `fundsMapFileName` | `folio_code` | Fund code |
| `locationMapFileName` | `folio_code` | Location code |

## Order Merging

The transformer intelligently merges rows into composite orders:
- Rows with the **same order ID** are merged into a single Purchase Order
- Each row creates a separate Purchase Order Line within the order
- Differences between rows (other than POL data) are logged for review

## Output Files

Files are created in `iterations/<iteration>/results/`:

| File | Description |
|------|-------------|
| `folio_orders_<task_name>.json` | FOLIO Composite Order records (PO + embedded POLs) |
| `orders_id_map_<task_name>.json` | Legacy ID to FOLIO UUID mapping |

## Examples

### Basic Order Transformation

```json
{
    "name": "transform_orders",
    "migrationTaskType": "OrdersTransformer",
    "ordersMappingFileName": "orders_mapping.json",
    "organizationsCodeMapFileName": "org_codes.tsv",
    "acquisitionMethodMapFileName": "acquisition_methods.tsv",
    "files": [
        {
            "file_name": "orders.tsv"
        }
    ]
}
```

### With Fund Distribution

```json
{
    "name": "transform_orders",
    "migrationTaskType": "OrdersTransformer",
    "ordersMappingFileName": "orders_mapping.json",
    "organizationsCodeMapFileName": "org_codes.tsv",
    "acquisitionMethodMapFileName": "acquisition_methods.tsv",
    "fundsMapFileName": "funds.tsv",
    "fundsExpenseClassMapFileName": "expense_classes.tsv",
    "files": [
        {
            "file_name": "orders.tsv"
        }
    ]
}
```

### With All Status Mappings

```json
{
    "name": "transform_orders",
    "migrationTaskType": "OrdersTransformer",
    "ordersMappingFileName": "orders_mapping.json",
    "organizationsCodeMapFileName": "org_codes.tsv",
    "acquisitionMethodMapFileName": "acquisition_methods.tsv",
    "paymentStatusMapFileName": "payment_status.tsv",
    "receiptStatusMapFileName": "receipt_status.tsv",
    "workflowStatusMapFileName": "workflow_status.tsv",
    "locationMapFileName": "locations.tsv",
    "fundsMapFileName": "funds.tsv",
    "files": [
        {
            "file_name": "orders.tsv"
        }
    ]
}
```

## Running the Task

```shell
folio-migration-tools mapping_files/config.json transform_orders --base_folder ./
```

## Posting Orders

After transformation, post orders using BatchPoster:

```json
{
    "name": "post_orders",
    "migrationTaskType": "BatchPoster",
    "objectType": "CompositeOrders",
    "batchSize": 1,
    "files": [
        {
            "file_name": "folio_orders_transform_orders.json"
        }
    ]
}
```

```{note}
Orders are posted one at a time (`batchSize: 1`) because FOLIO's orders API processes each composite order individually.
```

## See Also

- [Mapping File Based Mapping](../mapping_file_based_mapping) - Mapping file syntax
- [OrganizationTransformer](organization_transformer) - Creating vendor organizations
- [BatchPoster](batch_poster) - Posting orders
