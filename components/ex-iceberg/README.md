Iceberg Extractor
=============

Component for extracting data from Iceberg tables.
Supports full, incremental, and append loading modes.
Offers three selection options: all data, selected columns, or a custom query.
Currently, only REST catalogs are supported.


Configuration
=============

Configuration root level parameters:
- **Catalog Name:** The Iceberg catalog to be used.- **Catalog Name:** Name of the Iceberg catalog to use.
- **Warehouse:** Warehouse ID.
- **URI:** REST endpoint for the Iceberg catalog.
- **Token:** API token for authentication.

Configuration row level parameters:
Data source configuration parameters:
- **Namespace:** Namespace of the Iceberg table.
- **Table Name:** Name of the Iceberg table.

Data selection parameters:
- **Data selection mode:** Supported modes are All data and Selected columns.
- **Columns to extract:** List of columns to extract (used if "Selected columns" mode is chosen).

Destination paremeters:
- **Store as Parquet:** If enabled, the extractor saves the result as a Parquet file in file storage instead of a table in the Keboola storage bucket.
- **Load Type:** In Full Load mode, the destination table is overwritten on each run. In Incremental Load mode, data is upserted into the destination table based on the primary key. Append mode does not use primary keys and does not deduplicate data.
- **Primary Key [optional]:** List of primary key columns for incremental loads. If not specified, incremental mode works as append mode.
- **Preserve Insertion Order:** Disabling this option may help prevent out-of-memory issues.
- **File Name:** Name of the output file (only when storing as Parquet). If left empty, a name is generated as `bucket.table_name` or `catalog.schema.table_name` when using Unity Catalog.
- **Table Name:** Name of the output table (only when storing in the storage bucket). If left empty, a name is generated as `bucket.table_name` or `catalog.schema.table_name` when using Unity Catalog.
