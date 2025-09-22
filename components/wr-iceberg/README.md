Iceberg Writer
=============

Component for writing data to Iceberg tables. Supports replace, append, and upsert load modes. Currently, only REST catalogs are supported.

Configuration
=============

Configuration root level parameters:
- **Catalog Name:** The Iceberg catalog to be used.- **Catalog Name:** Name of the Iceberg catalog to use.
- **Warehouse:** Warehouse ID.
- **URI:** REST endpoint for the Iceberg catalog.
- **Token:** API token for authentication.

Configuration row level parameters:
- **Namespace:** Namespace of the Iceberg table.
- **Table Name:** Name of the Iceberg table to write to.
- **Load Type:** Supported load types are Replace, Append, and Upsert.
- **Primary Key:** List of primary key columns for upsert loads. If not specified, the primary keys of the input table are used.
- **Preserve Insertion Order:** Disabling this option may help prevent out-of-memory issues.