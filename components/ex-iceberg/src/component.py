"""
Template Component main class.

"""

import logging
import os
from collections import OrderedDict
from datetime import datetime
import duckdb
import polars
from pyiceberg.catalog.rest import RestCatalog

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement, ValidationResult, MessageType

from configuration import Configuration


DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.duckdb = self._init_duckdb_connection()
        self.catalog = self._init_catalog()

    def run(self):
        start_time = datetime.now()

        self.duckdb.execute(f""" CREATE TABLE out_table AS {self.get_query()} """)

        logging.info(
            self.duckdb.sql("""
                    SELECT path, round(size/10**6)::INT as 'size_MB' FROM duckdb_temporary_files();
                                                     """).show()
        )

        if self.params.destination.parquet_output:
            out_file = self.create_out_file_definition(f"{self.params.destination.file_name}.parquet")
            q = f" COPY out_table TO '{out_file.full_path}'; "
            logging.debug(f"Running query: {q}; ")
            self.duckdb.execute(q)
            self.write_manifest(out_file)
        else:
            table_meta = self.duckdb.execute("DESCRIBE out_table;").fetchall()
            schema = OrderedDict(
                {
                    c[0]: ColumnDefinition(
                        data_types=BaseType(dtype=self.convert_base_types(c[1])),
                        primary_key=c[3] == "PRI" if not self.params.destination.primary_key else False,
                    )
                    for c in table_meta
                }  # c[0] is the column name, c[1] is the data type, c[3] is the primary key
            )

            table_name = self.params.destination.table_name or self.params.source.table_name

            out_table = self.create_out_table_definition(
                f"{table_name}.csv",
                schema=schema,
                primary_key=self.params.destination.primary_key,
                incremental=self.params.destination.incremental,
                has_header=True,
            )

            try:
                q = f"COPY out_table TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
                logging.debug(f"Running query: {q}; ")
                self.duckdb.execute(q)
                self.write_manifest(out_table)
            except duckdb.ConversionException as e:
                raise UserException(f"Error during query execution: {e}")

        logging.debug(f"Execution time: {(datetime.now() - start_time).seconds:.2f} seconds")

    def _init_catalog(self):
        catalog = RestCatalog(
            name=self.params.catalog.name,
            warehouse=self.params.catalog.warehouse,
            uri=self.params.catalog.uri,
            token=self.params.catalog.token,
        )

        return catalog

    def _init_duckdb_connection(self):
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = {
            "temp_directory": DUCK_DB_DIR,
            "max_memory": f"{self.params.duckdb_max_memory_mb}MB",
        }
        logging.info(f"Connecting to DuckDB with config: {config}")
        conn = duckdb.connect(database=":memory:", config=config)

        conn.execute(f"""
            INSTALL iceberg;
            LOAD iceberg;

            CREATE SECRET r2_secret (
            TYPE ICEBERG,
            TOKEN '{self.params.catalog.token}');

            ATTACH '{self.params.catalog.warehouse}' AS catalog (
            TYPE ICEBERG,
            ENDPOINT '{self.params.catalog.uri.replace("https://", "")}');
        """)

        if not self.params.destination.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;")
        return conn

    def get_query(self):
        if self.params.data_selection.mode == "custom_query":
            query = (
                self.params.data_selection.query
                % f'catalog."{self.params.source.namespace}"."{self.params.source.table_name}"'
            )
        elif self.params.data_selection.mode == "select_columns":
            query = f"""
            SELECT {", ".join(self.params.data_selection.columns)}
            FROM catalog."{self.params.source.namespace}"."{self.params.source.table_name}" """
        elif self.params.data_selection.mode == "all_data":
            query = f'SELECT * FROM catalog."{self.params.source.namespace}"."{self.params.source.table_name}")'
        else:
            raise UserException("Invalid data selection mode")

        return query

    @staticmethod
    def convert_base_types(dtype: str) -> SupportedDataTypes:
        if dtype in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return SupportedDataTypes.INTEGER
        elif dtype in ["REAL", "DECIMAL"]:
            return SupportedDataTypes.NUMERIC
        elif dtype == "DOUBLE":
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif dtype in ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    def to_markdown(self, out):
        polars.Config.set_tbl_formatting("ASCII_MARKDOWN")
        polars.Config.set_tbl_hide_dataframe_shape(True)
        formatted_output = str(out)
        return formatted_output

    @sync_action("list_namespaces")
    def list_namespaces(self):
        namespaces = self.catalog.list_namespaces()
        return [SelectElement(n[0]) for n in namespaces]

    @sync_action("list_tables")
    def list_tables(self):
        tables = self.catalog.list_tables(self.params.source.namespace)
        return [SelectElement(t[1]) for t in tables]

    @sync_action("list_snapshots")
    def list_snapshots(self):
        snapshots = self.catalog.load_table((self.params.source.namespace, self.params.source.table_name)).snapshots()
        return [
            SelectElement(
                label=str(datetime.fromtimestamp(s.timestamp_ms / 1000)),
                value=str(s.snapshot_id),
            )
            for s in snapshots
        ]

    @sync_action("list_columns")
    def list_columns(self):
        schema = self.catalog.load_table((self.params.source.namespace, self.params.source.table_name)).schema()
        return [SelectElement(label=f"{field.name} ({field.field_type})", value=field.name) for field in schema.fields]

    @sync_action("table_preview")
    def table_preview(self):
        out = self.duckdb.execute(f"""
                SELECT *
                FROM catalog."{self.params.source.namespace}"."{self.params.source.table_name}")
                LIMIT 10;
                """).pl()

        formatted_output = self.to_markdown(out)

        return ValidationResult(formatted_output, MessageType.SUCCESS)

    @sync_action("query_preview")
    def query_preview(self):
        query = (
            self.params.data_selection.query % f'catalog."{self.params.source.namespace}"."{self.params.source.name}"'
        )

        if "limit" not in query.lower():
            query = f"{query} LIMIT 10;"

        out = self.duckdb.execute(query).pl()
        formatted_output = self.to_markdown(out)
        return ValidationResult(formatted_output, MessageType.SUCCESS)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
