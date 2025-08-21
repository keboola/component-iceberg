import logging
import os
from datetime import datetime

import duckdb
from duckdb import DuckDBPyRelation
from pyiceberg.catalog.rest import RestCatalog, Table as IcebergTable
import pyarrow as pa

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import (
    IODefinition,
    TableDefinition,
    # ,FileDefinition,
    # BaseType,
    # ColumnDefinition,
    # SupportedDataTypes,
)
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from storage_api_client import SAPIClient
from configuration import Configuration, Mode


DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.duckdb = None
        self.catalog = self._init_catalog()

    def run(self):
        start_time = datetime.now()
        self.duckdb = self._init_duckdb_connection()
        tables = self.get_input_tables_definitions()
        files = self.get_input_files_definitions()

        if (tables and files) or (not tables and not files):
            raise UserException("Each configuration row can be mapped to either a file or a table, but not both.")

        if len(tables) > 1:
            raise UserException("Each configuration row can have only one input table")

        self.load_table(tables[0])

        logging.debug(f"Execution time: {(datetime.now() - start_time).seconds:.2f} seconds")

    def _init_duckdb_connection(self):
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = {
            "temp_directory": DUCK_DB_DIR,
            "max_memory": f"{self.params.duckdb_max_memory_mb}MB",
        }
        logging.debug(f"Connecting to DuckDB with config: {config}")
        conn = duckdb.connect(database=f"{DUCK_DB_DIR}/tmp.db", config=config)

        if not self.params.destination.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;")
        return conn

    def _create_table_relation(self, in_table: IODefinition) -> DuckDBPyRelation:
        if isinstance(in_table, TableDefinition):
            dtypes = {key: value.data_types.get("base").dtype for key, value in in_table.schema.items()}

            relation = self.duckdb.read_csv(
                path_or_buffer=in_table.full_path,
                delimiter=in_table.delimiter,
                quotechar=in_table.enclosure,
                header=in_table.has_header,
                names=in_table.column_names,
                dtype=dtypes,
                all_varchar=self.params.destination.all_varchar,
            )
        else:
            # relation = self.duckdb.read_parquet()
            relation = None

        return relation

    def load_table(self, table: IODefinition):
        namespace = self.params.destination.namespace
        table_name = self.params.destination.table_name
        relation = self._create_table_relation(table)

        if not self.catalog.namespace_exists(namespace):
            self.catalog.create_namespace(namespace)

        pk = None
        if isinstance(table, TableDefinition):
            pk = table.primary_key or []

        batches = relation.fetch_arrow_reader(batch_size=5_000_000)

        first = True

        for raw_batch in batches:
            batch = pa.Table.from_batches([raw_batch])

            if first:
                table = self._prepare_table(namespace, table_name, batch.schema)

            if self.params.destination.mode == Mode.upsert:
                table.upsert(batch, join_cols=self.params.destination.primary_key or pk)
            else:
                table.append(batch)

    def _prepare_table(self, namespace: str, table_name: str, schema: pa.Schema) -> IcebergTable:
        exist = self.catalog.table_exists(identifier=(namespace, table_name))

        if self.params.destination.mode == Mode.replace:
            if exist:
                self.catalog.drop_table(identifier=(namespace, table_name))
                exist = False

        if not exist:
            table = self.catalog.create_table(
                identifier=(namespace, table_name),
                schema=schema,
            )
        else:
            table = self.catalog.load_table(identifier=(namespace, table_name))

        return table

    def _init_catalog(self):
        if self.configuration.action not in ["", "run"]:
            logging.getLogger().setLevel(logging.CRITICAL)

        catalog = RestCatalog(
            name=self.params.catalog.name,
            warehouse=self.params.catalog.warehouse,
            uri=self.params.catalog.uri,
            token=self.params.catalog.token,
        )

        return catalog

    @sync_action("list_namespaces")
    def list_namespaces(self):
        namespaces = self.catalog.list_namespaces()
        return [SelectElement(n[0]) for n in namespaces]

    @sync_action("list_tables")
    def list_tables(self):
        tables = self.catalog.list_tables(self.params.destination.namespace)
        return [SelectElement(t[1]) for t in tables]

    @sync_action("list_table_columns")
    def list_table_columns(self):
        in_tables = self.configuration.tables_input_mapping

        if in_tables:
            storage_client = SAPIClient(self.environment_variables.url, self.environment_variables.token)

            table_id = self.configuration.tables_input_mapping[0].source
            columns = storage_client.get_table_detail(table_id)["columns"]
        else:
            raise UserException("Can list only columns from input tables, not files.")

        return [SelectElement(col) for col in columns]


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
