"""
Template Component main class.

"""

import logging
import os
from collections import OrderedDict
from datetime import datetime
import tracemalloc
import duckdb
from pyiceberg.catalog.rest import RestCatalog

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration


# DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.duckdb = self._init_duckdb_connection()
        self.catalog = self._init_catalog()
        self.tracemalloc = tracemalloc.start()

    def run(self):

        start_time = datetime.now()

        table = self.catalog.load_table((self.params.source.namespace, self.params.source.name))


        # Padá na OOM
        # table.scan(
        #     # snapshot_id=new,
        #     # row_filter=GreaterThanOrEqual("id", 2) | GreaterThanOrEqual("score", 90.0),
        #     # selected_fields=("name", "score", "new_col"),
        # ).to_duckdb(table_name="out_table", connection=self.duckdb)

        # Funkční workaround
        batches = table.scan(
            limit=100_000,
            # snapshot_id=new,
            # row_filter=GreaterThanOrEqual("id", 2) | GreaterThanOrEqual("score", 90.0),
            # selected_fields=("name", "score", "new_col"),
        ).to_arrow_batch_reader()

        batch = next(batches)
        duckdb.sql("CREATE TABLE out_table AS SELECT * FROM batch")
        logging.info(f"Created initial table with {batch.num_rows} rows")

        for batch in batches:
            duckdb.sql("INSERT INTO out_table SELECT * FROM batch")
            logging.info(f"Inserted {batch.num_rows} rows")

#
#             logging.info(self.duckdb.sql("""
#             SELECT path, round(size/10**6)::INT as 'size_MB' FROM duckdb_temporary_files();
#                                              """).show())
#
#             logging.info(self.duckdb.sql("""
# SELECT tag, round(memory_usage_bytes/10**6)::INT as 'mem_MB',
#     round(temporary_storage_bytes/10**6)::INT as 'storage_MB'
# FROM duckdb_memory();
#                                              """).show())

            import psutil

            # Create a list to store process info
            process_list = []

            # Iterate over all running processes
            for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
                try:
                    # Append process details to the list
                    process_list.append({
                        'PID': proc.info['pid'],
                        'Name': proc.info['name'],
                        'Memory Usage (RSS)': proc.info['memory_info'].rss / (1024 ** 2)
                    })
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    # Handle any errors due to process termination or access denial
                    continue

            logging.info(f"Current memory usage of processes: {process_list}")

            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')
            out = ""
            for stat in top_stats[:10]:
                out += f"{stat}\n"
            logging.info(f"1st stage: \n {out}")

        # self.duckdb.execute("CREATE TABLE persist AS SELECT * FROM out_table")
        # self.duckdb.close()
        # exit(0)
        #
        # self.duckdb.execute("CREATE TABLE out_table AS SELECT * FROM persist")

        if self.params.destination.parquet_output:
            out_file = self.create_out_file_definition(f"{self.params.destination.file_name}.parquet")
            q = f" COPY out_table TO '{out_file.full_path}'; "
            logging.debug(f"Running query: {q}; ")
            self.duckdb.execute(q)
            self.write_manifest(out_file)
        else:
            table_meta = self.duckdb.execute(f"DESCRIBE out_table;").fetchall()
            schema = OrderedDict(
                {
                    c[0]: ColumnDefinition(
                        data_types=BaseType(dtype=self.convert_base_types(c[1])),
                        primary_key=c[3] == "PRI" if not self.params.destination.primary_key else False,
                    )
                    for c in table_meta
                }  # c[0] is the column name, c[1] is the data type, c[3] is the primary key
            )

            table_name = self.params.destination.table_name or self.params.source.name

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
        # os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = {
            # "temp_directory": DUCK_DB_DIR,
            "max_memory": f"{self.params.duckdb_max_memory_mb}MB",
        }
        logging.info(f"Connecting to DuckDB with config: {config}")
        conn = duckdb.connect("data/out/duck.db", config=config)
        # conn = duckdb.connect(database=f"{DUCK_DB_DIR}/tmp.db", config=config)
        # conn = duckdb.connect(database=":memory:", config=config)

        if not self.params.destination.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;")
        return conn

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
        snapshots = self.catalog.load_table((self.params.source.namespace, self.params.source.name)).snapshots()
        return [
            SelectElement(
                label=str(datetime.fromtimestamp(s.timestamp_ms / 1000)),
                value=str(s.snapshot_id),
            )
            for s in snapshots
        ]

    @sync_action("list_columns")
    def list_columns(self):
        schema = self.catalog.load_table((self.params.source.namespace, self.params.source.name)).schema()
        return [SelectElement(label=f"{field.name} ({field.field_type})", value=field.name) for field in schema.fields]


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
