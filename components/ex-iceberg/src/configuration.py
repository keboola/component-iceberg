from enum import Enum
from pydantic import BaseModel, Field, computed_field


class CommonCatalogConfiguration(BaseModel):
    name: str
    warehouse: str
    uri: str
    token: str = Field(alias="#token")


class CommonConfiguration(BaseModel):
    catalog: CommonCatalogConfiguration
    duckdb_max_memory_mb: int = 128
    debug: bool = False


class DataSelectionMode(str, Enum):
    all_data = "all_data"
    select_columns = "select_columns"
    custom_query = "custom_query"


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Source(BaseModel):
    namespace: str = ""
    table_name: str = ""
    snapshot_id: int | None = None


class DataSelection(BaseModel):
    mode: DataSelectionMode = Field(default=DataSelectionMode.all_data)
    columns: list[str] = Field(default_factory=list)
    query: str = ""


class Destination(BaseModel):
    preserve_insertion_order: bool = True
    parquet_output: bool = False
    file_name: str = ""
    table_name: str = ""
    load_type: LoadType = Field(default=LoadType.incremental_load)
    primary_key: list[str] = Field(default_factory=list)

    @computed_field
    @property
    def incremental(self) -> bool:
        return self.load_type == LoadType.incremental_load


class Configuration(CommonConfiguration):
    source: Source = Field(default_factory=Source)
    data_selection: DataSelection = Field(default_factory=DataSelection)
    destination: Destination = Field(default_factory=Destination)
