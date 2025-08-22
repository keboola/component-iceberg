from enum import Enum
from pydantic import BaseModel, Field


class CommonCatalogConfiguration(BaseModel):
    name: str
    warehouse: str
    uri: str
    token: str = Field(alias="#token")


class CommonConfiguration(BaseModel):
    catalog: CommonCatalogConfiguration
    duckdb_max_memory_mb: int = 128
    debug: bool = False


class Mode(str, Enum):
    replace = "replace"
    append = "append"
    upsert = "upsert"


class Destination(BaseModel):
    namespace: str = ""
    table_name: str = ""
    mode: Mode = Field(default=Mode.replace)
    preserve_insertion_order: bool = True
    all_varchar: bool = False
    primary_key: list[str] = Field(default_factory=list)
    partition_by: list[str] = Field(default_factory=list)


class Configuration(CommonConfiguration):
    destination: Destination = Field(default_factory=Destination)
