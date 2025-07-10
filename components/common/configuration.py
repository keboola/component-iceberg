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

