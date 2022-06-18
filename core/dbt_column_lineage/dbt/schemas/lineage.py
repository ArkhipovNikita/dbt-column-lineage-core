from dataclasses import dataclass, field
from typing import List

from dbt.clients.system import write_json
from dbt_column_lineage.dbt.schemas.base import dbtIntegrationMixin


@dataclass
class Source(dbtIntegrationMixin):
    name: str
    columns: List[str]


@dataclass
class ColumnLineage(dbtIntegrationMixin):
    name: str
    formula: str = ""
    sources: List[Source] = field(default_factory=list)


ColumnsLineage = List[ColumnLineage]


@dataclass
class ModelColumnsLineage(dbtIntegrationMixin):
    name: str
    columns: ColumnsLineage


@dataclass
class ModelsColumnsLineage(dbtIntegrationMixin):
    models: List[ModelColumnsLineage]

    def write(self, path: str):
        data = self.to_dict()
        write_json(path, data)
