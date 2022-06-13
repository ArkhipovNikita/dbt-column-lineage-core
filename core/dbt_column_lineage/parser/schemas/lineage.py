from dataclasses import dataclass
from typing import List, Mapping

from dbt_column_lineage.parser.schemas.relation import Relation


@dataclass
class ColumnLineage:
    formula: str
    lineage: Mapping[Relation, List[str]]


ColumnsLineage = Mapping[str, ColumnLineage]
