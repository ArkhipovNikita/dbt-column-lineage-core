from typing import Iterable, Mapping

from dbt_column_lineage.parser.schemas.relation import Relation

ColumnLineage = Mapping[Relation, Iterable[str]]
ColumnsLineage = Mapping[str, ColumnLineage]