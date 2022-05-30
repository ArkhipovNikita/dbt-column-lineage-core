from typing import Iterable, Mapping

from core.parser.schemas.relation import Relation

ColumnLineage = Mapping[Relation, Iterable[str]]
ColumnsLineage = Mapping[str, ColumnLineage]
