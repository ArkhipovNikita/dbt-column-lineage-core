from typing import Iterable

from core.parser.schemas.lineage import ColumnsLineage
from core.parser.schemas.relation import Relation
from core.parser.services.lineage import get_columns_lineage
from core.parser.services.parse import parse
from core.parser.services.resolve import resolve


def resolve_columns_lineage(sql: str, initial_relations: Iterable[Relation]) -> ColumnsLineage:
    # TODO: parse adapter
    root, ctes = parse(sql)
    resolve(root, ctes, initial_relations)
    columns_lineage = get_columns_lineage(root)

    return columns_lineage
