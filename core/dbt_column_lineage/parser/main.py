from typing import Iterable

from dbt_column_lineage.parser.schemas.lineage import ColumnsLineage
from dbt_column_lineage.parser.schemas.relation import Relation
from dbt_column_lineage.parser.services.lineage import get_columns_lineage
from dbt_column_lineage.parser.services.parse import parse
from dbt_column_lineage.parser.services.resolve import resolve


def resolve_columns_lineage(sql: str, initial_relations: Iterable[Relation]) -> ColumnsLineage:
    # TODO: parse adapter
    root, ctes = parse(sql)
    resolve(root, ctes, initial_relations)
    columns_lineage = get_columns_lineage(root)

    return columns_lineage
