from collections import defaultdict

from dbt_column_lineage.parser.schemas.lineage import ColumnLineage, ColumnsLineage
from dbt_column_lineage.parser.schemas.parsed import Field, Root
from dbt_column_lineage.parser.schemas.relation import Relation


def get_column_lineage(field: Field) -> ColumnLineage:
    stack = [field]
    res = defaultdict(list)

    while len(stack) != 0:
        field = stack.pop()

        for field_ref in field.depends_on:
            reference = field_ref.source.reference

            # finish
            if isinstance(reference, Relation):
                res[field_ref.source.reference].append(field_ref.name)
                continue

            # get field by name
            field_ = reference.get_field(field_ref.name)

            if not field_:
                raise ValueError(
                    "Reference {} doesn't have field {}".format(reference, field_ref.name)
                )

            stack.append(field_)

    return res


def get_columns_lineage(root: Root) -> ColumnsLineage:
    res = {}
    for field in root.fields:
        res[field.name] = get_column_lineage(field)

    return res
