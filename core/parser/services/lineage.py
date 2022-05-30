from collections import defaultdict

from core.parser.schemas.lineage import ColumnLineage, ColumnsLineage
from core.parser.schemas.parsed import Field, Root
from core.parser.schemas.relation import Relation


def get_column_lineage(field: Field) -> ColumnLineage:
    stack = [field]
    res = defaultdict(list)

    while len(stack) != 0:
        field = stack.pop()

        for field_ref in field.depends_on:

            # finish
            if isinstance(field_ref.source.reference, Relation):
                res[field_ref.source.reference].append(field_ref.name)
                continue

            # get field by name
            for field_ in field_ref.source.reference.fields:
                if field_ref.name == field_.name:
                    stack.append(field_)
                    break

    return res


def get_columns_lineage(root: Root) -> ColumnsLineage:
    res = {}
    for field in root.fields:
        res[field.name] = get_column_lineage(field)

    return res
