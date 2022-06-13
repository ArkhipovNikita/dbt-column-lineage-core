import re
from typing import Iterable, List

from dbt_column_lineage.parser.schemas.parsed import A_Star, FieldRef, NodeSQL
from dbt_column_lineage.parser.schemas.relation import ComponentName
from pglast.ast import ColumnRef

COMPONENT_SEP = "."


def get_field_def(field_row: str) -> str:
    return re.sub("\s*(\s+as[\s\S]+)?(,)?\s*$", "", field_row, flags=re.IGNORECASE)


def get_field_ref_components(field_ref: FieldRef) -> List[str]:
    components = map(field_ref.path.get_part, ComponentName)
    components = filter(None, components)
    components = list(components)
    components.append(field_ref.name)

    return components


def quote_component(component: str) -> str:
    # TODO: must be depended on database
    return '"{}"'.format(component)


def screen_component(component: str) -> str:
    if component == A_Star:
        return "\{}".format(component)

    return component


def pattern_component(component: str) -> str:
    screened = screen_component(component)
    quoted = quote_component(screened)
    return r"(?:{}|{})".format(screened, quoted)


def get_formula(
    field_refs: Iterable[FieldRef],
    column_refs: Iterable[ColumnRef],
    node_sql: NodeSQL,
) -> str:
    formula = get_field_def(node_sql.area)
    parts = []
    prev_end_idx = 0

    for column_ref, field_ref in zip(column_refs, field_refs):
        components = get_field_ref_components(field_ref)
        pattern = COMPONENT_SEP.join(map(pattern_component, components))

        # find field_ref in formula
        start_idx = column_ref.location - node_sql.start_idx
        match = re.match(pattern, formula[start_idx:])
        end_idx = match.end() + start_idx

        parts.append(formula[prev_end_idx:start_idx])
        prev_end_idx = end_idx

    parts.append(formula[prev_end_idx:])

    return "%s".join(parts)
