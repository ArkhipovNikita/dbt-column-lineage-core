import copy
from typing import List

from dbt_column_lineage.parser.schemas.parsed import NodeSQL
from dbt_column_lineage.parser.schemas.token import TokenList
from pglast.ast import ColumnRef


def get_field_def(tokens: TokenList) -> TokenList:
    tokens = copy.deepcopy(tokens)

    if tokens[-1].name == "ASCII_44":
        # common
        del tokens[-1]

    if tokens[-1].name == "IDENT":
        # ident
        del tokens[-1]
        # as
        del tokens[-1]

    return tokens


def get_formula(node_sql: NodeSQL, column_refs: List[ColumnRef]) -> str:
    formula_tokens = get_field_def(node_sql.tokens_area)
    formula_bounds = (formula_tokens[0].start, formula_tokens[-1].end)
    prev_end_idx = formula_bounds[0]
    parts = []
    i = 0

    for column_ref in column_refs:
        start_idx = column_ref.location
        end_idx = None

        for j in range(i, len(formula_tokens)):
            token = formula_tokens[j]
            if token.start == start_idx:
                end_idx = token.end
                i = j
                break

        if not end_idx:
            raise Exception("Token that represents column ref wasn't found")

        parts.append(node_sql.sql[prev_end_idx:start_idx])
        prev_end_idx = end_idx + 1

    parts.append(node_sql.sql[prev_end_idx : formula_bounds[1] + 1])

    return "%s".join(parts)
