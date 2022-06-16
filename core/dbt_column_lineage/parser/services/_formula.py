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

    if len(tokens) > 2 and tokens[-1].name == "IDENT" and tokens[-2].name == "AS":
        # ident
        del tokens[-1]
        # as
        del tokens[-1]

    return tokens


def get_formula(node_sql: NodeSQL, column_refs: List[ColumnRef]) -> str:
    formula_tokens = get_field_def(node_sql.tokens_area)
    exclude = []

    # fill in exclude list (bounds of column_ref)
    for i, column_ref in enumerate(column_refs):
        start_idx = column_ref.location
        end_idx = None

        # TODO: optimize search
        for j in range(len(formula_tokens)):
            token = formula_tokens[j]
            if token.start == start_idx:
                # slide tokens to cover all parts of column ref
                column_ref_token_count = 2 * len(column_ref.fields) - 1
                j += column_ref_token_count - 1
                token = formula_tokens[j]

                end_idx = token.end

                break

        if not end_idx:
            raise Exception("Token that represents column ref wasn't found")

        exclude.append((i, start_idx, end_idx))

    exclude.sort(key=lambda e: e[1])
    formula_bounds = (formula_tokens[0].start, formula_tokens[-1].end)
    prev_end_idx = formula_bounds[0]
    parts = []

    # format original formula: replace column_refs with {pos}
    for pos, start_idx, end_idx in exclude:
        parts.append(node_sql.sql[prev_end_idx:start_idx])
        parts.append("{%s}" % (pos,))
        prev_end_idx = end_idx + 1

    parts.append(node_sql.sql[prev_end_idx : formula_bounds[1] + 1])

    return "".join(parts)
