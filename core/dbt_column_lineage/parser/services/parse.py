from operator import attrgetter
from typing import List, Tuple

from dbt_column_lineage.parser.exceptions import RootNotFoundException
from dbt_column_lineage.parser.schemas.parsed import (
    CTE,
    A_Star,
    Field,
    FieldRef,
    NodeSQL,
    Root,
    Source,
    Statement,
)
from dbt_column_lineage.parser.schemas.relation import ComponentName, Path
from dbt_column_lineage.parser.schemas.token import TokenList
from dbt_column_lineage.parser.services._formula import get_formula
from dbt_column_lineage.parser.visitors import (
    ColumnRefVisitor,
    CommonTableExprVisitor,
    RangeVarVisitor,
    ResTargetVisitor,
    SelectStmtVisitor,
)
from pglast import _extract_comments, parse_sql
from pglast.ast import A_Star as A_StarNode
from pglast.ast import ColumnRef, CommonTableExpr, Node, ResTarget, SelectStmt
from pglast.parser import Token, scan


def get_field_ref(node: ColumnRef) -> FieldRef:
    component_names = ComponentName.values()

    if len(node.fields) > len(component_names):
        raise ValueError("Too many values in column reference.")

    field = node.fields[-1]
    field = A_Star if isinstance(field, A_StarNode) else field.val

    args = node.fields[:-1]
    args = list(map(attrgetter("val"), args))
    path = Path.from_args(args)

    return FieldRef(path=path, name=field)


def get_field(node: ResTarget, node_sql: NodeSQL) -> Field:
    column_refs = ColumnRefVisitor()(node)
    field_refs = list(map(get_field_ref, column_refs))
    formula = get_formula(node_sql, column_refs)

    return Field(alias=node.name, depends_on=field_refs, formula=formula)


def get_fields(node: Node, node_sql: NodeSQL) -> List[Field]:
    targets = ResTargetVisitor()(node)

    bounds = list(map(attrgetter("location"), targets))
    bounds.append(node_sql.end_idx)

    fields = []

    for i, target in enumerate(targets):
        cte = get_field(
            target,
            NodeSQL(
                sql=node_sql.sql,
                tokens=node_sql.tokens,
                start_idx=bounds[i],
                end_idx=bounds[i + 1],
            ),
        )

        fields.append(cte)

    return fields


def get_source(node: RangeVarVisitor) -> Source:
    return Source(
        path=Path(
            database=node.catalogname,
            schema=node.schemaname,
            identifier=node.relname,
        ),
        alias=(node.alias.aliasname if node.alias else None),
    )


def get_sources(node: Node) -> List[Source]:
    range_vars = RangeVarVisitor()(node)
    return list(map(get_source, range_vars))


def get_statement(node: SelectStmt, node_sql: NodeSQL) -> Statement:
    fields_start_idx = node.targetList[0].location

    if not node.fromClause:
        # FIXME: if there is no from clause it doesn't mean sql ends up with target
        fields_end_idx = node_sql.end_idx
    else:
        last_target_start_idx = node.targetList[-1].location
        tokens = node_sql.tokens.real_slice(last_target_start_idx)

        from_token = next(filter(lambda token: token.name == "FROM", tokens))
        fields_end_idx = from_token.start - 1

    fields_node_sql = NodeSQL(
        sql=node_sql.sql,
        tokens=node_sql.tokens,
        start_idx=fields_start_idx,
        end_idx=fields_end_idx,
    )
    fields = get_fields(node.targetList, fields_node_sql)
    sources = get_sources(node.fromClause)

    return Statement(
        fields=fields,
        sources=sources,
    )


def get_cte(node: CommonTableExpr, node_sql: NodeSQL) -> CTE:
    statement = get_statement(node.ctequery, node_sql)

    return CTE(
        name=node.ctename,
        fields=statement.fields,
        sources=statement.sources,
    )


def get_ctes(node: SelectStmt, node_sql: NodeSQL) -> List[CTE]:
    cte_exprs = CommonTableExprVisitor(flat=True)(node.withClause)

    location_idxs = list(map(attrgetter("location"), cte_exprs))
    location_idxs.append(node_sql.end_idx)

    ctes = []

    for i, cte_expr in enumerate(cte_exprs):
        cte = get_cte(
            cte_expr,
            NodeSQL(
                sql=node_sql.sql,
                tokens=node_sql.tokens,
                start_idx=location_idxs[i],
                end_idx=location_idxs[i + 1],
            ),
        )

        ctes.append(cte)

    return ctes


def get_root(node: SelectStmt, node_sql: NodeSQL) -> Root:
    statement = get_statement(node, node_sql)

    return Root(
        fields=statement.fields,
        sources=statement.sources,
    )


def remove_comments(sql: str) -> str:
    parts = []
    comments = _extract_comments(sql)
    prev_end_idx = 0

    for comment in comments:
        start_idx = comment.location
        parts.append(sql[prev_end_idx:start_idx])
        prev_end_idx = start_idx + len(comment.text)

    parts.append(sql[prev_end_idx:])

    return "".join(parts)


def get_ctes_end_idx(node: SelectStmt, tokens: TokenList) -> int:
    def is_parenthesis(token: Token):
        return token.name in ("ASCII_40", "ASCII_41")

    ctes = CommonTableExprVisitor(flat=True)(node.withClause)

    if not ctes:
        return 0

    last_cte = ctes[-1]
    del ctes

    tokens = tokens.real_slice(last_cte.location)
    tokens = filter(is_parenthesis, tokens)

    # first parenthesis is always opening
    next(tokens)
    balance = 1

    for token in tokens:
        # (
        if token.name == "ASCII_40":
            balance += 1
        # )
        elif token.name == "ASCII_41":
            balance -= 1

        if balance < 0:
            raise Exception("Invalid parenthesis sequence.")

        if balance == 0:
            return token.start

    raise Exception("Invalid parenthesis sequence.")


def parse(sql: str) -> Tuple[Root, List[CTE]]:
    sql = remove_comments(sql)
    parsed_sql = parse_sql(sql)

    tokens = scan(sql)
    tokens = TokenList(tokens)

    stmt = parsed_sql[0].stmt

    select_stmts = SelectStmtVisitor(flat=True)(stmt)

    if not select_stmts:
        RootNotFoundException()

    select_stmt = select_stmts[0]
    ctes_end_idx = get_ctes_end_idx(select_stmt, tokens)

    root = get_root(
        select_stmt,
        NodeSQL(
            sql=sql,
            tokens=tokens,
            start_idx=ctes_end_idx + 1,
            end_idx=len(sql) - 1,
        ),
    )

    ctes = get_ctes(
        stmt,
        NodeSQL(
            sql=sql,
            tokens=tokens,
            start_idx=0,
            # TODO: remove spaces ?
            end_idx=ctes_end_idx,
        ),
    )

    return root, ctes
