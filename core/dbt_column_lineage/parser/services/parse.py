import re
from dataclasses import dataclass
from functools import cached_property
from operator import attrgetter
from typing import List, Tuple

from dbt_column_lineage.parser.exceptions import RootNotFoundException
from dbt_column_lineage.parser.schemas.parsed import (
    CTE,
    A_Star,
    Field,
    FieldRef,
    Root,
    Source,
    Statement,
)
from dbt_column_lineage.parser.schemas.relation import ComponentName, Path
from dbt_column_lineage.parser.visitors import (
    ColumnRefVisitor,
    CommonTableExprVisitor,
    RangeVarVisitor,
    ResTargetVisitor,
    SelectStmtVisitor,
)
from pglast import parse_sql
from pglast.ast import A_Star as A_StarNode
from pglast.ast import ColumnRef, CommonTableExpr, Node, ResTarget, SelectStmt


@dataclass(frozen=True)
class NodeSQL:
    # must be lowered
    sql: str
    # global indexes
    start_idx: int
    end_idx: int

    @cached_property
    def area(self) -> str:
        return self.sql[self.start_idx : self.end_idx]


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

    # TODO: node_sql for column ref
    # strip alias, punctuation and space
    formula = (
        re.sub("\s+as[\s\S]+", "", node_sql.area, flags=re.IGNORECASE)
        if node.name
        else re.sub("(?:,)\s*$", "", node_sql.area)
    )

    parts = []
    prev_idx = 0
    for column_ref, field_ref in zip(column_refs, field_refs):
        start_idx = column_ref.location - node_sql.start_idx

        # todo: take from field_ref
        components = list(
            map(lambda c: c.val if not isinstance(c, A_StarNode) else "\*", column_ref.fields)
        )
        # todo: quote policy
        quote_component = lambda s: '"{}"'.format(s)
        component_pattern = lambda s: "(?:{}|{})".format(s, quote_component(s))
        # todo: extract separator
        pattern = ".".join(map(component_pattern, components))
        m = re.match(pattern, formula[start_idx:])

        if m is None:
            print()

        end_idx = m.end() + start_idx
        parts.append(formula[prev_idx:start_idx])
        prev_idx = end_idx
    else:
        parts.append(formula[prev_idx:])

    formula = "%s".join(parts)

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
        match = re.match(
            r"(.+)\s+from",
            node_sql.sql[last_target_start_idx:],
            flags=re.IGNORECASE,
        )
        fields_end_idx = last_target_start_idx + match.end(1)

    fields_node_sql = NodeSQL(
        sql=node_sql.sql,
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


def parse(sql: str) -> Tuple[Root, List[CTE]]:
    # TODO: clean comments
    parsed_sql = parse_sql(sql)
    stmt = parsed_sql[0].stmt

    select_stmts = SelectStmtVisitor(flat=True)(stmt)

    if not select_stmts:
        RootNotFoundException()

    select_stmt = select_stmts[0]

    root = get_root(
        select_stmt,
        NodeSQL(
            sql=sql,
            start_idx=1470,
            # start_idx=select_stmt.location,
            end_idx=len(sql) - 1,
        ),
    )

    ctes = get_ctes(
        stmt,
        NodeSQL(
            sql=sql,
            start_idx=0,
            end_idx=1470,
            # TODO: remove spaces ?
            # end_idx=select_stmt.location,
        ),
    )

    return root, ctes
