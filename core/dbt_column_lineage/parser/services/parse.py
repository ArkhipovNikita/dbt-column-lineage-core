from typing import List, Tuple

from dbt_column_lineage.parser.exceptions import RootNotFoundException
from dbt_column_lineage.parser.schemas.parsed import (
    CTE,
    A_Star,
    Field,
    FieldRef,
    Root,
    Source,
)
from dbt_column_lineage.parser.schemas.relation import ComponentName, Path
from pglast import parse_sql
from pglast.ast import A_Star as A_StarNode
from pglast.ast import ColumnRef, CommonTableExpr, Node, RangeVar, ResTarget, SelectStmt
from pglast.visitors import Ancestor, Skip, Visitor


class FieldRefVisitor(Visitor):
    def __call__(self, node: Node) -> List[FieldRef]:
        self.field_refs = []
        super().__call__(node)
        return self.field_refs

    def visit_ColumnRef(self, ancestors: Ancestor, node: ColumnRef):
        component_names = ComponentName.values()

        if len(node.fields) > len(component_names):
            raise ValueError("Too many values in column reference.")

        component_names = ComponentName.values()
        args = node.fields[: len(component_names)]
        path = Path.from_args(args)

        field = (
            node.fields[len(component_names)] if len(node.fields) > len(component_names) else None
        )
        # FIXME: when field is None then val
        field = A_Star if isinstance(field, A_StarNode) else field.val

        target_ref = FieldRef(
            path=path,
            name=field,
        )

        self.field_refs.append(target_ref)


class FieldVisitor(Visitor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field_ref_visitor = FieldRefVisitor()

    def __call__(self, node: Node) -> List[Field]:
        self.targets = []
        super().__call__(node)
        return self.targets

    def visit_ResTarget(self, ancestors: Ancestor, node: ResTarget):
        target_refs = self.field_ref_visitor(node)

        self.targets.append(
            Field(
                alias=node.name,
                depends_on=target_refs,
            )
        )

        return Skip


class SourceVisitor(Visitor):
    def __call__(self, node: Node) -> List[Source]:
        self.sources = []
        super().__call__(node)
        return self.sources

    def visit_RangeVar(self, ancestors: Ancestor, node: RangeVar):
        self.sources.append(
            Source(
                path=Path(
                    database=node.catalogname, schema=node.schemaname, identifier=node.relname
                ),
                alias=node.alias.aliasname if node.alias else None,
            )
        )

        return Skip


class CTEVisitor(Visitor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field_visitor = FieldVisitor()
        self.source_visitor = SourceVisitor()

    def __call__(self, node: Node) -> List[CTE]:
        self.ctes = []
        super().__call__(node)
        return self.ctes

    def visit_CommonTableExpr(self, ancestors: Ancestor, node: CommonTableExpr):
        ctequery = node.ctequery
        fields = self.field_visitor(ctequery.targetList)
        sources = self.source_visitor(ctequery.fromClause)

        self.ctes.append(
            CTE(
                name=node.ctename,
                fields=fields,
                sources=sources,
            )
        )

        return Skip


class RootVisitor(Visitor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field_visitor = FieldVisitor()
        self.source_visitor = SourceVisitor()

    def __call__(self, node: Node) -> Root:
        self.root = None
        super().__call__(node)

        if self.root is None:
            raise RootNotFoundException()

        return self.root

    def visit_SelectStmt(self, ancestors: Ancestor, node: SelectStmt):
        fields = self.field_visitor(node.targetList)
        sources = self.source_visitor(node.fromClause)

        self.root = Root(
            fields=fields,
            sources=sources,
        )

        return Skip


def parse(sql: str) -> Tuple[Root, List[CTE]]:
    parsed_sql = parse_sql(sql)
    stmt = parsed_sql[0].stmt

    return RootVisitor()(stmt), CTEVisitor()(stmt.withClause)
