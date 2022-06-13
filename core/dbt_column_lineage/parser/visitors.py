from abc import ABC
from typing import Generic, List, Type, TypeVar

from pglast.ast import ColumnRef, CommonTableExpr, Node, RangeVar, ResTarget, SelectStmt
from pglast.visitors import Skip, Visitor

NodeType = TypeVar("NodeType", bound=Node)


class NodeVisitor(Generic[NodeType], ABC, Visitor):
    def __init__(self, flat: bool = False):
        super().__init__()

        self.flat = flat

    def __call__(self, node: Node) -> List[NodeType]:
        self.nodes = []
        super().__call__(node)

        return self.nodes


def make_node_visitor(node_type: Type[Node]) -> Type[NodeVisitor]:
    def visit_func(self, ancestors, node):
        self.nodes.append(node)

        if self.flat:
            return Skip

    node_type_name = node_type.__name__
    cls_name = "{}Visitor".format(node_type_name)

    visit_func_name = "visit_{}".format(node_type_name)
    visit_func.__name__ = visit_func_name

    # TODO: add NodeVisitor[node_type],
    cls = type(cls_name, (NodeVisitor,), {visit_func_name: visit_func})

    return cls


ColumnRefVisitor = make_node_visitor(ColumnRef)
ResTargetVisitor = make_node_visitor(ResTarget)
RangeVarVisitor = make_node_visitor(RangeVar)
SelectStmtVisitor = make_node_visitor(SelectStmt)
CommonTableExprVisitor = make_node_visitor(CommonTableExpr)
