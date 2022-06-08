import re
from operator import attrgetter
from typing import List

from dbt.adapters.base import BaseRelation as DBTRelation
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.compiled import CompiledModelNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.relation import ComponentName
from dbt.contracts.relation import Path as DBTPath
from dbt.node_types import NodeType
from dbt_column_lineage.dbt.schemas.lineage import ColumnsLineage
from dbt_column_lineage.parser.main import resolve_columns_lineage
from dbt_column_lineage.parser.schemas.relation import Path, Relation


def get_node_columns_lineage(
    adapter: SQLAdapter,
    manifest: Manifest,
    node: CompiledModelNode,
) -> ColumnsLineage:
    depends_on_models = list(
        filter(
            lambda n: n.resource_type == NodeType.Model,
            map(lambda n: manifest.nodes[n], node.depends_on_nodes),
        )
    )

    if not depends_on_models:
        return {}

    initial_relations = []
    for depends_on_model in depends_on_models:
        initial_relations.append(_get_relation_from_node(adapter, depends_on_model))

    columns_lineage = resolve_columns_lineage(node.compiled_sql, initial_relations)

    # replace relation with model unique_id
    relation_model_map = dict(zip(initial_relations, depends_on_models))
    res = {}

    for column, column_lineage in columns_lineage.items():

        temp = {}
        for relation, columns in column_lineage.items():
            model = relation_model_map[relation]
            temp[model.unique_id] = columns

        res[column] = temp

    return res


def _get_relation_from_node(adapter: SQLAdapter, node: CompiledModelNode) -> Relation:
    # TODO: cache got relations
    vals = re.findall('[^".]+', node.relation_name)
    dbt_path = _get_dbt_path_from_vals(vals)
    dbt_relation = DBTRelation(path=dbt_path)

    with adapter.connection_named("master"):
        relation_columns = adapter.get_columns_in_relation(dbt_relation)

    path = _get_path_from_vals(vals)
    field_names = tuple(map(attrgetter("name"), relation_columns))
    relation = Relation(path=path, field_names=field_names)

    return relation


def _get_dbt_path_from_vals(vals: List[str]) -> DBTPath:
    # TODO: check for vals length ?
    component_names = list(map(str, ComponentName))
    components = dict(zip(component_names[-len(vals) :], vals))
    return DBTPath(**components)


def _get_path_from_vals(vals: List[str]) -> Path:
    return Path.from_args(*vals)
