import re
from operator import attrgetter
from typing import List, Tuple

from dbt.adapters.base import BaseRelation as DBTRelation
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.compiled import CompiledModelNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.relation import ComponentName
from dbt.contracts.relation import Path as DBTPath
from dbt.node_types import NodeType
from dbt_column_lineage.dbt.schemas.lineage import ColumnLineage, ColumnsLineage, Source
from dbt_column_lineage.parser.main import resolve_columns_lineage
from dbt_column_lineage.parser.schemas.relation import Path, Relation


def get_node_columns_lineage(
    adapter: SQLAdapter,
    manifest: Manifest,
    node: CompiledModelNode,
) -> ColumnsLineage:
    dbt_columns_lineage = []
    depends_on_models = list(
        filter(
            lambda n: n.resource_type == NodeType.Model,
            map(lambda n: manifest.nodes[n], node.depends_on_nodes),
        )
    )

    if not depends_on_models:
        dbt_relation = _get_dbt_relation_from_node(node)
        column_names = get_dbt_relation_columns(adapter, dbt_relation)
        dbt_columns_lineage.extend(
            [ColumnLineage(name=column_name) for column_name in column_names]
        )
        return dbt_columns_lineage

    initial_relations = []
    for depends_on_model in depends_on_models:
        initial_relations.append(_get_relation_from_node(adapter, depends_on_model))

    columns_lineage = resolve_columns_lineage(node.compiled_sql, initial_relations)

    # replace relation with model unique_id
    relation_model_map = dict(zip(initial_relations, depends_on_models))

    for column_name, column_lineage in columns_lineage.items():
        sources = []

        # get sources
        for relation, columns in column_lineage.lineage.items():
            model = relation_model_map[relation]
            source = Source(
                name=model.unique_id,
                columns=columns,
            )
            sources.append(source)

        dbt_column_lineage = ColumnLineage(
            name=column_name,
            formula=column_lineage.formula,
            sources=sources,
        )
        dbt_columns_lineage.append(dbt_column_lineage)

    return dbt_columns_lineage


def _get_relation_from_node(adapter: SQLAdapter, node: CompiledModelNode) -> Relation:
    dbt_relation = _get_dbt_relation_from_node(node)
    field_names = get_dbt_relation_columns(adapter, dbt_relation)

    vals = _get_node_relation_name_vals(node)
    path = _get_path_from_vals(vals)

    relation = Relation(path=path, field_names=field_names)

    return relation


def _get_node_relation_name_vals(node: CompiledModelNode) -> List[str]:
    return re.findall('[^".]+', node.relation_name)


def _get_dbt_relation_from_node(node: CompiledModelNode) -> DBTRelation:
    vals = _get_node_relation_name_vals(node)
    dbt_path = _get_dbt_path_from_vals(vals)
    return DBTRelation(path=dbt_path)


def get_dbt_relation_columns(adapter: SQLAdapter, dbt_relation: DBTRelation) -> Tuple[str, ...]:
    # TODO: cache got relations

    with adapter.connection_named("master"):
        columns = adapter.get_columns_in_relation(dbt_relation)

    column_names = tuple(map(attrgetter("name"), columns))

    return column_names


def _get_dbt_path_from_vals(vals: List[str]) -> DBTPath:
    # TODO: check for vals length ?
    component_names = list(map(str, ComponentName))
    components = dict(zip(component_names[-len(vals) :], vals))
    return DBTPath(**components)


def _get_path_from_vals(vals: List[str]) -> Path:
    return Path.from_args(vals)
