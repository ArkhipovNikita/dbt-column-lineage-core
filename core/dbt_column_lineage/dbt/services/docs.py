from itertools import count
from operator import attrgetter
from typing import Dict, Iterator

from dbt_column_lineage.dbt.schemas.lineage import ColumnLineage, ModelsColumnsLineage
from graphviz import Digraph


def _has_many_columns(column_lineage: ColumnLineage) -> bool:
    cnt = 0

    for source in column_lineage.sources:
        cnt += len(source.columns)
        if cnt > 1:
            return True

    return False


def _setup_graph() -> Digraph:
    g = Digraph("G")

    g.graph_attr["rankdir"] = "LR"
    g.graph_attr["ranksep"] = "3"
    g.graph_attr["ratio"] = "auto"
    g.edge_attr["arrowtail"] = "dot"
    g.edge_attr["arrowsize"] = "0.5"

    return g


def _get_model_column_index_map(
    models_columns_lineage: ModelsColumnsLineage,
    index: Iterator[str],
) -> Dict[str, Dict[str, str]]:
    return {
        model_columns_lineage.name: {
            column.name: next(index) for column in model_columns_lineage.columns
        }
        for model_columns_lineage in models_columns_lineage.models
    }


def _init_clusters(
    g: Digraph,
    models_columns_lineage: ModelsColumnsLineage,
    model_column_index_map: Dict[str, Dict[str, str]],
):
    for model_columns_lineage in models_columns_lineage.models:
        model_name = model_columns_lineage.name
        columns_lineage = model_columns_lineage.columns

        column_names = map(attrgetter("name"), columns_lineage)
        column_index_map = model_column_index_map[model_name]

        with g.subgraph(name="cluster_{}".format(model_name)) as c:
            c.attr(rank="same")
            c.attr(color="black")
            c.node_attr.update(style="filled", color="lightgrey")

            for column_name in column_names:
                c.node(name=column_index_map[column_name], label=column_name)

            c.attr(label=model_name, fontname="times bold")


def _init_edges(
    g: Digraph,
    models_columns_lineage: ModelsColumnsLineage,
    model_column_index_map: Dict[str, Dict[str, str]],
    index: Iterator[str],
):
    for model_columns_lineage in models_columns_lineage.models:
        model_name = model_columns_lineage.name
        columns_lineage = model_columns_lineage.columns

        column_index_map = model_column_index_map[model_name]

        for column_lineage in columns_lineage:
            current_node = column_index_map[column_lineage.name]
            # replace column_name with empty string
            formula = (
                "" if column_lineage.formula == column_lineage.name else column_lineage.formula
            )

            has_many_columns = _has_many_columns(column_lineage)

            if has_many_columns:
                intermediate_node = next(index)
                g.node(intermediate_node, shape="point")
                target_node = intermediate_node
                label = ""
            else:
                target_node = current_node
                label = formula

            for source in column_lineage.sources:
                source_column_index_map = model_column_index_map[source.name]
                for column in source.columns:
                    g.edge(source_column_index_map[column], target_node, dir="back", label=label)

            if has_many_columns:
                g.edge(intermediate_node, current_node, dir="none", label=formula)


def draw_lineage(
    models_columns_lineage: ModelsColumnsLineage,
    filename: str,
    directory: str,
):
    g = _setup_graph()

    index = map(str, count(1, 1))
    model_column_index_map = _get_model_column_index_map(models_columns_lineage, index)

    _init_clusters(g, models_columns_lineage, model_column_index_map)
    _init_edges(g, models_columns_lineage, model_column_index_map, index)

    g.render(filename, directory, cleanup=True)
