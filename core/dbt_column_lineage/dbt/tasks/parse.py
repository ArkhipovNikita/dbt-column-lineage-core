from operator import attrgetter

from dbt.exceptions import InternalException
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task.compile import CompileRunner, CompileTask
from dbt_column_lineage.dbt.schemas.graph import ParsedColumnLineageNode
from dbt_column_lineage.dbt.schemas.lineage import (
    ModelColumnsLineage,
    ModelsColumnsLineage,
)
from dbt_column_lineage.dbt.services.lineage import get_node_columns_lineage
from dbt_column_lineage.dbt.tasks.lineage import LineageTask


class ParseColumnLineageRunner(CompileRunner):
    # FIXME: compiled node order
    def compile(self, manifest) -> ParsedColumnLineageNode:
        node = super().compile(manifest)

        columns_lineage = get_node_columns_lineage(self.adapter, manifest, node)
        data = node.to_dict(omit_none=True)
        data["columns_lineage"] = [
            column_lineage.to_dict(omit_none=True) for column_lineage in columns_lineage
        ]
        node = ParsedColumnLineageNode.from_dict(data)

        return node


class ParseColumnLineageTask(CompileTask, LineageTask):
    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Model],
        )

    def get_runner_type(self, _):
        return ParseColumnLineageRunner

    def run(self) -> ModelsColumnsLineage:
        result = super().run()
        nodes = map(attrgetter("node"), result.results)

        models_columns_lineage = [
            ModelColumnsLineage(
                name=node.unique_id,
                columns=node.columns_lineage,
            )
            for node in nodes
        ]
        models_columns_lineage = ModelsColumnsLineage(models=models_columns_lineage)
        self.lineage = models_columns_lineage
        self.write_lineage()

        return models_columns_lineage
