from dbt.exceptions import InternalException
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task.compile import CompileRunner, CompileTask
from dbt_column_lineage.dbt.schemas.graph import ParsedColumnLineageNode
from dbt_column_lineage.dbt.services.lineage import get_node_columns_lineage


class ParseColumnLineageRunner(CompileRunner):
    # FIXME: compiled node order
    def compile(self, manifest) -> ParsedColumnLineageNode:
        node = super().compile(manifest)

        columns_lineage = get_node_columns_lineage(self.adapter, manifest, node)
        data = node.to_dict(omit_none=True)
        data["columns_lineage"] = columns_lineage
        node = ParsedColumnLineageNode.from_dict(data)

        return node


class ParseColumnLineageTask(CompileTask):
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
