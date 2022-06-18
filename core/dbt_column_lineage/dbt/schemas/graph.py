from dataclasses import dataclass, field

from dbt.contracts.graph.compiled import CompiledModelNode
from dbt_column_lineage.dbt.schemas.lineage import ColumnsLineage


@dataclass
class ParsedColumnLineageNode(CompiledModelNode):
    columns_lineage: ColumnsLineage = field(default_factory=list)
