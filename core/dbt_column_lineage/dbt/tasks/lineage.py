import os.path
from typing import Optional

from dbt.clients.system import read_json
from dbt.task.base import ConfiguredTask
from dbt_column_lineage.dbt.paths import get_column_lineage_manifest_path
from dbt_column_lineage.dbt.schemas.lineage import ModelsColumnsLineage


class LineageTask(ConfiguredTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.lineage: Optional[ModelsColumnsLineage] = None

    def write_lineage(self):
        path = get_column_lineage_manifest_path(self.config)
        self.lineage.write(path)

    def load_lineage(self):
        path = get_column_lineage_manifest_path(self.config)

        if not os.path.exists(path):
            return

        data = read_json(path)
        self.lineage = ModelsColumnsLineage.from_dict(data)

    def _runtime_initialize(self):
        self.load_lineage()
