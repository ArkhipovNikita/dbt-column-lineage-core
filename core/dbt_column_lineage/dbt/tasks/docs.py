from dbt.exceptions import InternalException
from dbt_column_lineage.dbt.consts import COLUMN_LINEAGE_DOCS_FILENAME
from dbt_column_lineage.dbt.paths import get_column_lineage_directory
from dbt_column_lineage.dbt.services.docs import draw_lineage
from dbt_column_lineage.dbt.tasks.lineage import LineageTask


class DocsTask(LineageTask):
    def run(self):
        self._runtime_initialize()

        if not self.lineage:
            raise InternalException("Initially column lineage manifest must be created.")

        filename = COLUMN_LINEAGE_DOCS_FILENAME
        directory = get_column_lineage_directory(self.config)
        draw_lineage(self.lineage, filename, directory)
