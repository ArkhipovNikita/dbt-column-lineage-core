from typing import Dict, List

from dbt.clients.system import write_json

# name of a model: list of colum names
ColumnLineage = Dict[str, List[str]]
# name of a column: name of a model: list of colum names
ColumnsLineage = Dict[str, ColumnLineage]


class ModelsColumnsLineage(Dict[str, ColumnLineage]):
    def write(self, path: str):
        write_json(path, self)
