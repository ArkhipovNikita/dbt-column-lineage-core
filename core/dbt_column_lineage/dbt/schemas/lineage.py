from typing import List, Mapping

# name of a model: list of colum names
ColumnLineage = Mapping[str, List[str]]
# name of a column: name of a model: list of colum names
ColumnsLineage = Mapping[str, ColumnLineage]
