from dataclasses import dataclass, field
from functools import cached_property
from typing import List, Optional


@dataclass(frozen=True)
class Path:
    database: Optional[str] = None
    schema: Optional[str] = None
    identifier: Optional[str] = None

    def __post_init__(self):
        components = (
            self.identifier,
            self.schema,
            self.database,
        )

        is_none_set = False

        for component in components:
            if component is None:
                is_none_set = True
            elif is_none_set:
                raise ValueError("There cannot be intermediate None value.")

    @cached_property
    def is_empty(self) -> bool:
        return self == empty_path


@dataclass
class Relation:
    path: Path
    field_names: List[str]

    _field_names: set[str] = field(init=False)

    def __post_init__(self):
        self._field_names = set(self.field_names)

    def has_field(self, name: str) -> bool:
        return name in self._field_names


empty_path = Path()
