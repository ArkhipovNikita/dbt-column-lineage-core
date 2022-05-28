from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Set

from core.parser.schemas.base import FieldSearchMixin


@dataclass(frozen=True)
class Path:
    database: Optional[str] = None
    schema: Optional[str] = None
    identifier: Optional[str] = None

    def __post_init__(self):
        self._check_intermediate_none()

    @cached_property
    def is_empty(self) -> bool:
        return self == empty_path

    def _check_intermediate_none(self):
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


@dataclass(frozen=True)
class Relation(FieldSearchMixin):
    path: Path
    field_names: Set[str]

    def has_field(self, name: str) -> bool:
        return name in self.field_names


empty_path = Path()
