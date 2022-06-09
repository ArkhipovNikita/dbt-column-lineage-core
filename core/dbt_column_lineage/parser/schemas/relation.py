from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import Optional, Tuple

from dbt_column_lineage.parser.schemas.base import FieldSearchMixin


class ComponentName(Enum):
    DATABASE = "database"
    SCHEMA = "schema"
    IDENTIFIER = "identifier"

    @classmethod
    def values(cls):
        return list(map(lambda c: c.value, cls))


@dataclass(frozen=True)
class Path:
    database: Optional[str] = None
    schema: Optional[str] = None
    identifier: Optional[str] = None

    def __post_init__(self):
        self._check_intermediate_none()

    @classmethod
    def from_args(cls, args) -> "Path":
        component_names = ComponentName.values()

        if len(args) > len(component_names):
            raise ValueError("Too many components to create path.")

        components = dict(zip(component_names[-len(args) :], args))

        return cls(**components)

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
    field_names: Tuple[str, ...]

    def has_field(self, name: str) -> bool:
        return name in self.field_names


empty_path = Path()
