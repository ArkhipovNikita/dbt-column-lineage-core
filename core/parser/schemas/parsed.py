from dataclasses import dataclass, field
from functools import cached_property
from typing import List, Optional

from core.parser.schemas.relation import Path


@dataclass
class FieldRef:
    path: Path
    name: str
    source: Optional["Source"] = None


@dataclass
class Field:
    depends_on: List[FieldRef]
    alias: Optional[str] = None

    @cached_property
    def name(self) -> str:
        if self.alias:
            return self.alias

        # if a name doesn't have specified alias
        # then it must be depended on only one name
        if len(self.depends_on) != 1:
            raise ValueError("Field must have a alias.")

        return self.depends_on[0].name


@dataclass
class Source:
    # identifier must be always set
    path: Path
    alias: Optional[str] = None

    def __post_init__(self):
        if self.path.identifier is None:
            raise ValueError("Path must contain identifier at least.")

    @cached_property
    def search_path(self) -> Path:
        return Path(identifier=self.alias) if self.alias else self.path


@dataclass
class Statement:
    fields: List[Field]
    sources: List[Source]

    _field_names: set[str] = field(init=False)

    def __post_init__(self):
        self._field_names = set(field_.name for field_ in self.fields)

    def has_field(self, name: str) -> bool:
        return name in self._field_names


@dataclass
class CTE(Statement):
    name: str


@dataclass
class Root(Statement):
    pass
