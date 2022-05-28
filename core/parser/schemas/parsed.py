from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Union

from core.parser.schemas.base import FieldSearchMixin
from core.parser.schemas.relation import Path, Relation

A_Star = "*"


@dataclass
class FieldRef:
    path: Path
    name: str

    # resolved
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

    @cached_property
    def is_a_star(self) -> bool:
        return self.name == A_Star


@dataclass
class Source:
    # identifier must be always set
    path: Path
    alias: Optional[str] = None

    # resolved
    reference: Union["CTE", Relation] = None

    def __post_init__(self):
        if self.path.identifier is None:
            raise ValueError("Path must contain identifier at least.")

    @cached_property
    def search_path(self) -> Path:
        return Path(identifier=self.alias) if self.alias else self.path


@dataclass
class Statement(FieldSearchMixin):
    fields: List[Field]
    sources: List[Source]

    # TODO: search by index ?
    def has_field(self, name: str) -> bool:
        for field_ in self.fields:
            if field_.name == name:
                return True
        return False


@dataclass
class CTE(Statement):
    name: str


@dataclass
class Root(Statement):
    pass
