from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Union

from dbt_column_lineage.parser.schemas.base import FieldSearchMixin
from dbt_column_lineage.parser.schemas.relation import Path, Relation
from dbt_column_lineage.parser.schemas.token import TokenList

A_Star = "*"


@dataclass(frozen=True)
class NodeSQL:
    # must be lowered
    sql: str
    tokens: TokenList
    # global indexes
    start_idx: int
    end_idx: int

    @cached_property
    def tokens_area(self) -> TokenList:
        return self.tokens.real_slice(self.start_idx, self.end_idx + 1)


@dataclass
class FieldRef:
    path: Path
    name: str

    # resolved
    source: Optional["Source"] = None

    def __str__(self):
        path = self.path
        components = (path.database, path.schema, path.identifier, self.name)
        components = filter(None, components)
        return ".".join(components)


@dataclass
class Field:
    depends_on: List[FieldRef]
    alias: Optional[str] = None

    formula: Optional[str] = None

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

    # TODO: get by index ?
    def get_field(self, name: str) -> Optional[Field]:
        for field_ in self.fields:
            if field_.name == name:
                return field_
        return None

    def has_field(self, name: str) -> bool:
        return bool(self.get_field(name))


@dataclass
class CTE(Statement):
    name: str


@dataclass
class Root(Statement):
    pass
