from typing import Dict, List

from core.parser.exceptions import SourceNotFoundException
from core.parser.schemas.parsed import CTE, Field, FieldRef, Root, Source, Statement
from core.parser.schemas.relation import Path, Relation


class Resolver:
    def __call__(
        self,
        root: Root,
        ctes: List[CTE],
        initial_relations: List[Relation],
    ):
        self.cte_map = self.get_cte_map(ctes)
        self.initial_relations_map = self.get_relation_map(initial_relations)

        statements: List[Statement] = [root, *ctes]
        for statement in statements:
            self.resolve(statement.fields, statement.sources)

    def resolve(self, fields: List[Field], sources: List[Source]):
        source_map = self.get_source_map(sources)

        for field in fields:
            for field_ref in field.depends_on:
                field_ref.source = self.get_field_ref_source(field_ref, sources, source_map)

    def get_field_ref_source(
        self, field_ref: FieldRef, sources: List[Source], source_map: Dict[Path, Source]
    ) -> Source:
        if not field_ref.path.is_empty:
            return source_map[field_ref.path]

        for source in sources:
            cte = self.cte_map.get(source.path.identifier)

            if cte and cte.has_field(field_ref.name):
                return source

            initial_relation = self.initial_relations_map.get(source.path)
            if initial_relation and initial_relation.has_field(field_ref.name):
                return source

        raise SourceNotFoundException()

    def get_source_map(self, sources: List[Source]) -> Dict[Path, Source]:
        return {source.search_path: source for source in sources}

    def get_relation_map(self, relations: List[Relation]) -> Dict[Path, Relation]:
        return {relation.path: relation for relation in relations}

    def get_cte_map(self, ctes: List[CTE]) -> Dict[str, CTE]:
        return {cte.name: cte for cte in ctes}


resolve = Resolver()
