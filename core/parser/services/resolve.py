from operator import attrgetter
from typing import Dict, List, Iterable, Union, Mapping

import networkx as nx

from core.parser.exceptions import SourceNotFoundException, SourceReferenceNotFoundException
from core.parser.schemas.parsed import CTE, Field, FieldRef, Root, Source, Statement
from core.parser.schemas.relation import Path, Relation, empty_path


class Resolver:
    def __call__(
        self,
        root: Root,
        ctes: List[CTE],
        initial_relations: List[Relation],
    ):
        self.root = root
        self.statements: List[Statement] = [root, *ctes]

        self.cte_map: Dict[str, CTE] = {cte.name: cte for cte in ctes}
        self.initial_relations_map: Dict[Path, Relation] = {
            relation.path: relation
            for relation in initial_relations
        }

        self.resolve_sources()
        self.sort_statements()
        self.resolve_fields()

    def resolve_sources(self):
        for statement in self.statements:
            for source in statement.sources:
                source.reference = self.get_source_reference(source)

    def get_source_reference(self, source: Source) -> Union[CTE, Relation]:
        cte = self.cte_map.get(source.path.identifier)
        if cte:
            return cte
        else:
            initial_relation = self.initial_relations_map.get(source.path)
            if initial_relation:
                return initial_relation

        raise SourceReferenceNotFoundException()

    def sort_statements(self):
        root_name = ''
        statements_map = {root_name: self.root, **self.cte_map}

        # build dag
        dag = nx.DiGraph()

        for statement in self.statements:
            references = map(attrgetter('reference'), statement.sources)
            references = filter(
                lambda reference: isinstance(reference, CTE),
                references,
            )

            for reference in references:
                dag.add_edge(
                    reference.name,
                    statement.name if isinstance(statement, CTE) else root_name,
                )

        # get topological order
        nodes = nx.topological_sort(dag)
        self.statements = [statements_map[node] for node in nodes]

    def resolve_fields(self):
        for statement in self.statements:
            source_map = self.get_source_map(statement.sources)
            a_star_fields = []
            new_fields = []

            for field in statement.fields:
                if field.is_a_star:
                    a_star_fields.append(field)
                    new_fields.extend(self.get_a_star_fields(field, statement.sources, source_map))
                    continue

                for field_ref in field.depends_on:
                    field_ref.source = self.get_field_ref_source(
                        field_ref, statement.sources, source_map
                    )

            statement.fields = [field for field in statement.fields if field not in a_star_fields]
            statement.fields.extend(new_fields)

    def get_a_star_fields(
        self,
        field: Field,
        sources: Iterable[Source],
        source_map: Mapping[Path, Source],
    ) -> List[Field]:
        if field.depends_on[0].path.is_empty:
            sources = sources
        else:
            sources = source_map[field.depends_on[0].path]

        fields = []
        for source in sources:
            field_names = (
                source.reference.field_names
                if isinstance(source.reference, Relation)
                else map(attrgetter('name'), source.reference.fields)
            )
            fields.extend(
                [
                    Field(depends_on=[FieldRef(path=empty_path, name=field_name, source=source)])
                    for field_name in field_names
                ]
            )

        return fields

    def get_field_ref_source(
        self,
        field_ref: FieldRef,
        sources: Iterable[Source],
        source_map: Mapping[Path, Source],
    ) -> Source:
        if not field_ref.path.is_empty:
            return source_map[field_ref.path]

        for source in sources:
            if source.reference.has_field(field_ref.name):
                return source

        raise SourceNotFoundException()

    def get_source_map(self, sources: Iterable[Source]) -> Dict[Path, Source]:
        return {source.search_path: source for source in sources}


resolve = Resolver()
