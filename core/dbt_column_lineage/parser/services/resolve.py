from dataclasses import replace
from operator import attrgetter
from typing import Dict, Iterable, List, Union

import networkx as nx
from dbt_column_lineage.parser.exceptions import (
    SourceNotFoundException,
    SourceReferenceNotFoundException,
)
from dbt_column_lineage.parser.schemas.parsed import (
    CTE,
    Field,
    FieldRef,
    Root,
    Source,
    Statement,
)
from dbt_column_lineage.parser.schemas.relation import Path, Relation


class SourcesResolver:
    def __call__(
        self,
        initial_relations: Iterable[Relation],
        statements: Iterable[Statement],
    ):
        self.statements = statements

        self.cte_map: Dict[str, CTE] = {
            statement.name: statement for statement in statements if isinstance(statement, CTE)
        }
        self.initial_relations_map: Dict[Path, Relation] = {
            relation.path: relation for relation in initial_relations
        }

        self.resolve_sources()

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


class StatementsSorter:
    def __call__(self, statements: Iterable[Statement]) -> Iterable[Statement]:
        self.root_name = ""
        self.statements = statements

        self.statements_map: Dict[str, Union[Statement]] = {
            statement.name if isinstance(statement, CTE) else self.root_name: statement
            for statement in statements
        }

        return self.sort_statements()

    def sort_statements(self) -> Iterable[Statement]:
        # build dag
        dag = nx.DiGraph()

        for statement in self.statements:
            references = map(attrgetter("reference"), statement.sources)
            references = filter(
                lambda reference: isinstance(reference, CTE),
                references,
            )

            for reference in references:
                dag.add_edge(
                    reference.name,
                    statement.name if isinstance(statement, CTE) else self.root_name,
                )

        # get topological order
        nodes = nx.topological_sort(dag)
        return map(lambda node: self.statements_map[node], nodes)


class FieldResolver:
    def __call__(self, statement: Statement):
        self.statement = statement
        self.source_map: Dict[Path, Source] = {
            source.search_path: source for source in statement.sources
        }

        self.resolve_normal_fields()
        self.resolve_a_star_fields()
        self.resolve_formulas()

    def resolve_a_star_fields(self):
        fields = filter(attrgetter("is_a_star"), self.statement.fields)

        for field in fields:
            self.statement.fields.remove(field)
            self.statement.fields.extend(self.get_a_star_fields(field))

    def get_a_star_fields(self, field: Field) -> List[Field]:
        sources = (
            self.statement.sources
            if field.depends_on[0].path.is_empty
            else [self.source_map[field.depends_on[0].path]]
        )
        fields = []

        for source in sources:
            field_names = (
                source.reference.field_names
                if isinstance(source.reference, Relation)
                else map(attrgetter("name"), source.reference.fields)
            )
            fields.extend(
                [
                    Field(
                        depends_on=[
                            FieldRef(
                                path=replace(source.search_path),
                                name=field_name,
                                source=source,
                            ),
                        ],
                        formula="{}",
                    )
                    for field_name in field_names
                ]
            )

        return fields

    def resolve_normal_fields(self):
        fields = filter(lambda f: not f.is_a_star, self.statement.fields)

        for field in fields:
            for field_ref in field.depends_on:
                field_ref.source = self.get_field_ref_source(field_ref)

    def get_field_ref_source(self, field_ref: FieldRef) -> Source:
        if not field_ref.path.is_empty:
            return self.source_map[field_ref.path]

        for source in self.statement.sources:
            if source.reference.has_field(field_ref.name):
                return source

        raise SourceNotFoundException()

    def resolve_formulas(self):
        for field in self.statement.fields:
            formulas = []

            for field_ref in field.depends_on:
                reference = field_ref.source.reference

                if isinstance(reference, Relation):
                    formulas.append(field_ref.name)
                    continue

                field_ = reference.get_field(field_ref.name)
                formulas.append(field_.formula)

            field.formula = field.formula.format(*formulas)


class FieldsResolver:
    def __init__(self):
        self.field_resolver = FieldResolver()

    def __call__(self, statements: Iterable[Statement]):
        for statement in statements:
            self.field_resolver(statement)


def resolve(root: Root, ctes: List[CTE], initial_relations: Iterable[Relation]):
    statements = [root, *ctes]

    SourcesResolver()(initial_relations, statements)
    statements = StatementsSorter()(statements)
    FieldsResolver()(statements)
