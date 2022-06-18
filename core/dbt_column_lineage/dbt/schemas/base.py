from dbt.dataclass_schema import dbtClassMixin
from mashumaro.types import SerializableType


class dbtIntegrationMixin(dbtClassMixin, SerializableType):
    def _serialize(self):
        return self.to_dict()

    @classmethod
    def _deserialize(cls, value):
        return cls.from_dict(value)
