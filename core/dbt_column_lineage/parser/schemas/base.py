class FieldSearchMixin:
    def has_field(self, name: str) -> bool:
        raise NotImplementedError
