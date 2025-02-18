from abc import ABC, abstractmethod

class AbstractSchemaProvider(ABC):

    def __init__(self):
        pass

    @classmethod
    def factory(cls, class_name, *args, **kwargs):
        for subclass in cls.__subclasses__():
            if subclass.__name__ == class_name:
                return subclass(*args, **kwargs)
        raise ValueError(f"Class {class_name} not found.")

    @abstractmethod
    def getSchema(self):
        pass