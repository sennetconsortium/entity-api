from enum import Enum


class Entities(str, Enum):
    SOURCE = 'source'
    SAMPLE = 'sample'
    DATASET = 'dataset'


class SpecimenCategory(str, Enum):
    ORGAN = 'organ'
    BLOCK = 'block'
    SECTION = 'section'
    SUSPENSION = 'suspension'


class Organs(str, Enum):
    BLOOD = 'BD'


class DataTypes(str, Enum):
    LIGHTSHEET = 'Lightsheet'


def get_entities() -> list[Entities]:
    return [Entities.SOURCE, Entities.SAMPLE, Entities.DATASET]