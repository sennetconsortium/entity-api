from enum import Enum


class Entities:
    SOURCE = 'Source'
    SAMPLE = 'Sample'
    DATASET = 'Dataset'


class SpecimenCategory:
    ORGAN = 'organ'
    BLOCK = 'block'
    SECTION = 'section'
    SUSPENSION = 'suspension'


class Organs:
    BLOOD = 'BD'


class DataTypes:
    LIGHTSHEET = 'Lightsheet'


def get_entities() -> list[Entities]:
    return [Entities.SOURCE, Entities.SAMPLE, Entities.DATASET]