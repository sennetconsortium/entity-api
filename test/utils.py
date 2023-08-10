from dataclasses import dataclass, fields
from atlas_consortia_commons.object import enum_val_lower


@dataclass
class SpecimenCategories:
    BLOCK: str = 'Block'
    ORGAN: str = 'Organ'
    SECTION: str = 'Section'
    SUSPENSION: str = 'Suspension'


@dataclass
class Entities:
    DATASET: str = 'Dataset'
    PUBLICATION_ENTITY: str = 'Publication Entity'
    SAMPLE: str = 'Sample'
    SOURCE: str = 'Source'


@dataclass
class SourceTypes:
    MOUSE: str = 'Mouse'
    HUMAN: str = 'Human'
    HUMAN_ORGANOID: str = 'Human Organoid'
    MOUSE_ORGANOID: str = 'Mouse Organoid'


@dataclass
class AssayTypes:
    BULKRNA: str = "bulk-RNA" 
    CITESEQ: str = "CITE-Seq"
    CODEX: str = "CODEX"
    CODEXCYTOKIT: str = "codex_cytokit"
    CODEXCYTOKITV1: str = "codex_cytokit_v1"
    COSMX_RNA: str = "CosMX(RNA)"
    DBITSEQ: str = "DBiT-seq"
    FACS__FLUORESCENCEACTIVATED_CELL_SORTING: str = "FACS-Fluorescence-activatedCellSorting"
    GEOMX_RNA: str = "GeoMX(RNA)"
    IMAGEPYRAMID: str = "image_pyramid"
    LCMS: str = "LC-MS"
    LIGHTSHEET: str = "Lightsheet"
    MIBI: str = "MIBI"
    MIBIDEEPCELL: str = "mibi_deepcell"
    MINTCHIP: str = "Mint-ChIP"
    PUBLICATION: str = "publication"
    PUBLICATIONANCILLARY: str = "publication_ancillary"
    SALMONRNASEQ10X: str = "salmon_rnaseq_10x"
    SALMONRNASEQBULK: str = "salmon_rnaseq_bulk"
    SALMONSNRNASEQ10X: str = "salmon_sn_rnaseq_10x"
    SASP: str = "SASP"
    SCRNASEQ: str = "scRNA-seq"
    SNATACSEQ: str = "snATAC-seq"
    SNRNASEQ: str = "snRNA-seq"
    SNRNASEQ10XGENOMICSV3: str = "snRNAseq-10xGenomics-v3"
    STAINED_SLIDES: str = "StainedSlides"
    VISIUM: str = "Visium"


def entities(as_arr: bool = False, cb=str, as_data_dict: bool = False):
    if as_arr and cb == enum_val_lower:
        return [e.default.lower() for e in fields(Entities)]
    if as_arr and cb == str:
        return [e.default for e in fields(Entities)]
    if as_data_dict:
        return {e.name: e.default for e in fields(Entities)}
    return Entities
    
def specimen_categories(as_arr: bool = False, cb=str, as_data_dict: bool = False):
    if as_arr and cb == enum_val_lower:
        return [e.default.lower() for e in fields(SpecimenCategories)]
    if as_arr and cb == str:
        return [e.default for e in fields(SpecimenCategories)]
    if as_data_dict:
        return {e.name: e.default for e in fields(SpecimenCategories)}
    return SpecimenCategories

def source_types(as_arr: bool = False, cb=str, as_data_dict: bool = False):
    if as_arr and cb == enum_val_lower:
        return [e.default.lower() for e in fields(SourceTypes)]
    if as_arr and cb == str:
        return [e.default for e in fields(SourceTypes)]
    if as_data_dict:
        return {e.name: e.default for e in fields(SourceTypes)}
    return SourceTypes

def assay_types(as_arr: bool = False, cb=str, as_data_dict: bool = False):
    if as_arr and cb == enum_val_lower:
        return [e.default.lower() for e in fields(AssayTypes)]
    if as_arr and cb == str:
        return [e.default for e in fields(AssayTypes)]
    if as_data_dict:
        return {e.name: e.default for e in fields(AssayTypes)}
    return AssayTypes