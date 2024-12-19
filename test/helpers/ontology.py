# from dataclasses import dataclass, fields
# from unittest.mock import patch
#
# import pytest
# from atlas_consortia_commons.object import enum_val_lower
#
# from lib.ontology import Ontology
#
#
# @pytest.fixture(scope="session")
# def ontology():
#     """Automatically add ontology mock functions to all tests"""
#     with patch("atlas_consortia_commons.ubkg.ubkg_sdk.UbkgSDK", new=MockOntology):
#         yield
#
#
# class Ubkgock:
#     def get_ubkg(self, node, key: str = 'VALUESET', endpoint: str = None):
#
#
# @dataclass
# class SpecimenCategories:
#     BLOCK: str = "Block"
#     ORGAN: str = "Organ"
#     SECTION: str = "Section"
#     SUSPENSION: str = "Suspension"
#
#
# @dataclass
# class Entities:
#     DATASET: str = "Dataset"
#     PUBLICATION_ENTITY: str = "Publication Entity"
#     SAMPLE: str = "Sample"
#     SOURCE: str = "Source"
#
#
# @dataclass
# class SourceTypes:
#     MOUSE: str = "Mouse"
#     HUMAN: str = "Human"
#     HUMAN_ORGANOID: str = "Human Organoid"
#     MOUSE_ORGANOID: str = "Mouse Organoid"
#
#
# @dataclass
# class OrganTypes:
#     AD: str = "AD" BD: str = "BD"
#     BM: str = "BM"
#     BR: str = "BR"
#     BS: str = "BS"
#     LI: str = "LI"
#     LK: str = "LK"
#     LL: str = "LL"
#     LN: str = "LN"
#     LO: str = "LO"
#     LV: str = "LV"
#     MU: str = "MU"
#     OT: str = "OT"
#     PA: str = "PA"
#     PL: str = "PL"
#     RK: str = "RK"
#     RL: str = "RL"
#     RO: str = "RO"
#     SK: str = "SK"
#
#
# @dataclass
# class AssayTypes:
#     BULKRNA: str = "bulk-RNA"
#     CITESEQ: str = "CITE-Seq"
#     CODEX: str = "CODEX"
#     CODEXCYTOKIT: str = "codex_cytokit"
#     CODEXCYTOKITV1: str = "codex_cytokit_v1"
#     COSMX_RNA: str = "CosMX(RNA)"
#     DBITSEQ: str = "DBiT-seq"
#     FACS__FLUORESCENCEACTIVATED_CELL_SORTING: str = "FACS-Fluorescence-activatedCellSorting"
#     GEOMX_RNA: str = "GeoMX(RNA)"
#     IMAGEPYRAMID: str = "image_pyramid"
#     LCMS: str = "LC-MS"
#     LIGHTSHEET: str = "Lightsheet"
#     MIBI: str = "MIBI"
#     MIBIDEEPCELL: str = "mibi_deepcell"
#     MINTCHIP: str = "Mint-ChIP"
#     PUBLICATION: str = "publication"
#     PUBLICATIONANCILLARY: str = "publication_ancillary"
#     SALMONRNASEQ10X: str = "salmon_rnaseq_10x"
#     SALMONRNASEQBULK: str = "salmon_rnaseq_bulk"
#     SALMONSNRNASEQ10X: str = "salmon_sn_rnaseq_10x"
#     SASP: str = "SASP"
#     SCRNASEQ: str = "scRNA-seq"
#     SNATACSEQ: str = "snATAC-seq"
#     SNRNASEQ: str = "snRNA-seq"
#     SNRNASEQ10XGENOMICSV3: str = "snRNAseq-10xGenomics-v3"
#     STAINED_SLIDES: str = "StainedSlides"
#     VISIUM: str = "Visium"
#
#
# @dataclass
# class DatasetTypes:
#     HISTOLOGY: str = "Histology"
#     MOLECULAR_CARTOGRAPHY: str = "Molecular Cartography"
#     RNASEQ: str = "RNASeq"
#     ATACSEQ: str = "ATACSeq"
#     SNARESEQ2: str = "SNARE-seq2"
#     PHENOCYCLER: str = "PhenoCycler"
#     CYCIF: str = "CyCIF"
#     MERFISH: str = "MERFISH"
#     MALDI: str = "MALDI"
#     _2D_IMAGING_MASS_CYTOMETRY: str = "2D Imaging Mass Cytometry"
#     NANOSPLITS: str = "nanoSPLITS"
#     AUTOFLUORESCENCE: str = "Auto-fluorescence"
#     CONFOCAL: str = "Confocal"
#     THICK_SECTION_MULTIPHOTON_MXIF: str = "Thick section Multiphoton MxIF"
#     SECOND_HARMONIC_GENERATION_SHG: str = "Second Harmonic Generation (SHG)"
#     ENHANCED_STIMULATED_RAMAN_SPECTROSCOPY_SRS: str = "Enhanced Stimulated Raman Spectroscopy (SRS)"
#     SIMS: str = "SIMS"
#     CELL_DIVE: str = "Cell DIVE"
#     CODEX: str = "CODEX"
#     LIGHTSHEET: str = "Lightsheet"
#     MIBI: str = "MIBI"
#     LCMS: str = "LC-MS"
#     DESI: str = "DESI"
#     _10X_MULTIOME: str = "10x Multiome"
#     VISIUM: str = "Visium"
#
#
# class MockOntology(Ontology):
#     @staticmethod
#     def entities():
#         if Ontology.Ops.as_arr and MockOntology.Ops.cb == enum_val_lower:
#             return [e.default.lower() for e in fields(Entities)]
#         if MockOntology.Ops.as_arr and MockOntology.Ops.cb == str:
#             return [e.default for e in fields(Entities)]
#         if MockOntology.Ops.as_data_dict:
#             return {e.name: e.default for e in fields(Entities)}
#         return Entities
#
#     @staticmethod
#     def specimen_categories():
#         if MockOntology.Ops.as_arr and MockOntology.Ops.cb == enum_val_lower:
#             return [e.default.lower() for e in fields(SpecimenCategories)]
#         if MockOntology.Ops.as_arr and MockOntology.Ops.cb == str:
#             return [e.default for e in fields(SpecimenCategories)]
#         if MockOntology.Ops.as_data_dict:
#             return {e.name: e.default for e in fields(SpecimenCategories)}
#         return SpecimenCategories
#
#     @staticmethod
#     def source_types():
#         if MockOntology.Ops.as_arr and MockOntology.Ops.cb == enum_val_lower:
#             return [e.default.lower() for e in fields(SourceTypes)]
#         if MockOntology.Ops.as_arr and MockOntology.Ops.cb == str:
#             return [e.default for e in fields(SourceTypes)]
#         if Ontology.Ops.as_data_dict:
#             return {e.name: e.default for e in fields(SourceTypes)}
#         return SourceTypes
#
#     @staticmethod
#     def assay_types():
#         if Ontology.Ops.as_arr and Ontology.Ops.cb == enum_val_lower:
#             return [e.default.lower() for e in fields(AssayTypes)]
#         if Ontology.Ops.as_arr and Ontology.Ops.cb == str:
#             return [e.default for e in fields(AssayTypes)]
#         if Ontology.Ops.as_data_dict:
#             return {e.name: e.default for e in fields(AssayTypes)}
#         return AssayTypes
#
#     @staticmethod
#     def organ_types():
#         if Ontology.Ops.as_arr and MockOntology.Ops.cb == enum_val_lower:
#             return [e.default.lower() for e in fields(OrganTypes)]
#         if MockOntology.Ops.as_arr and MockOntology.Ops.cb == str:
#             return [e.default for e in fields(OrganTypes)]
#         if MockOntology.Ops.as_data_dict:
#             return {e.name: e.default for e in fields(OrganTypes)}
#         return OrganTypes
#
#     @staticmethod
#     def dataset_types():
#         if Ontology.Ops.as_arr and MockOntology.Ops.cb == enum_val_lower:
#             return [e.default.lower() for e in fields(DatasetTypes)]
#         if MockOntology.Ops.as_arr and MockOntology.Ops.cb == str:
#             return [e.default for e in fields(DatasetTypes)]
#         if MockOntology.Ops.as_data_dict:
#             return {e.name.removeprefix("_"): e.default for e in fields(DatasetTypes)}
#         return DatasetTypes
#
#
# VALUES = {
#     'VALUESET_C020076': [
#         {'code': 'C000008', 'sab': 'SENNET', 'term': 'Organ'},
#         {'code': 'C020079', 'sab': 'SENNET', 'term': 'Suspension'},
#         {'code': 'C020078', 'sab': 'SENNET', 'term': 'Section'},
#         {'code': 'C020077', 'sab': 'SENNET', 'term': 'Block'}
#     ],
#     'organs_C000008': [
#         {'category': None, 'code': 'C030024', 'laterality': None, 'organ_cui': 'C0001527', 'organ_uberon': 'UBERON:0001013', 'rui_code': 'AD', 'sab': 'SENNET', 'term': 'Adipose Tissue'},
#         {'category': None, 'code': 'C030025', 'laterality': None, 'organ_cui': 'C0005767', 'organ_uberon': 'UBERON:0000178', 'rui_code': 'BD', 'sab': 'SENNET', 'term': 'Blood'},
#         {'category': None, 'code': 'C030102', 'laterality': None, 'organ_cui': 'C0262950', 'organ_uberon': 'UBERON:0001474', 'rui_code': 'BX', 'sab': 'SENNET', 'term': 'Bone'},
#         {'category': None, 'code': 'C030101', 'laterality': None, 'organ_cui': 'C0005953', 'organ_uberon': 'UBERON:0002371', 'rui_code': 'BM', 'sab': 'SENNET', 'term': 'Bone Marrow'},
#         {'category': None, 'code': 'C030026', 'laterality': None, 'organ_cui': 'C1269537', 'organ_uberon': 'UBERON:0000955', 'rui_code': 'BR', 'sab': 'SENNET', 'term': 'Brain'},
#         {'category': None, 'code': 'C030070', 'laterality': None, 'organ_cui': 'C0018787', 'organ_uberon': 'UBERON:0000948', 'rui_code': 'HT', 'sab': 'SENNET', 'term': 'Heart'},
#         {'category': {...}, 'code': 'C030029', 'laterality': 'Left', 'organ_cui': 'C0227614', 'organ_uberon': 'UBERON:0004538', 'rui_code': 'LK', 'sab': 'SENNET', 'term': 'Kidney (Left)'},
#         {'category': {...}, 'code': 'C030030', 'laterality': 'Right', 'organ_cui': 'C0227613', 'organ_uberon': 'UBERON:0004539', 'rui_code': 'RK', 'sab': 'SENNET', 'term': 'Kidney (Right)'},
#         {'category': None, 'code': 'C030031', 'laterality': None, 'organ_cui': 'C0021851', 'organ_uberon': 'UBERON:0000059', 'rui_code': 'LI', 'sab': 'SENNET', 'term': 'Large Intestine'},
#         {'category': None, 'code': 'C030032', 'laterality': None, 'organ_cui': 'C0023884', 'organ_uberon': 'UBERON:0002107', 'rui_code': 'LV', 'sab': 'SENNET', 'term': 'Liver'},
#         {'category': {...}, 'code': 'C030034', 'laterality': 'Left', 'organ_cui': 'C0225730', 'organ_uberon': 'UBERON:0002168', 'rui_code': 'LL', 'sab': 'SENNET', 'term': 'Lung (Left)'},
#         {'category': {...}, 'code': 'C030035', 'laterality': 'Right', 'organ_cui': 'C0225706', 'organ_uberon': 'UBERON:0002167', 'rui_code': 'RL', 'sab': 'SENNET', 'term': 'Lung (Right)'},
#         {'category': None, 'code': 'C030052', 'laterality': None, 'organ_cui': 'C0024204', 'organ_uberon': 'UBERON:0000029', 'rui_code': 'LY', 'sab': 'SENNET', 'term': 'Lymph Node'},
#         {'category': {...}, 'code': 'C030082', 'laterality': 'Left', 'organ_cui': 'C0222601', 'organ_uberon': 'FMA:57991', 'rui_code': 'ML', 'sab': 'SENNET', 'term': 'Mammary Gland (Left)'},
#         {'category': {...}, 'code': 'C030083', 'laterality': 'Right', 'organ_cui': 'C0222600', 'organ_uberon': 'FMA:57987', 'rui_code': 'MR', 'sab': 'SENNET', 'term': 'Mammary Gland (Right)'}, {'category': None, 'code': 'C030036', 'laterality': None, 'organ_cui': 'C4083049', 'organ_uberon': 'UBERON:0005090', 'rui_code': 'MU', 'sab': 'SENNET', 'term': 'Muscle'}, {'category': None, 'code': 'C030039', 'laterality': None, 'organ_cui': 'SENNET:C030039 CUI', 'organ_uberon': None, 'rui_code': 'OT', 'sab': 'SENNET', 'term': 'Other'}, {'category': {...}, 'code': 'C030038', 'laterality': 'Left', 'organ_cui': 'C0227874', 'organ_uberon': 'UBERON:0002119', 'rui_code': 'LO', 'sab': 'SENNET', 'term': 'Ovary (Left)'}, {'category': {...}, 'code': 'C030041', 'laterality': 'Right', 'organ_cui': 'C0227873', 'organ_uberon': 'UBERON:0002118', 'rui_code': 'RO', 'sab': 'SENNET', 'term': 'Ovary (Right)'}, {'category': None, 'code': 'C030054', 'laterality': None, 'organ_cui': 'C0030274', 'organ_uberon': 'UBERON:0001264', 'rui_code': 'PA', 'sab': 'SENNET', 'term': 'Pancreas'}, {'category': None, 'code': 'C030055', 'laterality': None, 'organ_cui': 'C0032043', 'organ_uberon': 'UBERON:0001987', 'rui_code': 'PL', 'sab': 'SENNET', 'term': 'Placenta'}, {'category': None, 'code': 'C030040', 'laterality': None, 'organ_cui': 'C1123023', 'organ_uberon': 'UBERON:0002097', 'rui_code': 'SK', 'sab': 'SENNET', 'term': 'Skin'}, {'category': None, 'code': 'C03081', 'laterality': None, 'organ_cui': 'C0037925', 'organ_uberon': 'UBERON:0002240', 'rui_code': 'SC', 'sab': 'SENNET', 'term': 'Spinal Cord'}, {'category': None, 'code': 'C030072', 'laterality': None, 'organ_cui': 'C0040113', 'organ_uberon': 'UBERON:0002370', 'rui_code': 'TH', 'sab': 'SENNET', 'term': 'Thymus'}, {'category': {...}, 'code': 'C030084', 'laterality': 'Left', 'organ_cui': 'C0229868', 'organ_uberon': 'FMA:54974', 'rui_code': 'LT', 'sab': 'SENNET', 'term': 'Tonsil (Left)'}, {'category': {...}, 'code': 'C030085', 'laterality': 'Right', 'organ_cui': 'C0229867', 'organ_uberon': 'FMA:54973', 'rui_code': 'RT', 'sab': 'SENNET', 'term': 'Tonsil (Right)'}],
#     'VALUESET_C000012': [{'code': 'C050002', 'sab': 'SENNET', 'term': 'Dataset'}, {'code': 'C050003', 'sab': 'SENNET', 'term': 'Sample'}, {'code': 'C050004', 'sab': 'SENNET', 'term': 'Source'}, {'code': 'C050021', 'sab': 'SENNET', 'term': 'Publication Entity'}, {'code': 'C050022', 'sab': 'SENNET', 'term': 'Upload'}],
#     'assay_classes_C004000': [{'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, {'rule_description': {...}, 'value': {...}}, ...],
#     'datasets_C003041': [{'code': 'C015001', 'sab': 'SENNET', 'term': 'UNKNOWN'}, {'code': 'C006303', 'sab': 'SENNET', 'term': 'Cell DIVE'}, {'code': 'C006503', 'sab': 'SENNET', 'term': 'CODEX'}, {'code': 'C007604', 'sab': 'SENNET', 'term': 'Light Sheet'}, {'code': 'C007804', 'sab': 'SENNET', 'term': 'MIBI'}, {'code': 'C003084', 'sab': 'SENNET', 'term': 'snRNAseq'}, {'code': 'C011902', 'sab': 'SENNET', 'term': 'LC-MS'}, {'code': 'C003074', 'sab': 'SENNET', 'term': 'HiFi-Slide'}, {'code': 'C003047', 'sab': 'SENNET', 'term': 'Histology'}, {'code': 'C003055', 'sab': 'SENNET', 'term': 'PhenoCycler'}, {'code': 'C003056', 'sab': 'SENNET', 'term': 'CyCIF'}, {'code': 'C003058', 'sab': 'SENNET', 'term': 'MALDI'}, {'code': 'C003066', 'sab': 'SENNET', 'term': 'SIMS'}, {'code': 'C012502', 'sab': 'SENNET', 'term': 'DESI'}, {'code': 'C003061', 'sab': 'SENNET', 'term': 'Auto-fluorescence'}, {'code': 'C003062', 'sab': 'SENNET', 'term': 'Confocal'}, {'code': 'C003063', 'sab': 'SENNET', 'term': 'Thick section Multiphoton MxIF'}, {'code': 'C003064', 'sab': 'SENNET', 'term': 'Second Harmonic Generation (SHG)'}, {'code': 'C003065', 'sab': 'SENNET', 'term': 'Enhanced Stimulated Raman Spectroscopy (SRS)'}, {'code': 'C003080', 'sab': 'SENNET', 'term': 'ATACseq (bulk)'}, {'code': 'C003081', 'sab': 'SENNET', 'term': 'RNAseq (bulk)'}, {'code': 'C014004', 'sab': 'SENNET', 'term': '10X Multiome'}, {'code': 'C003051', 'sab': 'SENNET', 'term': 'Molecular Cartography'}, {'code': 'C003070', 'sab': 'SENNET', 'term': 'CosMx'}, {'code': 'C003078', 'sab': 'SENNET', 'term': 'Xenium'}, {'code': 'C003057', 'sab': 'SENNET', 'term': 'MERFISH'}, {'code': 'C003071', 'sab': 'SENNET', 'term': 'DBiT'}, {'code': 'C003082', 'sab': 'SENNET', 'term': 'scATACseq'}, {'code': 'C003083', 'sab': 'SENNET', 'term': 'scRNAseq'}, {'code': 'C003054', 'sab': 'SENNET', 'term': 'SNARE-seq2'}, {'code': 'C003059', 'sab': 'SENNET', 'term': '2D Imaging Mass Cytometry'}, {'code': 'C003073', 'sab': 'SENNET', 'term': 'GeoMx (NGS)'}, {'code': 'C003076', 'sab': 'SENNET', 'term': 'Visium (no probes)'}, {'code': 'C003077', 'sab': 'SENNET', 'term': 'Visium (with probes)'}, {'code': 'C003053', 'sab': 'SENNET', 'term': 'ATACseq'}, {'code': 'C003052', 'sab': 'SENNET', 'term': 'RNAseq'}, {'code': 'C003075', 'sab': 'SENNET', 'term': 'RNAseq (with probes)'}, {'code': 'C003060', 'sab': 'SENNET', 'term': 'nanoSPLITS'}, {'code': 'C015002', 'sab': 'SENNET', 'term': 'Segmentation Mask'}, {'code': 'C003072', 'sab': 'SENNET', 'term': 'GeoMx (nCounter)'}, {'code': 'C004034', 'sab': 'SENNET', 'term': 'epic'}, {'code': 'C006705', 'sab': 'SENNET', 'term': 'DARTFish'}, {'code': 'C007002', 'sab': 'SENNET', 'term': '3D Imaging Mass Cytometry'}, {'code': 'C011406', 'sab': 'SENNET', 'term': 'Slideseq'}, {'code': 'C015000', 'sab': 'SENNET', 'term': 'MUSIC'}, {'code': 'C200553', 'sab': 'SENNET', 'term': 'seqFISH'}],
#     'VALUESET_C050020': [{'code': 'C050007', 'sab': 'SENNET', 'term': 'Mouse'}, {'code': 'C050006', 'sab': 'SENNET', 'term': 'Human'}, {'code': 'C050009', 'sab': 'SENNET', 'term': 'Human Organoid'}, {'code': 'C050010', 'sab': 'SENNET', 'term': 'Mouse Organoid'}]
# }
