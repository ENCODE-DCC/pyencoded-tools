#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Print experiment.xml
# https://github.com/IHEC/ihec-metadata/blob/master/specs/Ihec_metadata_specification.md
# Ihec_metadata_specification.md:
#     Chromatin Accessibility, WGBS, MeDIP-Seq, MRE-Seq, ChIP-Seq, RNA-Seq
# I added ATAC-seq, RRBS following existing data format
# Missing: microRNA counts, transcription profiling by array assay

# IHEC also requires format following
# https://www.ebi.ac.uk/ena/submit/read-xml-format-1-5, see SRA.experiment.xsd


import argparse
import json
import os
import xml.etree.ElementTree as ET

from encode_utils.connection import Connection
conn = Connection('prod')


def exp_xml(ref_epi_obj):
    exp_set_xml = ET.Element('EXPERIMENT_SET')
    for expObj in sorted(
        ref_epi_obj['related_datasets'], key=lambda e: e['accession']
    ):
        if expObj.get('status') != 'released':
            print(
                '****** In experiment.xml, '
                'skipping experiment {} due to status: {}'.format(
                    expObj.get('accession'), expObj.get('status')
                )
            )
            continue
        expType = exp_type(expObj)
        if not expType:
            print(
                '****** In experiment.xml, '
                'skipping experiment {} due to experiment type: {}'.format(
                    expObj.get('accession'),
                    expObj.get('assay_term_name', 'none')
                )
            )
            continue
        molecule = exp_molecule(expObj)
        lib_xml = exp_library_layout_platform_xml_map(expObj)

        exp_xml = ET.SubElement(
            exp_set_xml,
            'EXPERIMENT',
            center_name='ENCODE',
            accession=expObj['accession']
        )
        ET.SubElement(exp_xml, 'TITLE').text = expObj.get(
            'description', 'none'
        )
        ET.SubElement(
            exp_xml, 'STUDY_REF', accession='ENCODE', refcenter='ENCODE'
        )

        design_xml = ET.SubElement(exp_xml, 'DESIGN')
        ET.SubElement(design_xml, 'DESIGN_DESCRIPTION').text = expObj.get(
            'description', 'none'
        )
        exp_biosamples = sorted(
            [
                rep['library']['biosample']
                for rep in expObj.get('replicates', [])
                if 'library' in rep and 'biosample' in rep['library']
            ],
            key=lambda b: b['accession']
        )
        exp_biosamples = exp_biosamples or {
            'accession': 'none',
            'summary': 'none'
        }
        for biosample in exp_biosamples:
            ET.SubElement(
                design_xml,
                'SAMPLE_DESCRIPTOR',
                accession=biosample['accession'],
                refcenter='ENCODE',
                refname=biosample['summary']
            )
            # SRA schema cannot do multiple sampler per experiment unless you
            # claim they are POOL biosamples
            break
        lib_descriptor_xml = ET.SubElement(design_xml, 'LIBRARY_DESCRIPTOR')
        ET.SubElement(
            lib_descriptor_xml, 'LIBRARY_STRATEGY'
        ).text = exp_library_strategy(expObj)
        ET.SubElement(
            lib_descriptor_xml, 'LIBRARY_SOURCE'
        ).text = exp_library_source(molecule['molecule'])
        ET.SubElement(
            lib_descriptor_xml, 'LIBRARY_SELECTION'
        ).text = exp_library_selection(expObj)
        lib_descriptor_xml.append(lib_xml['layout_xml'])

        exp_xml.append(lib_xml['platform_xml'])

        exp_attribs_xml = ET.SubElement(exp_xml, 'EXPERIMENT_ATTRIBUTES')
        for tag in expType:
            exp_attrib_xml = ET.SubElement(
                exp_attribs_xml, 'EXPERIMENT_ATTRIBUTE'
            )
            ET.SubElement(exp_attrib_xml, 'TAG').text = tag
            ET.SubElement(exp_attrib_xml, 'VALUE').text = expType[tag]
        exp_attrib_xml = ET.SubElement(exp_attribs_xml, 'EXPERIMENT_ATTRIBUTE')
        ET.SubElement(exp_attrib_xml, 'TAG').text = 'EXPERIMENT_ONTOLOGY_CURIE'
        ET.SubElement(exp_attrib_xml, 'VALUE').text = exp_assay_ontology(expObj)
        exp_attrib_xml = ET.SubElement(exp_attribs_xml, 'EXPERIMENT_ATTRIBUTE')
        ET.SubElement(exp_attrib_xml, 'TAG').text = 'MOLECULE_ONTOLOGY_CURIE'
        ET.SubElement(exp_attrib_xml, 'VALUE').text = molecule['identifier']
        exp_attrib_xml = ET.SubElement(exp_attribs_xml, 'EXPERIMENT_ATTRIBUTE')
        ET.SubElement(exp_attrib_xml, 'TAG').text = 'MOLECULE'
        ET.SubElement(exp_attrib_xml, 'VALUE').text = molecule['molecule']

    ET.ElementTree(exp_set_xml).write(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'ENCODE',
            ref_epi_obj.get('accession') + '_experiment.xml'
        )
    )


def exp_type(exp):
    assay = exp.get('assay_term_name')
    exp_acc = exp.get('accession')
    expt = conn.get('experiments/{}'.format(exp_acc))
    # Process ChIP-seq
    # IHEC wants 'one of ('ChIP-Seq Input', 'Histone H3K4me1',
    # 'Histone H3K4me3', 'Histone H3K9me3', 'Histone H3K9ac',
    # 'Histone H3K27me3', 'Histone H3K36me3', etc.)'
    # I use 'ChIP-Seq Input' for control, 'Histone <histone name>' for
    # histones, 'ChIP-Seq Input: Transcription factor <TF name>' for everything
    # else
    if assay == 'ChIP-seq':
        target_id = expt.get('target', {}).get('uuid')
        if not target_id:
            assert exp['control_type']
            return {
                'EXPERIMENT_TYPE': 'ChIP-Seq Input',
                'EXPERIMENT_TARGET_HISTONE': 'NA',
            }
        else:
            target_obj = conn.get(target_id)
            target_label = target_obj['label']
            if 'histone' in target_obj['investigated_as']:
                return {
                    'EXPERIMENT_TYPE': 'Histone ' + target_label,
                    'EXPERIMENT_TARGET_HISTONE': target_label
                }
            else:
                tf_chip_type = {
                    'EXPERIMENT_TYPE': 'Transcription Factor',
                    'EXPERIMENT_TARGET_TF': target_label
                }
                if 'modifications' in target_obj:
                    tf_chip_type[
                        'EXPERIMENT_TARGET_TF_MODIFICATION'
                    ] = ', '.join(
                        '{} at {}{}'.format(
                            m['modification'],
                            m['amino_acid_code'],
                            m['position']
                        )
                        for m in target_obj['modifications']
                    )
                return tf_chip_type

    # Process RNA-seq
    if assay == 'RNA-seq':
        assay = 'RNA-Seq'
        if exp['assay_title'] == 'total RNA-seq':
            assay = 'total-RNA-Seq'
        return {'EXPERIMENT_TYPE': assay}

    # Process other assays
    ihec_exp_type = {
        'ATAC-seq': 'Chromatin Accessibility',
        'DNase-seq': 'Chromatin Accessibility',
        'microRNA-seq': 'smRNA-Seq',
        'MeDIP-seq': 'DNA Methylation',
        'MRE-seq': 'DNA Methylation',
        'polyA plus RNA-seq': 'mRNA-Seq',
        'RRBS': 'DNA Methylation',
        'whole-genome shotgun bisulfite sequencing': 'DNA Methylation'
    }
    if assay in ihec_exp_type:
        return {'EXPERIMENT_TYPE': ihec_exp_type[assay]}
    return {}


def exp_library_strategy(exp):
    ihec_lib = {
        'ATAC-seq': 'ATAC-seq',
        'ChIP-seq': 'ChIP-Seq',
        'DNase-seq': 'DNase-Hypersensitivity',
        'MeDIP-seq': 'MeDIP-Seq',
        'microRNA-seq': 'miRNA-Seq',
        'microRNA counts': 'miRNA-Seq',
        'MRE-seq': 'MRE-Seq',
        'polyA plus RNA-seq': 'RNA-Seq',
        'RNA-seq': 'RNA-Seq',
        'RRBS': 'Bisulfite-Seq',
        'whole-genome shotgun bisulfite sequencing': 'Bisulfite-Seq',
    }
    return ihec_lib.get(exp['assay_term_name'], 'OTHER')


def exp_library_selection(exp):
    ihec_lib_sel = {
        'ATAC-seq': 'RANDOM',
        'ChIP-seq': 'ChIP-Seq',
        'DNase-seq': 'DNase',
        'MeDIP-seq': '5-methylcytidine antibody',
        'microRNA-seq': 'size fractionation',
        'MRE-seq': 'HMPR',
        'RNA-seq': 'cDNA',
        'RRBS': 'Reduced Representation',
        'whole-genome shotgun bisulfite sequencing': 'RANDOM'
    }

    return ihec_lib_sel.get(exp['assay_term_name'], 'unspecified')


def exp_library_layout_platform_xml_map(exp):
    # https://www.ncbi.nlm.nih.gov/books/NBK54984/table/SRA_Glossary_BK.T._platform_descriptor_t/
    files = exp.get('files', [])
    if not files:
        return {}
    file_spec = {
        'platform_xml': ET.Element('PLATFORM'),
        'layout_xml': ET.Element('LIBRARY_LAYOUT')
    }
    ihec_platform_dict = {
        'Applied Biosystems SOLiD System 3 Plus': 'AB SOLiD System',
        'Applied Biosystems SOLiD System 4': 'AB SOLiD 4 System',
        # Is this right?
        'Illumina Genome Analyzer I': 'Illumina Genome Analyzer',
        # The choices are Analyzer, Analyzer II, Analyzer IIx
        'Illumina Genome Analyzer IIe': 'Illumina Genome Analyzer II',
        'Illumina HiSeq X Ten': 'HiSeq X Ten',
        'Illumina NextSeq 500': 'NextSeq 500',
        'Roche 454 Genome Sequencer FLX': '454 GS FLX',
        'Pacific Biosciences RS II': 'PacBio RS'
    }

    for onefile in files:
        run_type = onefile.get('run_type')
        if not run_type:
            continue
        if run_type == 'single-ended':
            ET.SubElement(file_spec['layout_xml'], 'SINGLE')
        else:
            ET.SubElement(file_spec['layout_xml'], 'PAIRED')
        platform_id = onefile.get('platform')
        if not platform_id:
            continue
        platformObj = conn.get(platform_id)
        platform_term_name = platformObj.get('term_name', '')
        if 'Illumina' in platform_term_name:
            tag = 'ILLUMINA'
        elif 'Pacific' in platform_term_name:
            tag = 'PACBIO_SMRT'
        elif 'Applied Biosystems' in platform_term_name:
            tag = 'ABI_SOLID'
        elif 'Roche 454' in platform_term_name:
            tag = 'LS454'
        else:
            continue
        ET.SubElement(
            ET.SubElement(file_spec['platform_xml'], tag), 'INSTRUMENT_MODEL'
        ).text = ihec_platform_dict.get(platform_term_name, platform_term_name)
        return file_spec

    return file_spec


def exp_assay_ontology(exp):
    assay_id = exp.get('assay_term_id')
    if not assay_id:
        return 'none'
    return assay_id.lower()


def exp_molecule(exp):
    molecule_none = {
        "molecule": "none",
        "identifier": "none"
    }
    molecule_DNA = {
        "molecule": "genomic DNA",
        "identifier": "so:0000991"
    }

    molecule_other = {
        "molecule": "",
        "identifier": ""
    }

    assay = exp.get("assay_term_name", 'none')
    if (assay == "none"):
        return molecule_none

    if assay in [
        "ATAC-seq", "ChIP-seq", "DNase-seq", "MeDIP-seq", "MRE-seq", "RRBS",
        "whole-genome shotgun bisulfite sequencing"
    ]:
        return molecule_DNA

    replicates = exp.get("replicates", [])
    if len(replicates) == 0:
        return molecule_none

    molecule = replicates[0].get("library", {}).get("nucleic_acid_term_name")
    if not molecule:
        return molecule_none

    molecule_id = replicates[0].get("library", {}).get("nucleic_acid_term_id")
    if not molecule_id:
        return molecule_none

    # IHEC: 'polyA RNA', 'total RNA', 'nuclear RNA', 'cytoplasmic RNA' for RNA
    # While the IHEC documentation says 'small RNA' is an option, the validator
    # rejects it.
    ihec_RNA = {
        "RNA": "total RNA",
        "polyadenylated mRNA": "polyA RNA",
        "miRNA": "cytoplasmic RNA"
    }

    molecule_conv = ihec_RNA.get(molecule, "none")
    if (molecule_conv != "none"):
        molecule = molecule_conv

    molecule_identifier = molecule_id.lower()

    molecule_other["molecule"] = molecule
    molecule_other["identifier"] = molecule_identifier
    return molecule_other


# This is a quick hack. It works for now because we don't have single cell data
def exp_library_source(molecule):
    if "RNA" in molecule:
        return "TRANSCRIPTOMIC"
    if "DNA" in molecule:
        return "GENOMIC"
    return "OTHER"


# only check the first - I assume that all replicates have the same biosample
def exp_biosample_accession(exp):
    replicates = exp.get("replicates", [])
    if len(replicates) > 0:
        return replicates[0].get("library", {}).get("biosample").get(
            "accession"
        )
    else:
        return None


def samples_xml(ref_epi_obj):
    # Get list of biosamples in the reference epigenome data set
    biosample_replicates_map = {}
    for expObj in ref_epi_obj['related_datasets']:
        if expObj.get('status') != 'released':
            continue
        expType = exp_type(expObj)
        if not expType:
            continue

        exp_biosamples = sorted(
            rep['library']['biosample']['accession']
            for rep in expObj.get('replicates', [])
            if 'library' in rep and 'biosample' in rep['library']
        )
        if not exp_biosamples:
            continue
        biosample_replicates_map.setdefault(exp_biosamples[0], set())
        if len(exp_biosamples) > 1:
            biosample_replicates_map[exp_biosamples[0]] |= set(
                exp_biosamples[1:]
            )

    sample_set_xml = ET.Element('SAMPLE_SET')
    for biosample_id in sorted(biosample_replicates_map):
        biosampleObj = conn.get(biosample_id)

        sample_xml = ET.SubElement(
            sample_set_xml,
            'SAMPLE',
            center_name='ENCODE',
            accession=biosampleObj['accession']
        )
        ET.SubElement(sample_xml, 'TITLE').text = biosampleObj['summary']
        sample_name_xml = ET.SubElement(sample_xml, 'SAMPLE_NAME')
        organism = biosampleObj.get('donor', {}).get('organism', {})
        ET.SubElement(
            sample_name_xml, 'TAXON_ID'
        ).text = organism.get('taxon_id')
        ET.SubElement(
            sample_name_xml, 'SCIENTIFIC_NAME'
        ).text = organism.get('scientific_name')
        ET.SubElement(
            sample_name_xml, 'COMMON_NAME'
        ).text = organism.get('name')

        sample_attribute_dict = {
            'BIOMATERIAL_PROVIDER': biosampleObj['source'].get('description'),
            'DISEASE': biosampleObj.get('health_status', 'NA'),
            'DISEASE_ONTOLOGY_CURIE': 'ncim:C115222', # unknown health status
            'DISEASE_ONTOLOGY_URI': 'https://nciterms.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=C115222',
            'TREATMENT': 'NA',
            'BIOLOGICAL_REPLICATES':
                list(biosample_replicates_map[biosample_id]) or 'NA',
        }
        if 'HEALTHY' in sample_attribute_dict['DISEASE'].capitalize():
            sample_attribute_dict['DISEASE_ONTOLOGY_CURIE'] = 'ncim:C0549184' # None (no diseases as specified by their schema)
            sample_attribute_dict['DISEASE_ONTOLOGY_URI'] = 'https://ncim.nci.nih.gov/ncimbrowser/pages/concept_details.jsf?dictionary=NCI%20Metathesaurus&code=C0549184'
        if biosampleObj.get('treatments'):
            sample_attribute_dict['TREATMENT'] = biosampleObj['summary']

        btype = biosampleObj['biosample_ontology']['classification']
        if btype in ['tissue', 'whole organism']:
            sample_attribute_dict.update(tissueXML(biosampleObj))
        if btype in ['primary cell', 'in vitro differentiated cells']:
            sample_attribute_dict.update(primaryCellCultureXML(biosampleObj))
        if btype == 'cell line':
            sample_attribute_dict.update(cellLineXML(biosampleObj))

        sample_attributes_xml = ET.SubElement(
            sample_xml, 'SAMPLE_ATTRIBUTES'
        )
        for (tag, value) in sample_attribute_dict.items():
            if isinstance(value, list):
                for v in value:
                    sample_attribute_xml = ET.SubElement(
                        sample_attributes_xml, 'SAMPLE_ATTRIBUTE'
                    )
                    ET.SubElement(sample_attribute_xml, 'TAG').text = tag
                    ET.SubElement(sample_attribute_xml, 'VALUE').text = v
            else:
                sample_attribute_xml = ET.SubElement(
                    sample_attributes_xml, 'SAMPLE_ATTRIBUTE'
                )
                ET.SubElement(sample_attribute_xml, 'TAG').text = tag
                ET.SubElement(sample_attribute_xml, 'VALUE').text = value

    ET.ElementTree(sample_set_xml).write(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'ENCODE',
            ref_epi_obj.get('accession') + '_samples.xml'
        )
    )


def cellLineXML(biosampleObj):
    return {
        'SAMPLE_ONTOLOGY_CURIE': biosampleObj['biosample_ontology']['term_id'].lower(),
        'SAMPLE_ONTOLOGY_URI': 'http://purl.obolibrary.org/obo/{}'.format(
            biosampleObj['biosample_ontology']['term_id'].replace(':', '_')
        ),
        'BIOMATERIAL_TYPE': 'Cell Line',
        'LINE': biosampleObj['biosample_ontology']['term_name'],
        'PASSAGE': str(biosampleObj.get("passage_number", "unknown")),
        'SEX': biosampleObj.get("sex", "unknown").capitalize(),
        'LINEAGE': ','.join(
            biosampleObj['biosample_ontology'].get('developmental_slims', [])
        ) or 'unknown',
        'DIFFERENTIATION_STAGE': 'unknown',
        'DIFFERENTIATION_METHOD': 'unknown',
        'MEDIUM': 'unknown',
        'BATCH': 'unknown',
    }


def donor(biosampleObj):
    donorId = biosampleObj.get('donor', {}).get('@id')
    if not donorId:
        return {}
    donorObj = conn.get(donorId)
    sample_attribute_dict = {
        'DONOR_ID': donorObj['accession'],
        'DONOR_LIFE_STAGE': biosampleObj['life_stage'],
        'DONOR_SEX': biosampleObj['sex'].capitalize(),
        'DONOR_HEALTH_STATUS': biosampleObj.get('health_status', 'NA'),
        'DONOR_HEALTH_STATUS_ONTOLOGY_CURIE': 'ncim:C115222',  # unknown health status
        'DONOR_HEALTH_STATUS_ONTOLOGY_URI': 'https://nciterms.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=C115222'
    }
    # Use calculated age on biosample because sample_collection_age may not
    # match listed donor age
    age = biosampleObj['age']
    # IHEC only allows integer age, otherwise, validator fails.
    if age != 'unknown':
        age = str(int(float(age)))
    else:
        age = 'NA'
    sample_attribute_dict['DONOR_AGE'] = age
    sample_attribute_dict['DONOR_AGE_UNIT'] = biosampleObj.get(
        'age_units', 'year'
    )

    ethnicity = donorObj.get('ethnicity', 'NA')
    if 'NA' not in ethnicity:
        ethnicity = ', '.join(ethnicity)
    sample_attribute_dict['DONOR_ETHNICITY'] = ethnicity

    if 'HEALTHY' in sample_attribute_dict['DONOR_HEALTH_STATUS'].capitalize():
        sample_attribute_dict['DONOR_HEALTH_STATUS_ONTOLOGY_CURIE'] = 'ncim:C0549184' # suggested None term
        sample_attribute_dict['DONOR_HEALTH_STATUS_ONTOLOGY_URI'] = 'https://ncim.nci.nih.gov/ncimbrowser/pages/concept_details.jsf?dictionary=NCI%20Metathesaurus&code=C0549184'

    return sample_attribute_dict


def tissueXML(biosampleObj):
    sample_attribute_dict = {
        'SAMPLE_ONTOLOGY_CURIE': biosampleObj['biosample_ontology']['term_id'].lower(),
        'SAMPLE_ONTOLOGY_URI': 'http://purl.obolibrary.org/obo/{}'.format(
            biosampleObj['biosample_ontology']['term_id'].replace(':', '_')
        ),
        'BIOMATERIAL_TYPE': 'Primary Tissue',
        'TISSUE_TYPE': biosampleObj['biosample_ontology']['term_name'],
        'TISSUE_DEPOT': biosampleObj['biosample_ontology']['term_name'],
        'COLLECTION_METHOD': 'unknown',
    }
    sample_attribute_dict.update(donor(biosampleObj))
    # Handle tissue NTR (germinal matrix)
    if sample_attribute_dict['SAMPLE_ONTOLOGY_CURIE'] == 'ntr:0001407':
        sample_attribute_dict['SAMPLE_ONTOLOGY_CURIE'] = 'uberon:0004022'
        sample_attribute_dict['SAMPLE_ONTOLOGY_URI'] = 'http://purl.obolibrary.org/obo/UBERON_0004022'

    return sample_attribute_dict


def primaryCellCultureXML(biosampleObj):
    sample_attribute_dict = {
        'SAMPLE_ONTOLOGY_CURIE': biosampleObj['biosample_ontology']['term_id'].lower(),
        'SAMPLE_ONTOLOGY_URI': 'http://purl.obolibrary.org/obo/{}'.format(
            biosampleObj['biosample_ontology']['term_id'].replace(':', '_')
        ),
        'BIOMATERIAL_TYPE': 'Primary Cell Culture',
        'CELL_TYPE': biosampleObj['biosample_ontology']['term_name'],
        'MARKERS': 'unknown',
        'CULTURE_CONDITIONS': 'unknown',
        'PASSAGE_IF_EXPANDED': str(biosampleObj.get('passage_number', 'NA')),
    }
    originated_from_uuid = biosampleObj.get('originated_from', {}).get('uuid')
    if originated_from_uuid:
        origin_sample = conn.get(originated_from_uuid)
        origin_sample_id = origin_sample['biosample_ontology']['term_id']
        if origin_sample_id.startswith('UBERON'):
            sample_attribute_dict['ORIGIN_SAMPLE_ONTOLOGY_CURIE'] = origin_sample_id.lower()
            sample_attribute_dict['ORIGIN_SAMPLE'] = origin_sample['biosample_ontology']['term_name']
            sample_attribute_dict['ORIGIN_SAMPLE_ONTOLOGY_URI'] = 'http://purl.obolibrary.org/obo/{}'.format(origin_sample_id.replace(':', '_'))
    sample_attribute_dict.update(donor(biosampleObj))

    # Handle some NTR terms
    if sample_attribute_dict['SAMPLE_ONTOLOGY_CURIE'] == 'ntr:0000427': # neurosphere
        sample_attribute_dict['SAMPLE_ONTOLOGY_CURIE'] = 'cl:0000047'
        sample_attribute_dict['SAMPLE_ONTOLOGY_URI'] = 'http://purl.obolibrary.org/obo/CL_0000047'
    elif sample_attribute_dict['SAMPLE_ONTOLOGY_CURIE'] == 'ntr:0003830': # mid-neurogenesis radial glial cells
        sample_attribute_dict['SAMPLE_ONTOLOGY_CURIE'] = 'cl:0000681'
        sample_attribute_dict['SAMPLE_ONTOLOGY_URI'] = 'http://purl.obolibrary.org/obo/CL_0000681'
    elif sample_attribute_dict['SAMPLE_ONTOLOGY_CURIE'] == 'ntr:0000856': # mesendoderm
        sample_attribute_dict['SAMPLE_ONTOLOGY_CURIE'] = 'cl:0000222'
        sample_attribute_dict['SAMPLE_ONTOLOGY_URI'] = 'http://purl.obolibrary.org/obo/CL_0000222'

    return sample_attribute_dict


def ihec_json(ref_epi_obj):
    localName = ref_epi_obj['accession']
    project = ref_epi_obj['award']['project']
    ihec_json = {}
    if project == 'Roadmap':
        project = 'NIH Roadmap Epigenomics'
        aliases = ref_epi_obj.get('aliases', [])
        for alias in aliases:
            if ('roadmap-' not in alias):
                continue
            localName = alias
            break

    ihec_json = {
        'project': project,
        'local_name': localName,
        'description': ref_epi_obj.get('description'),
        'raw_data': [
            {'archive': 'ENCODE', 'primary_id': expObj['accession']}
            for expObj in ref_epi_obj['related_datasets']
            if expObj.get('status') == 'released' and exp_type(expObj)
        ],
    }

    # find if there is ihec accession #
    for dbxref in ref_epi_obj.get('dbxrefs', []):
        if not dbxref.startswith('IHEC:'):
            continue
        acc = dbxref[5:]
        if 'IHEC' in acc:
            ihec_json['accession'] = acc
        break

    encode_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'ENCODE',
        '{}.refepi.json'.format(ref_epi_obj['accession'])
    )
    with open(encode_path, 'w') as f:
        json.dump(ihec_json, f, indent=4, sort_keys=True)


def main():
    parser = argparse.ArgumentParser(
        description='Prepare EpiRR submissions for ENCODE reference epigenomes'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--one',
        dest='accessions',
        nargs='+',
        help='One ENCODE reference epigenomes accession to process.'
    )
    group.add_argument(
        '--all',
        action='store_true',
        help='One ENCODE reference epigenomes accession to process.'
    )

    args = parser.parse_args()
    if not args.all:
        accessions = args.accessions
    else:
        accessions = [
            exp['accession'] for exp in conn.search(
                [
                    ('type', 'ReferenceEpigenome'),
                    ('status', 'released'),
                    ('field', 'accession'),
                    ('limit', 'all')
                ]
            )
        ]

    for epiRR_Acc in accessions:
        ref_epi_obj = conn.get('reference-epigenomes/{}'.format(epiRR_Acc))

        # Write the files
        exp_xml(ref_epi_obj)
        samples_xml(ref_epi_obj)
        ihec_json(ref_epi_obj)

        # Validate
        os.system(
            'python -m version_metadata'
            ' -jsonlog:ENCODE/{epiRR_Acc}_samples.log.json'
            ' -overwrite-outfile'
            ' -out:ENCODE/{epiRR_Acc}_samples.validated.xml'
            ' -sample ENCODE/{epiRR_Acc}_samples.xml'.format(
                epiRR_Acc=epiRR_Acc
            )
        )
        os.system(
            'python -m version_metadata'
            ' -jsonlog:ENCODE/{epiRR_Acc}_experiment.log.json'
            ' -overwrite-outfile'
            ' -out:ENCODE/{epiRR_Acc}_experiment.validated.xml'
            ' -experiment ENCODE/{epiRR_Acc}_experiment.xml'.format(
                epiRR_Acc=epiRR_Acc
            )
        )

    print('Done!')


if __name__ == '__main__':
    # execute only if run as a script
    main()
