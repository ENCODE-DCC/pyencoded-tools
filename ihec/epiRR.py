#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Print experiment.xml
# https://github.com/IHEC/ihec-metadata/blob/master/specs/Ihec_metadata_specification.md
# Ihec_metadata_specification.md:
#     Chromatin Accessibility, WGBS, MeDIP-Seq, MRE-Seq, ChIP-Seq, RNA-Seq
# I added ATAC-seq, RRBS following existing data format
# Missing: microRNA counts, transcription profiling by array assay

# IHEC aslo requires format forllowing
# https://www.ebi.ac.uk/ena/submit/read-xml-format-1-5, see SRA.experiment.xsd


import argparse
import json
import os
import xml.etree.ElementTree as ET

from encode_utils.connection import Connection
conn = Connection('prod')


def exp_xml(rr_obj):
    exp_set_xml = ET.Element('EXPERIMENT_SET')
    for expObj in sorted(
        rr_obj['related_datasets'], key=lambda e: e['accession']
    ):
        exp_status = expObj.get('status')
        # if exp_status!='released' and exp_status!='archived':
        if exp_status != 'released':
            print(
                '****** In experiment.xml, '
                'skipping experiment {} due to status: {}'.format(
                    expObj.get('accession'), exp_status
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
        ET.SubElement(
            design_xml,
            'SAMPLE_DESCRIPTOR',
            accession=exp_biosample_accession(expObj) or 'none',
            refcenter='ENCODE',
            refname=expObj.get('biosample_summary', 'none')
        )
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
        ET.SubElement(exp_attrib_xml, 'TAG').text = 'EXPERIMENT_ONTOLOGY_URI'
        ET.SubElement(exp_attrib_xml, 'VALUE').text = exp_ontology_uri(expObj)
        exp_attrib_xml = ET.SubElement(exp_attribs_xml, 'EXPERIMENT_ATTRIBUTE')
        ET.SubElement(exp_attrib_xml, 'TAG').text = 'MOLECULE_ONTOLOGY_URI'
        ET.SubElement(exp_attrib_xml, 'VALUE').text = molecule['uri']
        exp_attrib_xml = ET.SubElement(exp_attribs_xml, 'EXPERIMENT_ATTRIBUTE')
        ET.SubElement(exp_attrib_xml, 'TAG').text = 'MOLECULE'
        ET.SubElement(exp_attrib_xml, 'VALUE').text = molecule['molecule']

    ET.ElementTree(exp_set_xml).write(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'ENCODE',
            rr_obj.get('accession') + '_experiment.xml'
        )
    )


def exp_type(exp):
    assay = exp.get('assay_term_name')
    # Process ChIP-seq
    # IHEC wants 'one of ('ChIP-Seq Input', 'Histone H3K4me1',
    # 'Histone H3K4me3', 'Histone H3K9me3', 'Histone H3K9ac',
    # 'Histone H3K27me3', 'Histone H3K36me3', etc.)'
    # I use 'ChIP-Seq Input' for control, 'Histone <histone name>' for
    # histones, 'ChIP-Seq Input: Transcription factor <TF name>' for everything
    # else
    if assay == 'ChIP-seq':
        target_id = exp.get('target', {}).get('uuid')
        if not target_id:
            assert exp['control_type']
            return {'EXPERIMENT_TYPE': 'ChIP-Seq Input'}
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
        elif exp['assay_title'] == 'polyA plus RNA-seq':
            assay = 'mRNA-Seq'
        return {'EXPERIMENT_TYPE': assay}

    # Process other assays
    ihec_exp_type = {
        'ATAC-seq': 'Chromatin Accessibility',
        'DNase-seq': 'Chromatin Accessibility',
        'microRNA-seq': 'smRNA-Seq',
        'MeDIP-seq': 'DNA Methylation',
        'MRE-seq': 'DNA Methylation',
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


def exp_ontology_uri(exp):
    assay_id = exp.get('assay_term_id')
    if not assay_id:
        return 'none'
    return 'http://purl.obolibrary.org/obo/{}'.format(
        assay_id.replace(':', '_')
    )


def exp_molecule(exp):
    molecule_none = {
        "molecule": "none",
        "uri": "none"
    }
    molecule_DNA = {
        "molecule": "genomic DNA",
        "uri": "http://purl.obolibrary.org/obo/SO_0000991"
    }

    molecule_other = {
        "molecule": "",
        "uri": ""
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

    molecule_uri = 'http://purl.obolibrary.org/obo/' + molecule_id.replace(
        ':', '_'
    )

    molecule_other["molecule"] = molecule
    molecule_other["uri"] = molecule_uri
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


def samples_xml(rr_obj):
    # Get list of biosamples in the reference epigenome data set
    biosample_set = set()
    for expObj in rr_obj['related_datasets']:
        exp_status = expObj.get('status')
        if exp_status != 'released':
            continue
        expType = exp_type(expObj)
        if not expType:
            continue

        biosample = exp_biosample_accession(expObj)
        if biosample:
            biosample_set.add(biosample)

    sample_set_xml = ET.Element('SAMPLE_SET')
    for biosample_id in sorted(biosample_set):
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
            'SAMPLE_ONTOLOGY_URI': 'http://purl.obolibrary.org/obo/{}'.format(
                biosampleObj['biosample_ontology']['term_id']
            )
        }
        donor = biosampleObj.get('donor')
        disease = donor.get('health_status', 'Healthy').capitalize()
        if donor['organism']['name'] != 'human':
            disease = biosampleObj.get(
                'model_organism_health_status', 'Healthy'
            ).capitalize()
        if disease == 'Healthy':
            disease_ontology_uri = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=C115935'  # noqa: E501
        else:
            disease_ontology_uri = 'https://ncit.nci.nih.gov/ncitbrowser/pages/multiple_search.jsf?nav_type=terminologies'  # noqa: E501
        sample_attribute_dict['DISEASE_ONTOLOGY_URI'] = disease_ontology_uri
        sample_attribute_dict['DISEASE'] = disease
        sample_attribute_dict[
            'BIOMATERIAL_PROVIDER'
        ] = biosampleObj['source'].get('description')
        btype = biosampleObj['biosample_ontology']['classification']
        if btype in ['tissue', 'whole organism']:
            sample_attribute_dict.update(tissueXML(biosampleObj))
        if btype in ['primary cell', 'stem cell']:
            sample_attribute_dict.update(primaryCellCultureXML(biosampleObj))
        if btype in [
            'cell line', 'in vitro differentiated cells',
            'induced pluripotent stem cell line'
        ]:
            sample_attribute_dict.update(cellLineXML(biosampleObj))

        sample_attributes_xml = ET.SubElement(
            sample_xml, 'SAMPLE_ATTRIBUTES'
        )
        for (tag, value) in sample_attribute_dict.items():
            sample_attribute_xml = ET.SubElement(
                sample_attributes_xml, 'SAMPLE_ATTRIBUTE'
            )
            ET.SubElement(sample_attribute_xml, 'TAG').text = tag
            ET.SubElement(sample_attribute_xml, 'VALUE').text = value

    ET.ElementTree(sample_set_xml).write(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'ENCODE',
            rr_obj.get('accession') + '_samples.xml'
        )
    )


def cellLineXML(biosampleObj):
    return {
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
    sample_attribute_dict = {'DONOR_ID': donorObj['accession']}

    # mouse donor
    if 'mouse' in donorId:
        age = biosampleObj.get('model_organism_age', 'NA')
        # IHEC only allows integer age, otherwise, validator fails.
        if age != 'NA':
            age = str(int(float(age)))
        sample_attribute_dict['DONOR_AGE'] = age
        sample_attribute_dict['DONOR_AGE_UNIT'] = biosampleObj.get(
            'model_organism_age_units', 'year'
        )
        sample_attribute_dict['DONOR_LIFE_STAGE'] = biosampleObj.get(
            'mouse_life_stage', 'unknown'
        )
        disease = biosampleObj.get(
            'model_organism_health_status', 'Healthy'
        ).capitalize()
        if disease == 'Healthy':
            disease_ontology_uri = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=C115935'  # noqa: E501
        else:
            disease_ontology_uri = 'https://ncit.nci.nih.gov/ncitbrowser/pages/multiple_search.jsf?nav_type=terminologies'  # noqa: E501
        sample_attribute_dict[
            'DONOR_HEALTH_STATUS_ONTOLOGY_URI'
        ] = disease_ontology_uri
        sample_attribute_dict[
            'DONOR_HEALTH_STATUS'
        ] = disease
        sample_attribute_dict['DONOR_SEX'] = biosampleObj.get(
            'model_organism_sex', 'unknown'
        ).capitalize()
        sample_attribute_dict['DONOR_ETHNICITY'] = 'NA'

        # return donorInfo
        return sample_attribute_dict

    # human donor
    age = donorObj.get('model_organism_age', 'NA')
    # IHEC only allows integer age, otherwise, validator fails.
    if age != 'NA':
        age = str(int(float(age)))
    sample_attribute_dict['DONOR_AGE'] = age
    sample_attribute_dict['DONOR_AGE_UNIT'] = donorObj.get('age_units', 'year')
    sample_attribute_dict['DONOR_LIFE_STAGE'] = donorObj.get(
        'life_stage', 'unknown'
    )
    disease = donorObj.get('health_status', 'Healthy').capitalize()
    if disease == 'Healthy':
        disease_ontology_uri = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=C115935'  # noqa: E501
    else:
        disease_ontology_uri = 'https://ncit.nci.nih.gov/ncitbrowser/pages/multiple_search.jsf?nav_type=terminologies'  # noqa: E501
    sample_attribute_dict[
        'DONOR_HEALTH_STATUS_ONTOLOGY_URI'
    ] = disease_ontology_uri
    sample_attribute_dict['DONOR_HEALTH_STATUS'] = disease
    sample_attribute_dict['DONOR_SEX'] = donorObj.get(
        'sex', 'unknown'
    ).capitalize()
    sample_attribute_dict['DONOR_ETHNICITY'] = donorObj.get(
        'ethnicity', 'NA'
    )
    return sample_attribute_dict


def tissueXML(biosampleObj):
    sample_attribute_dict = {
        'BIOMATERIAL_TYPE': 'Primary Tissue',
        'TISSUE_TYPE': biosampleObj['biosample_ontology']['term_name'],
        'TISSUE_DEPOT': biosampleObj['source'].get('description', 'unknown'),
        'COLLECTION_METHOD': 'unknown',
    }
    sample_attribute_dict.update(donor(biosampleObj))
    return sample_attribute_dict


def primaryCellCultureXML(biosampleObj):
    sample_attribute_dict = {
        'BIOMATERIAL_TYPE': 'Primary Cell Culture',
        'CELL_TYPE': biosampleObj['biosample_ontology']['term_name'],
        'MARKERS': 'unknown',
        'CULTURE_CONDITIONS': 'unknown',
        'PASSAGE_IF_EXPANDED': str(biosampleObj.get('passage_number', 'NA')),
    }
    originated_from_uuid = biosampleObj.get('originated_from', {}).get('uuid')
    if originated_from_uuid:
        origin_sample = conn.get(originated_from_uuid)
        sample_attribute_dict[
            'ORIGIN_SAMPLE_ONTOLOGY_URI'
        ] = 'http://purl.obolibrary.org/obo/{}'.format(
            origin_sample['biosample_ontology']['term_id']
        )
        sample_attribute_dict[
            'ORIGIN_SAMPLE'
        ] = origin_sample['biosample_ontology']['term_name']
    sample_attribute_dict.update(donor(biosampleObj))

    return sample_attribute_dict


def ihec_json(rr_obj):
    localName = rr_obj['accession']
    project = 'ENCODE'
    ihec_json = {}
    if rr_obj.get('award').get('rfa') == 'Roadmap':
        project = 'NIH Roadmap Epigenomics'
        aliases = rr_obj.get('aliases', [])
        for alias in aliases:
            if ('roadmap-' not in alias):
                continue
            localName = alias
            break

    ihec_json = {
        'project': project,
        'local_name': localName,
        'description': rr_obj.get('description'),
        'raw_data': [
            {'archive': 'ENCODE', 'primary_id': expObj['accession']}
            for expObj in rr_obj['related_datasets']
            if expObj.get('status') == 'released' and exp_type(expObj)
        ],
    }

    # find if there is ihec accession #
    for xref in rr_obj.get('dbxrefs', []):
        if 'IHEC:' not in xref:
            continue
        acc = xref.replace('IHEC:', '').split('.')[0]
        break
    if 'IHEC' in acc:
        ihec_json['accession'] = acc

    encode_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'ENCODE',
        '{}.refepi.json'.format(rr_obj['accession'])
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
        rr_obj = conn.get('reference-epigenomes/{}'.format(epiRR_Acc))

        # Write the files
        exp_xml(rr_obj)
        samples_xml(rr_obj)
        ihec_json(rr_obj)

        # Validate
        os.system(
            'python __main__.py -sample ENCODE/'
            + epiRR_Acc
            + '_samples.xml -out:ENCODE/'
            + epiRR_Acc
            + '_samples.validated.xml -overwrite-outfile'
        )
        os.system(
            'python __main__.py -experiment ENCODE/'
            + epiRR_Acc
            + '_experiment.xml -out:ENCODE/'
            + epiRR_Acc
            + '_experiment.validated.xml -overwrite-outfile'
        )

    print('Done!')


if __name__ == '__main__':
    # execute only if run as a script
    main()
