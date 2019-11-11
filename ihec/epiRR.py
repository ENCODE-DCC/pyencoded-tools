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
import os

from encode_utils.connection import Connection
conn = Connection("prod")


def exp_xml(rr_obj):
    this_dir_path = os.path.dirname(os.path.realpath(__file__))
    encode_path = os.path.join(
        this_dir_path, "ENCODE", rr_obj.get("accession") + "_experiment.xml"
    )
    exp_xml = open(encode_path, 'w')
    exp_xml.write("<EXPERIMENT_SET>\n")
    for expObj in rr_obj['related_datasets']:
        exp_status = expObj.get('status')
        # if exp_status!='released' and exp_status!='archived':
        if exp_status != 'released':
            print(
                "****** In experiment.xml, skipping experiment "
                + expObj.get("accession")
                + " due to status: "
                + exp_status
            )
            continue
        expType = exp_type(expObj)
        if expType == 'OTHER':
            print(
                "****** In experiment.xml, skipping experiment "
                + expObj.get("accession")
                + " due to experiment type: "
                + expObj.get("assay_term_name", 'none')
            )
            continue

        expOntologyURI = exp_ontology_uri(expObj)
        libraryStrategy = exp_library_strategy(expObj)
        molecule = exp_molecule(expObj)
        librarySource = exp_library_source(molecule["molecule"])
        librarySelection = exp_library_selection(expObj)
        libraryLayout = exp_library_layout_platform(expObj)
        biosample_accession = exp_biosample_accession(expObj) or 'none'

        exp_xml.write(
            "<EXPERIMENT center_name=\"ENCODE\" accession=\""
            + expObj.get("accession", 'none')
            + "\">\n"
        )
        exp_xml.write(
            "\t<TITLE>" + expObj.get(
                "description", "none"
            ).replace('<', '&lt;').replace('>', '&gt;') + "</TITLE>\n"
        )
        exp_xml.write(
            "\t<STUDY_REF accession=\"ENCODE\" refcenter=\"ENCODE\"/>\n"
        )
        exp_xml.write("\t<DESIGN>\n")
        exp_xml.write(
            "\t\t<DESIGN_DESCRIPTION>"
            + expObj.get(
                "description", "none"
            ).replace('<', '&lt;').replace('>', '&gt;')
            + "</DESIGN_DESCRIPTION>\n"
        )
        exp_xml.write(
            '\t\t<SAMPLE_DESCRIPTOR accession="{}" refcenter="ENCODE" '
            'refname="{}"/>\n'.format(
                biosample_accession, expObj.get("biosample_summary", "none")
            )
        )

        exp_xml.write("\t\t<LIBRARY_DESCRIPTOR>\n")
        exp_xml.write(
            "\t\t\t<LIBRARY_STRATEGY>"+libraryStrategy+"</LIBRARY_STRATEGY>\n"
        )
        exp_xml.write(
            "\t\t\t<LIBRARY_SOURCE>"+librarySource+"</LIBRARY_SOURCE>\n"
        )
        exp_xml.write(
            "\t\t\t<LIBRARY_SELECTION>"
            + librarySelection
            + "</LIBRARY_SELECTION>\n"
        )
        exp_xml.write(
            "\t\t\t<LIBRARY_LAYOUT>"
            + libraryLayout["layout"]
            + "</LIBRARY_LAYOUT>\n"
        )
        exp_xml.write("\t\t</LIBRARY_DESCRIPTOR>\n")
        exp_xml.write("\t</DESIGN>\n")

        exp_xml.write("\t<PLATFORM>\n")
        exp_xml.write("\t\t"+libraryLayout["platform"]+"\n")
        exp_xml.write("\t</PLATFORM>\n")

        exp_xml.write("\t<EXPERIMENT_ATTRIBUTES>\n")
        exp_xml.write(
            "\t\t<EXPERIMENT_ATTRIBUTE><TAG>EXPERIMENT_TYPE</TAG><VALUE>"
            + expType
            + "</VALUE></EXPERIMENT_ATTRIBUTE>\n"
        )
        exp_xml.write(
            "\t\t<EXPERIMENT_ATTRIBUTE><TAG>EXPERIMENT_ONTOLOGY_URI</TAG>"
            "<VALUE>"
            + expOntologyURI
            + "</VALUE></EXPERIMENT_ATTRIBUTE>\n"
        )
        exp_xml.write(
            "\t\t<EXPERIMENT_ATTRIBUTE><TAG>MOLECULE_ONTOLOGY_URI</TAG><VALUE>"
            + molecule["uri"]
            + "</VALUE></EXPERIMENT_ATTRIBUTE>\n"
        )
        exp_xml.write(
            "\t\t<EXPERIMENT_ATTRIBUTE><TAG>MOLECULE</TAG><VALUE>"
            + molecule["molecule"]
            + "</VALUE></EXPERIMENT_ATTRIBUTE>\n"
        )
        exp_xml.write("\t</EXPERIMENT_ATTRIBUTES>\n")

        exp_xml.write("</EXPERIMENT>\n")

    exp_xml.write("</EXPERIMENT_SET>\n")
    exp_xml.close()


def exp_type(exp):
    assay = exp.get('assay_term_name', 'none')
    if (assay == 'none'):
        return assay

    # Process ChIP-seq
    # IHEC wants 'one of ('ChIP-Seq Input', 'Histone H3K4me1',
    # 'Histone H3K4me3', 'Histone H3K9me3', 'Histone H3K9ac',
    # 'Histone H3K27me3', 'Histone H3K36me3', etc.)'
    # I use 'ChIP-Seq Input' for control, 'Histone <histone name>' for
    # histones, 'ChIP-Seq Input: Transcription factor <TF name>' for everything
    # else
    if assay == 'ChIP-seq':
        target_id = exp.get('target', {}).get('uuid', 'none')
        if (target_id == 'none'):
            return 'ChIP-Seq Input'
        target_obj = conn.get(target_id)
        target = target_obj.get('label', 'none')
        if target.lower() == 'control':
            return 'ChIP-Seq Input'
        if target == 'none':
            return 'ChIP-Seq Input'

        investigated_as = target_obj.get('investigated_as', [])

        # Find histone or not
        is_histone = 0
        for investigated in investigated_as:
            if investigated == 'histone':
                is_histone = 1

        if (is_histone == 1):
            return 'Histone '+target
        else:
            return 'ChIP-Seq Input: Transcription factor '+target
    elif assay == 'RNA-seq':
        if exp['assay_title'] == 'total RNA-seq':
            return 'total-RNA-Seq'
        if exp['assay_title'] == 'polyA plus RNA-seq':
            return 'mRNA-Seq'
        return 'RNA-Seq'

    # Process other assays
    ihec_exp_type = {
        'ATAC-seq': 'Other',  # 'Chromatin Accessibility' failed validator
        'DNase-seq': 'Chromatin Accessibility',
        'microRNA-seq': 'smRNA-Seq',
        'MeDIP-seq': 'DNA Methylation',
        'MRE-seq': 'DNA Methylation',
        'RRBS': 'DNA Methylation',
        'whole-genome shotgun bisulfite sequencing': 'DNA Methylation'
    }

    assay = ihec_exp_type.get(assay, 'OTHER')
    return assay


def exp_library_strategy(exp):
    assay = exp.get("assay_term_name", 'none')
    if (assay == "none"):
        return assay

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

    assay = ihec_lib.get(assay, "OTHER")
    return assay


def exp_library_selection(exp):
    assay = exp.get("assay_term_name", 'none')
    if (assay == "none"):
        return "unspecified"

    ihec_lib_sel = {
        "ATAC-seq": "RANDOM",
        "ChIP-seq": "ChIP-Seq",
        "DNase-seq": "DNase",
        "MeDIP-seq": "5-methylcytidine antibody",
        "microRNA-seq": "size fractionation",
        "MRE-seq": "HMPR",
        "RNA-seq": "cDNA",
        "RRBS": "Reduced Representation",
        "whole-genome shotgun bisulfite sequencing": "RANDOM"
    }

    assay = ihec_lib_sel.get(assay, "other")
    return assay


def exp_library_layout_platform(exp):
    files = exp.get("files", [])
    file_spec = {
        "platform": "none",
        "layout": "none"
    }
    if len(files) == 0:
        return file_spec

    for onefile in files:
        run_type = onefile.get("run_type", "none")
        if run_type != "none":
            if run_type == "single-ended":
                file_spec["layout"] = "<SINGLE/>"
            else:
                file_spec["layout"] = "<PAIRED/>"

        platform_id = onefile.get("platform", "none")
        if (platform_id != "none"):
            platformObj = conn.get(platform_id)
            platform_term_name = platformObj.get("term_name", "none")
            file_spec["platform"] = platform(platform_term_name)

        # If we find a valid one, we are done
        if file_spec["platform"] != "none" and file_spec["layout"] != "none":
            return file_spec

    return file_spec


# https://www.ncbi.nlm.nih.gov/books/NBK54984/table/SRA_Glossary_BK.T._platform_descriptor_t/
def platform(platform_term_name):
    if platform_term_name == "none":
        return "none"

    ihec_platform = ""
    if "Illumina" in platform_term_name:
        tag = "ILLUMINA"
    elif "Pacific" in platform_term_name:
        tag = "PACBIO_SMRT"
    elif "Applied Biosystems" in platform_term_name:
        tag = "ABI_SOLID"
    elif "Roche 454" in platform_term_name:
        tag = "LS454"
    else:
        return "none"

    ihec_platform_dict = {
        "Applied Biosystems SOLiD System 3 Plus": "AB SOLiD System",
        "Applied Biosystems SOLiD System 4": "AB SOLiD 4 System",
        # Is this right?
        "Illumina Genome Analyzer I": "Illumina Genome Analyzer",
        # The choices are Analyzer, Analyzer II, Analyzer IIx
        "Illumina Genome Analyzer IIe": "Illumina Genome Analyzer II",
        "Illumina HiSeq X Ten": "HiSeq X Ten",
        "Illumina NextSeq 500": "NextSeq 500",
        "Roche 454 Genome Sequencer FLX": "454 GS FLX",
        "Pacific Biosciences RS II": "PacBio RS"
    }
    term_name = ihec_platform_dict.get(platform_term_name, "none")
    if term_name == "none":
        term_name = platform_term_name
    ihec_platform = (
        "<"+tag+"><INSTRUMENT_MODEL>"+term_name+"</INSTRUMENT_MODEL></"+tag+">"
    )
    return ihec_platform


def exp_ontology_uri(exp):
    assay_id = exp.get('assay_term_id', "none")
    if (assay_id == "none"):
        return assay_id
    exp_ont_uri = 'http://purl.obolibrary.org/obo/' + assay_id.replace(
        ':', '_'
    )
    return exp_ont_uri


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
    biosample_list = {}
    for expObj in rr_obj['related_datasets']:

        exp_status = expObj.get('status')
        if exp_status != 'released':
            continue
        expType = exp_type(expObj)
        if expType == 'OTHER':
            continue

        biosample = exp_biosample_accession(expObj)
        if biosample:
            biosample_list[biosample] = 1

    this_dir_path = os.path.dirname(os.path.realpath(__file__))
    encode_path = os.path.join(
        this_dir_path,
        "ENCODE",
        rr_obj.get("accession") + "_samples.xml"
    )
    sample_xml = open(encode_path, 'w')
    sample_xml.write("<SAMPLE_SET>\n")

    for biosample_id in biosample_list.keys():

        biosampleObj = conn.get(biosample_id)
        btype = biosampleObj.get("biosample_ontology", {}).get(
            "classification", "none"
        )

        outputStr = ""

        sample_xml.write(
            "<SAMPLE center_name=\"ENCODE\" accession=\""
            + biosampleObj["accession"]
            + "\">\n"
        )
        title = biosampleObj.get(
            "summary"
        ).replace('<', '&lt;').replace('>', '&gt;')
        sample_xml.write("<TITLE>" + title + "</TITLE>\n")
        sample_xml.write("<SAMPLE_NAME>\n")
        organism = biosampleObj.get("donor").get("organism")

        sample_xml.write(
            "\t<TAXON_ID>" + organism.get("taxon_id") + "</TAXON_ID>\n"
        )
        sample_xml.write(
            "\t<SCIENTIFIC_NAME>"
            + organism.get("scientific_name")
            + "</SCIENTIFIC_NAME>\n"
        )
        sample_xml.write(
            "\t<COMMON_NAME>" + organism.get("name") + "</COMMON_NAME>\n"
        )
        sample_xml.write("</SAMPLE_NAME>\n")

        sample_xml.write("<SAMPLE_ATTRIBUTES>\n")

        sample_xml.write(
            "\t<SAMPLE_ATTRIBUTE><TAG>SAMPLE_ONTOLOGY_URI</TAG>"
            "<VALUE>http://purl.obolibrary.org/obo/"
            + biosampleObj.get('biosample_ontology', {}).get('term_id')
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )

        donor = biosampleObj.get("donor")
        disease = donor.get('health_status', "Healthy").capitalize()
        if donor['organism']['name'] != 'human':
            disease = biosampleObj.get(
                "model_organism_health_status", "Healthy"
            ).capitalize()

        if disease == "Healthy":
            disease_ontology_uri = "http://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&#38;code=C115935&#38;ns=NCI_Thesaurus"  # noqa: E501
        else:
            disease_ontology_uri = "https://ncit.nci.nih.gov/ncitbrowser/pages/multiple_search.jsf?nav_type=terminologies"  # noqa: E501

        sample_xml.write(
            "\t<SAMPLE_ATTRIBUTE><TAG>DISEASE_ONTOLOGY_URI</TAG><VALUE>"
            + disease_ontology_uri
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )
        sample_xml.write(
            "\t<SAMPLE_ATTRIBUTE><TAG>DISEASE</TAG><VALUE>"
            + disease
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )
        sample_xml.write(
            "\t<SAMPLE_ATTRIBUTE><TAG>BIOMATERIAL_PROVIDER</TAG><VALUE>"
            + biosampleObj.get("source").get(
                "description"
            ).replace('<', '&lt;').replace('>', '&gt;')
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )

        if btype in ["tissue", "whole organism"]:
            outputStr = tissueXML(biosampleObj)
        if btype in ["primary cell", "stem cell"]:
            outputStr = primaryCellCultureXML(biosampleObj)
        if btype in [
            "cell line", "in vitro differentiated cells",
            "induced pluripotent stem cell line"
        ]:
            outputStr = cellLineXML(biosampleObj)

        sample_xml.write(outputStr)
        sample_xml.write("</SAMPLE_ATTRIBUTES>\n")
        sample_xml.write("</SAMPLE>\n")

    sample_xml.write("</SAMPLE_SET>\n")
    sample_xml.close()


def cellLineXML(biosampleObj):
    outputStr = (
        "\t<SAMPLE_ATTRIBUTE><TAG>BIOMATERIAL_TYPE</TAG>"
        "<VALUE>Cell Line</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>LINE</TAG><VALUE>"
        + biosampleObj.get('biosample_ontology', {}).get('term_name', "none")
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>PASSAGE</TAG><VALUE>"
        + str(biosampleObj.get("passage_number", "unknown"))
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>SEX</TAG><VALUE>"
        + biosampleObj.get("sex", "unknown").capitalize()
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )

    dev_slims = biosampleObj.get('biosample_ontology', {}).get(
        "developmental_slims", []
    )
    lineage = "unknown"
    if len(dev_slims) > 0:
        lineage = ','.join(dev_slims)
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>LINEAGE</TAG><VALUE>"
        + lineage
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )

    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DIFFERENTIATION_STAGE</TAG>"
        "<VALUE>unknown</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DIFFERENTIATION_METHOD</TAG>"
        "<VALUE>unknown</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>MEDIUM</TAG>"
        "<VALUE>unknown</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>BATCH</TAG>"
        "<VALUE>unknown</VALUE></SAMPLE_ATTRIBUTE>\n"
    )

    return outputStr


def donor(biosampleObj):
    donorId = biosampleObj["donor"]['@id']
    if donorId is None:
        return ""

    donorObj = conn.get(donorId)
    donorInfo = (
        "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_ID</TAG><VALUE>"
        + donorObj.get('accession', "NA")
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )

    # mouse donor
    if "mouse" in donorId:
        age = biosampleObj.get('model_organism_age', "NA")
        # IHEC only allows integer age, otherwise, validator fails.
        if age != "NA":
            age = str(int(float(age)))
        donorInfo += (
            "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_AGE</TAG><VALUE>"
            + age
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )
        donorInfo += (
            "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_AGE_UNIT</TAG><VALUE>"
            + biosampleObj.get('model_organism_age_units', "year")
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )
        donorInfo += (
            "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_LIFE_STAGE</TAG><VALUE>"
            + biosampleObj.get('mouse_life_stage', "unknown")
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )

        disease = biosampleObj.get(
            'model_organism_health_status', "Healthy"
        ).capitalize()
        if disease == "Healthy":
            disease_ontology_uri = "http://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&#38;code=C115935&#38;ns=NCI_Thesaurus"  # noqa: E501
        else:
            disease_ontology_uri = "https://ncit.nci.nih.gov/ncitbrowser/pages/multiple_search.jsf?nav_type=terminologies"  # noqa: E501

        donorInfo += (
            "\t<SAMPLE_ATTRIBUTE><TAG>DDONOR_HEALTH_STATUS_ONTOLOGY_URI</TAG>"
            "<VALUE>"
            + disease_ontology_uri
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )
        donorInfo += (
            "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_HEALTH_STATUS</TAG><VALUE>"
            + disease
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )

        donorInfo += (
            "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_SEX</TAG><VALUE>"
            + biosampleObj.get('model_organism_sex', "unknown").capitalize()
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )
        donorInfo += (
            "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_ETHNICITY</TAG>"
            "<VALUE>NA</VALUE></SAMPLE_ATTRIBUTE>\n"
        )

        return donorInfo

    # human donor
    age = donorObj.get('model_organism_age', "NA")
    # IHEC only allows integer age, otherwise, validator fails.
    if age != "NA":
        age = str(int(float(age)))

    donorInfo += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_AGE</TAG><VALUE>"
        + age
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    donorInfo += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_AGE_UNIT</TAG><VALUE>"
        + donorObj.get('age_units', "year")
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    donorInfo += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_LIFE_STAGE</TAG><VALUE>"
        + donorObj.get('life_stage', "unknown")
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )

    disease = donorObj.get('health_status', "Healthy").capitalize()
    if disease == "Healthy":
        disease_ontology_uri = "http://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&#38;code=C115935&#38;ns=NCI_Thesaurus"  # noqa: E501
    else:
        disease_ontology_uri = "https://ncit.nci.nih.gov/ncitbrowser/pages/multiple_search.jsf?nav_type=terminologies"  # noqa: E501

    donorInfo += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DDONOR_HEALTH_STATUS_ONTOLOGY_URI</TAG>"
        "<VALUE>"
        + disease_ontology_uri
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    donorInfo += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_HEALTH_STATUS</TAG><VALUE>"
        + disease
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )

    donorInfo += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_SEX</TAG><VALUE>"
        + donorObj.get('sex', "unknown").capitalize()
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    donorInfo += (
        "\t<SAMPLE_ATTRIBUTE><TAG>DONOR_ETHNICITY</TAG><VALUE>"
        + donorObj.get('ethnicity', "NA")
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    return donorInfo


def tissueXML(biosampleObj):
    outputStr = (
        "\t<SAMPLE_ATTRIBUTE><TAG>BIOMATERIAL_TYPE</TAG>"
        "<VALUE>Primary Tissue</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>TISSUE_TYPE</TAG><VALUE>"
        + biosampleObj.get('biosample_ontology', {}).get('term_name', 'none')
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>TISSUE_DEPOT</TAG><VALUE>"
        + biosampleObj.get('source', {}).get(
            'description', 'unknown'
        ).replace('<', '&lt;').replace('>', '&gt;')
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>COLLECTION_METHOD</TAG>"
        "<VALUE>unknown</VALUE></SAMPLE_ATTRIBUTE>\n"
    )

    outputStr += donor(biosampleObj)
    return outputStr


def primaryCellCultureXML(biosampleObj):
    outputStr = (
        "\t<SAMPLE_ATTRIBUTE><TAG>BIOMATERIAL_TYPE</TAG>"
        "<VALUE>Primary Cell Culture</VALUE></SAMPLE_ATTRIBUTE>\n"
    )

    originated_from = biosampleObj.get('originated_from', "none")
    if (originated_from != "none"):
        originated_from = originated_from.get('uuid')
        origin_sample = conn.get(originated_from)
        outputStr += (
            "\t<SAMPLE_ATTRIBUTE><TAG>ORIGIN_SAMPLE_ONTOLOGY_URI</TAG>"
            "<VALUE>http://purl.obolibrary.org/obo/"
            + origin_sample.get('biosample_ontology', {}).get('term_id')
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )
        outputStr += (
            "\t<SAMPLE_ATTRIBUTE><TAG>ORIGIN_SAMPLE</TAG><VALUE>"
            + origin_sample.get('biosample_ontology', {}).get('term_name')
            + "</VALUE></SAMPLE_ATTRIBUTE>\n"
        )

    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>CELL_TYPE</TAG><VALUE>"
        + biosampleObj.get('biosample_ontology', {}).get('term_name', 'NA')
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>MARKERS</TAG><VALUE>unknown</VALUE>"
        "</SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>CULTURE_CONDITIONS</TAG>"
        "<VALUE>unknown</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += (
        "\t<SAMPLE_ATTRIBUTE><TAG>PASSAGE_IF_EXPANDED</TAG><VALUE>"
        + str(biosampleObj.get("passage_number", "NA"))
        + "</VALUE></SAMPLE_ATTRIBUTE>\n"
    )
    outputStr += donor(biosampleObj)
    return outputStr


def ihec_json(rr_obj):
    this_dir_path = os.path.dirname(os.path.realpath(__file__))
    acc = rr_obj.get("accession")

    encode_path = os.path.join(this_dir_path, "ENCODE", acc+".refepi.json")
    ihec_json = open(encode_path, 'w')

    localName = rr_obj.get("accession")
    project = "ENCODE"
    if rr_obj.get("award").get("rfa") == "Roadmap":
        project = "NIH Roadmap Epigenomics"

        aliases = rr_obj.get("aliases", [])
        for alias in aliases:
            if ("roadmap-" not in alias):
                continue
            localName = alias
            break

    ihec_json.write("{\"project\": \"" + project + "\",\n")

    # find if there is ihec accession #
    xrefs = rr_obj.get("dbxrefs", [])
    for xref in xrefs:
        if ("IHEC:" not in xref):
            continue
        acc = xref.replace("IHEC:", "")
        acc = acc.split('.')[0]
        break

    if "IHEC" in acc:
        ihec_json.write("\"accession\": \""+acc+"\",\n")

    ihec_json.write("\"local_name\": \""+localName+"\",\n")
    ihec_json.write("\"description\": \""+rr_obj.get("description")+"\",\n")

    ihec_json.write("\"raw_data\": [\n")

    outputTxt = ""
    for expObj in rr_obj['related_datasets']:
        exp_status = expObj.get('status')
        # if exp_status!='released' and exp_status!='archived':
        if exp_status != 'released':
            continue
        expType = exp_type(expObj)
        if expType == 'OTHER':
            continue

        outputTxt += (
            "\t{\"archive\": \"ENCODE\", \"primary_id\": \""
            + expObj.get("accession")
            + "\"},\n"
        )
    outputTxt = outputTxt[:-2] + "\n"

    ihec_json.write(outputTxt)

    ihec_json.write("]}\n")
    ihec_json.close()


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
        rr_obj = conn.get("reference-epigenomes/"+epiRR_Acc)

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

    print("Done!")


if __name__ == "__main__":
    # execute only if run as a script
    main()
