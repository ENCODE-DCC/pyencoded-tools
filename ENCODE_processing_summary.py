#!/usr/bin/env python
# -*- coding: latin-1 -*-
''' Prepare a summary for different datatypes for ENCODE3
'''
import os.path
import argparse
import encodedcc
import collections
from encodedcc import get_ENCODE

EPILOG = '''
The output of this program is for consumption by a googlesheet.
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('--datatype',
                        help="The datatype of interest: CHIP, WGBS, Accessibility, RNA, RBP")
    parser.add_argument('--status',
                        help="released or unreleased")
    parser.add_argument('--grant',
                        help="specify the PI last name of the grant of interest, ENCODE2 will mimic ENCODE3")
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("~/keypairs.json")))
    parser.add_argument('--debug',
                        default=False,
                        action='store_true',
                        help="Print debug messages.  Default is False.")
    args = parser.parse_args()
    return args


def make_matrix(rows, columns, headers, queries, basic_query, connection):

        matrix = {}

        for row in rows:

            matrix[row] = [row]

            for col in headers:
                query = basic_query+queries[row]+columns[col]
                res = get_ENCODE(query, connection, frame='object')
                link = connection.server + query
                total = res['total']
                func = '=HYPERLINK(' + '"' + link + '",' + repr(total) + ')'
                matrix[row].append(func)
            print ('\t'.join(matrix[row]))
        print (' ')
        print (' ')


def make_rna_report(connection, columns, rows):

    basic_query = 'search/?type=Experiment'

    assays = {
        'RAMPAGE and CAGE': '&assay_title=CAGE&assay_title=RAMPAGE',
        'Total RNA': '&assay_title=RNA-seq',
        'PolyA RNA': '&assay_title=polyA+mRNA+RNA-seq',
        'PolyA depleted RNA': '&assay_title=polyA+depleted+RNA-seq',
        'Small RNA': '&assay_title=small+RNA-seq',
        'single cell': '&assay_title=single+cell+RNA-seq',
        'total': '&assay_title=RNA-seq&assay_title=polyA+mRNA+RNA-seq&assay_title=small+RNA-seq&assay_title=RAMPAGE&assay_title=CAGE&assay_title=single+cell+RNA-seq&assay_title=microRNA-seq&assay_title=microRNA+counts&assay_title=polyA+depleted+RNA-seq'
        }

    micro_assays = {
        'microRNA seq': '&assay_title=microRNA-seq',
        'microRNA counts': '&assay_title=microRNA+counts',
        }

    labels = [
        'Total',
        'Released',
        'Released with issues',
        'Unreleased',
        'Processed on GRCh38 or mm10',
        'Uniformly Processed on hg19',
        'Cannot be currently processed',
        'In processing queue',
        'Mismatched file status'
    ]

    micro_labels = [
        'Total',
        'Released',
        'Released with issues',
        'Unreleased',
        'Submitted on GRCh38',
        #'Submitted on hg19',
        'Submitted on mm10'
    ]

    headers = list(columns.keys())

    for assay in assays.keys():
        print (assay, '--------')
        print ('\t'.join([''] + headers))

        new_basic_query = basic_query + assays[assay]
        make_matrix(labels, columns, headers, rows, new_basic_query, connection)

    for assay in micro_assays.keys():
        print (assay, '--------')
        print ('\t'.join([''] + headers))

        new_basic_query = basic_query + micro_assays[assay]
        make_matrix(micro_labels, columns, headers, rows, new_basic_query, connection)


def make_methyl_report(connection, columns, rows):

    basic_query = 'search/?type=Experiment'

    assays = {
        'WGBS': '&assay_title=WGBS',
        'RRBS': '&assay_title=RRBS',
        'Array': '&assay_title=DNAme+array',
        'total': '&assay_title=WGBS&assay_title=RRBS&assay_title=DNAme+array'
        }

    labels = [
        'Total',
        'Released',
        'Released with issues',
        'Unreleased',
        'Processed on GRCh38 or mm10',
        'Uniformly Processed on hg19',
        'Cannot be currently processed',
        'In processing queue',
        'Mismatched file status'
    ]

    headers = list(columns.keys())

    for assay in assays.keys():
        print (assay, '--------')
        print ('\t'.join([''] + headers))

        new_basic_query = basic_query + assays[assay]
        make_matrix(labels, columns, headers, rows, new_basic_query, connection)


def make_chip_report(connection, columns):

    basic_query = 'search/?type=Experiment&assay_term_name=ChIP-seq'

    catagories = collections.OrderedDict([
        ('controls', '&target.investigated_as=control'),
        ('histone mods', '&target.investigated_as=histone'),
        ('other targets', '&target.investigated_as%21=control&target.investigated_as%21=histone')
        ])

    total_query = '&status=released&status=submitted&status=started&status=ready+for+review'
    released_query = '&status=released'
    proposed_query = '&status=proposed'
    unreleased_query = '&status=submitted&status=ready+for+review&status=started'
    concerns_query = '&internal_status=no+available+pipeline&internal_status=requires+lab+review&internal_status=unrunnable'
    grch38_query = '&files.assembly=GRCh38&'
    hg19_query = '&files.assembly=hg19'
    mm10_query = '&files.assembly=mm10'
    uniform_query = '&files.lab.name=encode-processing-pipeline'
    orange_audits_query = '&audit.NOT_COMPLIANT.category=missing+controlled_by&audit.NOT_COMPLIANT.category=insufficient+read+depth&audit.NOT_COMPLIANT.category=missing+documents&audit.NOT_COMPLIANT.category=control+insufficient+read+depth&audit.NOT_COMPLIANT.category=unreplicated+experiment&audit.NOT_COMPLIANT.category=poor+library+complexity&audit.NOT_COMPLIANT.category=severe+bottlenecking&audit.NOT_COMPLIANT.category=insufficient+replicate+concordance&audit.NOT_COMPLIANT.category=missing+possible_controls&audit.NOT_COMPLIANT.category=missing+input+control'   
    concerns_query = '&internal_status=requires+lab+review&internal_status=unrunnable*internal_status=no+available+pipeline'
    antibody_query = '&audit.NOT_COMPLIANT.category=not+eligible+antibody'
    red_audits_query = '&audit.ERROR.category=missing+raw+data+in+replicate&audit.ERROR.category=missing+donor&audit.ERROR.category=inconsistent+library+biosample&audit.ERROR.category=inconsistent+replicate&audit.ERROR.category=replicate+with+no+library&audit.ERROR.category=technical+replicates+with+not+identical+biosample&&audit.ERROR.category=missing+paired_with&audit.ERROR.category=missing+possible_controls&audit.ERROR.category=inconsistent+control&audit.ERROR.category=missing+antibody'
    processing_query = '&internal_status=pipeline+ready&internal_status=processing'
    mismatched_file_query = '&audit.INTERNAL_ACTION.category=mismatched+file+status'
    peaks_query = '&files.file_type=bigBed+narrowPeak'

    queries = {
        'Total': total_query,
        'Released': released_query,
        'Released with antibody issues': released_query + antibody_query,
        'Released with NOT COMPLIANT issues': released_query + orange_audits_query,
        'Released with ERROR issues': released_query + red_audits_query,
        'Unreleased': unreleased_query,
        'Proposed': proposed_query,
        'Processed on GRCh38 or mm10': total_query + grch38_query + mm10_query + uniform_query,
        'Uniformly Processed on hg19': total_query + hg19_query + uniform_query,
        'Has hg19 Peaks': total_query + hg19_query + uniform_query + peaks_query,
        'Released in pipeline': released_query + processing_query,
        'Cannot be currently processed': concerns_query,
        'In processing queue': processing_query,
        'Mismatched file status': mismatched_file_query
    }

    rows = [
        'Total',
        'Released',
        'Released with antibody issues',
        'Released with NOT COMPLIANT issues',
        'Released with ERROR issues',
        'Unreleased',
        'Processed on GRCh38 or mm10',
        'Uniformly Processed on hg19',
        'Has hg19 Peaks',
        'Cannot be currently processed',
        'In processing queue',
        'Mismatched file status'
        ]

    headers = list(columns.keys())

    for catagory in catagories.keys():

        print (catagory, '--------------------------------------')
        print ('\t'.join([''] + headers))
        new_basic_query = basic_query + catagories[catagory]
        make_matrix(rows, columns, headers, queries, new_basic_query, connection)


def make_dna_report(connection, columns):

    basic_query = 'search/?type=Experiment&award.project=ENCODE'

    assays = {
        'DNase': '&assay_title=DNase-seq',
        'ATAC': '&assay_title=ATAC-seq',
        'total': '&assay_title=DNase-seq&assay_title=ATAC-seq',
        }

    total_query = '&status=released&status=submitted&status=started&status=ready+for+review&status!=deleted&status!=revoked&status!=replaced'
    released_query = '&status=released'
    proposed_query = '&status=proposed'
    unreleased_query = '&status=submitted&status=ready+for+review&status=started'
    concerns_query = '&internal_status=no+available+pipeline&internal_status=requires+lab+review&internal_status=unrunnable&status!=deleted'
    grch38_query = '&assembly=GRCh38'
    hg19_query = '&assembly=hg19'
    mm10_query = '&assembly=mm10'
    uniform_query = '&files.lab.name=encode-processing-pipeline'
    processing_query = '&internal_status=pipeline+ready&internal_status=processing'
    red_audits_query = '&audit.ERROR.category=missing+raw+data+in+replicate&audit.ERROR.category=missing+donor&audit.ERROR.category=inconsistent+library+biosample&audit.ERROR.category=inconsistent+replicate&audit.ERROR.category=replicate+with+no+library&audit.ERROR.category=technical+replicates+with+not+identical+biosample&&audit.ERROR.category=missing+paired_with'
    orange_audits_query = '&audit.NOT_COMPLIANT.category=missing+controlled_by&audit.NOT_COMPLIANT.category=insufficient+read+depth&audit.NOT_COMPLIANT.category=missing+documents&audit.NOT_COMPLIANT.category=unreplicated+experiment&audit.NOT_COMPLIANT.category=missing+possible_controls&audit.NOT_COMPLIANT.category=missing+spikeins&audit.NOT_COMPLIANT.category=missing+RNA+fragment+size'
    mismatched_file_query = '&audit.INTERNAL_ACTION.category=mismatched+file+status'
    unknown_org_query = '&replicates.library.biosample.donor.organism.scientific_name%21=Homo+sapiens&replicates.library.biosample.donor.organism.scientific_name%21=Mus+musculus'

    rows = {
        'Total': total_query,
        'Released': released_query,
        'Released with ERROR': released_query+red_audits_query,
        'Released with NOT COMPLIANT': released_query+orange_audits_query,
        'Unreleased': unreleased_query,
        'Proposed': proposed_query,
        'Processed on GRCh38': total_query + grch38_query + uniform_query,
        'Uniformly processed on hg19': total_query + hg19_query + uniform_query,
        'Processed on mm10': total_query + mm10_query + uniform_query,
        'Cannot be currently processed': concerns_query,
        'In processing queue': total_query + processing_query,
        'Mismatched file status': mismatched_file_query
    }

    labels = [
        'Total',
        'Released',
        'Released with ERROR',
        'Released with NOT COMPLIANT',
        'Unreleased',
        'Processed on GRCh38',
        'Uniformly processed on hg19',
        'Processed on mm10',
        'Cannot be currently processed',
        'In processing queue',
        'Mismatched file status'
    ]

    headers = list(columns.keys())

    for assay in assays.keys():
        print (assay, '--------')
        print ('\t'.join([''] + headers))

        new_basic_query = basic_query + assays[assay]
        make_matrix(labels, columns, headers, rows, new_basic_query, connection)

def make_rbp_report(connection):

    basic_query = 'search/?type=Experiment&award.project=ENCODE'

    assays = {
        'eCLIP': '&assay_title=eCLIP',
        'iCLIP': '&assay_title=iCLIP',
        'RIP-seq': '&assay_title=RIP-seq',
        'Bind-n-Seq': '&assay_title=RNA+Bind-n-Seq',
        }

    seq_assays = {
        'shRNA knockdown': '&assay_title=shRNA+RNA-seq',
        'CRISPR': '&assay_title=CRISPR+RNA-seq',
        }

    total_assays = {
        'total': '&assay_title=eCLIP&assay_title=RIP-seq&assay_title=iCLIP&assay_title=RNA+Bind-n-Seq&assay_title=shRNA+RNA-seq&assay_title=CRISPR+RNA-seq&assay_title=RNA+Bind-n-Seq',
        }

    total_query = '&status=released&status=submitted&status=started&status=proposed&status=ready+for+review&status!=deleted&status!=revoked&status!=replaced&status!=archived'
    released_query = '&status=released'
    proposed_query = '&status=proposed'
    unreleased_query = '&status=submitted&status=ready+for+review&status=started'
    concerns_query = '&internal_status=no+available+pipeline&internal_status=requires+lab+review&internal_status=unrunnable&status!=deleted'
    grch38_query = '&files.assembly=GRCh38'
    hg19_query = '&files.assembly=hg19'
    uniform_query = '&files.lab.name=encode-processing-pipeline'
    submitted_query = '&files.lab.name!=encode-processing-pipeline'
    processing_query = '&internal_status=pipeline+ready&internal_status=processing'
    red_audits_query = '&audit.ERROR.category=missing+raw+data+in+replicate&audit.ERROR.category=missing+donor&audit.ERROR.category=inconsistent+library+biosample&audit.ERROR.category=inconsistent+replicate&audit.ERROR.category=replicate+with+no+library&audit.ERROR.category=technical+replicates+with+not+identical+biosample&&audit.ERROR.category=missing+paired_with'
    orange_audits_query = '&audit.NOT_COMPLIANT.category=missing+controlled_by&audit.NOT_COMPLIANT.category=insufficient+read+depth&audit.NOT_COMPLIANT.category=missing+documents&audit.NOT_COMPLIANT.category=unreplicated+experiment&audit.NOT_COMPLIANT.category=missing+possible_controls&audit.NOT_COMPLIANT.category=missing+spikeins&audit.NOT_COMPLIANT.category=missing+RNA+fragment+size'
    mismatched_file_query = '&audit.INTERNAL_ACTION.category=mismatched+file+status'
    unknown_org_query = '&replicates.library.biosample.donor.organism.scientific_name%21=Homo+sapiens&replicates.library.biosample.donor.organism.scientific_name%21=Mus+musculus'
    missing_signal_query = '&files.file_type!=bigWig&target.investigated_as!=control'
    antibody_query = '&audit.NOT_COMPLIANT.category=not+eligible+antibody'

    rows = {
        'Total': total_query,
        'Released': released_query,
        'Released with ERROR': released_query+red_audits_query,
        'Released with NOT COMPLIANT': released_query+orange_audits_query,
        'Antibody issues': antibody_query,
        'Processed on GRCh38': total_query + grch38_query + uniform_query,
        'Processed on hg19': total_query + hg19_query + uniform_query,
        'Submitted on GRCh38': total_query + grch38_query + submitted_query,
        'Submitted on hg19': total_query + hg19_query + submitted_query,
        'In processing queue': total_query + processing_query,
        'Unreleased': unreleased_query,
        'Proposed': proposed_query,
        'Mismatched file status': mismatched_file_query,
        'Missing signal files': total_query + missing_signal_query
    }

    labels = [
        'Total',
        'Released',
        'Released with ERROR',
        'Released with NOT COMPLIANT',
        'Antibody issues',
        'Unreleased',
        'Submitted on GRCh38',
        'Submitted on hg19',
        'Mismatched file status',
        'Missing signal files'
    ]

    seq_labels = [
        'Total',
        'Released',
        'Released with ERROR',
        'Released with NOT COMPLIANT',
        'Unreleased',
        'Processed on GRCh38',
        'Processed on hg19',
        'In processing queue',
        'Mismatched file status',
    ]

    total_labels = [
        'Total',
        'Released',
        'Released with ERROR',
        'Released with NOT COMPLIANT',
        'Antibody issues',
        'Unreleased',
        'Submitted on GRCh38',
        'Submitted on hg19',
        'Processed on GRCh38',
        'Processed on hg19',
        'Mismatched file status',
        'Missing signal files',
    ]

    columns = {
        'ENCODE3-experiments': '&award.rfa=ENCODE3&target.investigated_as!=control',
        'ENCODE3-controls': '&award.rfa=ENCODE3&target.investigated_as=control',
        'ENCODE2-experiments': '&award.rfa=ENCODE2&target.investigated_as!=control',
        'ENCODE2-controls': '&award.rfa=ENCODE2&target.investigated_as=control',
        'Total': '&award.rfa=ENCODE3&award.rfa=ENCODE2',
        }

    headers = [
        'ENCODE3-experiments',
        'ENCODE3-controls',
        'ENCODE2-experiments',
        'ENCODE2-controls',
        'Total'
        ]

    for assay in assays.keys():
        print (assay, '--------')
        print ('\t'.join([''] + headers))

        new_basic_query = basic_query + assays[assay]
        make_matrix(labels, columns, headers, rows, new_basic_query, connection)

    for assay in seq_assays.keys():
        print (assay, '--------')
        print ('\t'.join([''] + headers))

        new_basic_query = basic_query + seq_assays[assay]
        make_matrix(seq_labels, columns, headers, rows, new_basic_query, connection)

    for assay in total_assays.keys():
        print (assay, '--------')
        print ('\t'.join([''] + headers))

        new_basic_query = basic_query + total_assays[assay]
        make_matrix(total_labels, columns, headers, rows, new_basic_query, connection)

def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    labs = {
        'stam': '&lab.title=John+Stamatoyannopoulos%2C+UW&lab.title=Job+Dekker%2C+UMass',
        'gingeras': '&lab.title=Yijun+Ruan%2C+GIS&lab.title=Thomas+Gingeras%2C+CSHL&lab.title=Piero+Carninci%2C+RIKEN '
    }


    # ----------- QUERIES ----------------------------------------------------
    unreplicated_query = '&replication_type=unreplicated'
    not_pipeline_query = '&files.analysis_step_version.analysis_step.pipelines.title%21=Transcription+factor+ChIP-seq'
    no_peaks_query = '&files.file_type!=bigBed+narrowPeak'
    concordance_query = '&searchTerm=IDR%3Afail'  #'&searchTerm=IDR%3Afail'
    unrunnable_query = '&internal_status=unrunnable'
    pipeline_query = '&files.analysis_step_version.analysis_step.pipelines.title=Transcription+factor+ChIP-seq'
    read_depth_query = '&audit.NOT_COMPLIANT.category=insufficient+read+depth'
    read_depth_query_3 = '&audit.WARNING.category=low+read+depth'
    complexity_query = '&audit.NOT_COMPLIANT.category=insufficient+library+complexity'
    read_length_query = '&files.read_length=271272&files.read_length=657265&files.read_length=25&files.read_length=31&files.read_length=30'
    no_concerns_query = '&internal_status%21=requires+lab+review&internal_status%21=unrunnable'
    mismatched_file_query = '&audit.INTERNAL_ACTION.category=mismatched+file+status'

    human_query = '&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens'
    mouse_query = '&replicates.library.biosample.donor.organism.scientific_name=Mus+musculus'
    ENCODE2_query = '&award.rfa=ENCODE2&award.rfa=ENCODE2-Mouse'
    ENCODE3_query = '&award.rfa=ENCODE3'
    total_query = '&status=released&status=submitted&status=started&status=ready+for+review'
    released_query = '&status=released'
    proposed_query = '&status=proposed'
    unreleased_query = '&status=submitted&status=ready+for+review&status=started'
    concerns_query = '&internal_status=no+available+pipeline&internal_status=requires+lab+review&internal_status=unrunnable&status!=deleted&status!=revoked'
    grch38_query = '&assembly=GRCh38'
    hg19_query = '&files.genome_annotation=V19'
    mm10_query = '&assembly=mm10'
    uniform_query = '&files.lab.name=encode-processing-pipeline'
    audits_query = '&audit.NOT_COMPLIANT.category=missing+controlled_by&audit.NOT_COMPLIANT.category=insufficient+read+depth&audit.NOT_COMPLIANT.category=missing+documents&audit.NOT_COMPLIANT.category=unreplicated+experiment&assay_slims=Transcription&audit.NOT_COMPLIANT.category=missing+possible_controls&audit.NOT_COMPLIANT.category=missing+spikeins&audit.NOT_COMPLIANT.category=missing+RNA+fragment+size'
    processing_query = '&internal_status=pipeline+ready&internal_status=processing'
    unknown_org_query = '&replicates.library.biosample.donor.organism.scientific_name%21=Homo+sapiens&replicates.library.biosample.donor.organism.scientific_name%21=Mus+musculus'
    lab_query = labs.get(args.grant)

    row_queries = {
        'Total': total_query,
        'Released': released_query,
        'Released with issues': released_query+audits_query,
        'Unreleased': unreleased_query,
        'Proposed': proposed_query,
        'Processed on GRCh38': total_query + grch38_query + uniform_query,
        'Processed on GRCh38 or mm10': total_query + grch38_query + mm10_query + uniform_query,
        'Submitted on GRCh38': total_query + grch38_query,
        'Uniformly Processed on hg19': total_query + hg19_query + uniform_query,
        'Submitted on hg19': total_query + hg19_query,
        'Processed on mm10': total_query + mm10_query + uniform_query,
        'Submitted on mm10': total_query + mm10_query,
        'Cannot be currently processed': concerns_query,
        'In processing queue': processing_query,
        'Mismatched file status': mismatched_file_query
    }

    columns = collections.OrderedDict([
        ('ENCODE3-human', ENCODE3_query + human_query),
        ('ENCODE3-mouse', ENCODE3_query + mouse_query),
        ('ENCODE2-human', ENCODE2_query + human_query),
        ('ENCODE2-mouse', ENCODE2_query + mouse_query),
        # ('Organism Unknown', ENCODE3_query + unknown_org_query),
        ('Total', '&award.rfa=ENCODE3' + ENCODE2_query)
        ])

    if args.datatype == 'CHIP':
        make_chip_report(connection, columns, row_queries)
    elif args.datatype == 'RNA':
        make_rna_report(connection, columns, row_queries)
    elif args.datatype == 'METHYL':
        make_rna_report(connection, columns, row_queries)
    elif args.datatype == 'Accessibility':
        make_dna_report(connection, columns)
    elif args.datatype == 'RBP':
        make_rbp_report(connection)
    else:
        print ('unimplemented')

if __name__ == '__main__':
    main()
