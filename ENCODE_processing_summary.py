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
        'Proposed',
        'Processed on GRCh38',
        'Uniformly Processed on hg19',
        'Processed on mm10',
        'Cannot be currently processed',
        'In processing queue',
        'Mismatched file status'
    ]

    micro_labels = [
        'Total',
        'Released',
        'Released with issues',
        'Unreleased',
        'Proposed',
        'Submitted on GRCh38',
        #'Submitted on hg19',
        'Submitted on mm10'
    ]

    headers = list(columns.keys())

    for assay in assays.keys():
        print (assay, '--------')
        matrix = {}
        print ('\t'.join([''] + headers))
        for row in labels:

            matrix[row] = [row]

            for col in headers:
                query = basic_query+assays[assay]+rows[row]+columns[col]
                res = get_ENCODE(query, connection, frame='object')
                link = connection.server + query
                total = res['total']
                func = '=HYPERLINK(' + '"' + link + '",' + repr(total) + ')'
                matrix[row].append(func)

            print ('\t'.join(matrix[row]))

        print (' ')
        print (' ')

    for assay in micro_assays.keys():
        print (assay, '--------')
        matrix = {}
        print ('\t'.join([''] + headers))
        for row in micro_labels:

            matrix[row] = [row]

            for col in headers:
                query = basic_query+micro_assays[assay]+rows[row]+columns[col]
                res = get_ENCODE(query, connection, frame='object')
                link = connection.server + query
                total = res['total']
                func = '=HYPERLINK(' + '"' + link + '",' + repr(total) + ')'
                matrix[row].append(func)

            print ('\t'.join(matrix[row]))

        print (' ')
        print (' ')


def make_antibody_detail(graph):

    antibodies = {}
    for item in (graph):
        for rep in item['replicates']:
            ab = rep['antibody']['accession']
            target = rep['antibody']['targets'][0]['label']
            if ab not in antibodies:
                antibodies[ab] = [
                    target,
                    repr(len(rep['antibody']['characterizations']))
                    ]


def make_chip_report(connection, columns):

    basic_query = 'search/?type=Experiment&award.rfa=ENCODE3&assay_term_name=ChIP-seq'

    catagories = collections.OrderedDict([
        ('controls', '&target.investigated_as=control'),
        ('histone mods', '&target.investigated_as=histone'),
        ('other targets', '&target.investigated_as%21=control&target.investigated_as=histone')
        ])

    total_query = '&status=released&status=submitted&status=started&status=proposed&status=ready+for+review'
    released_query = '&status=released'
    proposed_query = '&status=proposed'
    unreleased_query = '&status=submitted&status=ready+for+review&status=started'
    concerns_query = '&internal_status=no+available+pipeline&internal_status=requires+lab+review&internal_status=unrunnable'
    grch38_query = '&assembly=GRCh38&'
    hg19_query = '&assembly=hg19'
    mm10_query = '&assembly=mm10'
    uniform_query = '&files.lab.name=encode-processing-pipeline'
    audits_query = '&audit.NOT_COMPLIANT.category=missing+controlled_by&audit.NOT_COMPLIANT.category=insufficient+read+depth&audit.NOT_COMPLIANT.category=missing+documents&audit.NOT_COMPLIANT.category=unreplicated+experiment&assay_slims=Transcription&audit.NOT_COMPLIANT.category=missing+possible_controls&audit.NOT_COMPLIANT.category=missing+spikeins&audit.NOT_COMPLIANT.category=missing+RNA+fragment+size'   
    concerns_query = '&internal_status=requires+lab+review&internal_status=unrunnable'
    antibody_query = '&audit.NOT_COMPLIANT.category=not+eligible+antibody'  
    processing_query = '&internal_status=pipeline+ready&internal_status=processing'

    queries = {
        'Total': total_query,
        'Released': released_query,
        'Released with antibody issues': released_query + audits_query,
        'Released with other issues': released_query + antibody_query,
        'Unreleased': unreleased_query,
        'Proposed': proposed_query,
        'Processed on GRCh38': total_query + grch38_query + uniform_query,
        'Submitted on GRCh38': total_query + grch38_query,
        'Uniformly Processed on hg19': total_query + hg19_query + uniform_query,
        'Processed on mm10': total_query + mm10_query + uniform_query,
        'Released in pipeline': released_query+processing_query,
        'Cannot be currently processed': concerns_query,
        'In processing queue': processing_query
    }

    rows = [
        'Total',
        'Released',
        'Released with antibody issues',
        'Released with other issues',
        'Unreleased',
        'Proposed',
        'Processed on GRCh38',
        'Uniformly Processed on hg19',
        'Processed on mm10',
        'Cannot be currently processed',
        'In processing queue'
        ]

    headers = list(columns.keys())

    for catagory in catagories.keys():
        print (catagory, '--------------------------------------')
        print ('\t'.join([''] + headers))

        matrix = {}

        for row in rows:

            matrix[row] = [row]

            for col in headers:
                query = basic_query+catagories[catagory]+queries[row]+columns[col]
                res = get_ENCODE(query, connection, frame='embedded')
                link = connection.server + query
                total = res['total']
                func = '=HYPERLINK(' + '"' + link + '",' + repr(total) + ')'
                matrix[row].append(func)
            print ('\t'.join(matrix[row]))
        print (' ')
        print (' ')


def make_dna_report(connection):

    basic_query = 'search/?type=Experiment&award.project=ENCODE'

    assays = {
        'DNase': '&assay_title=DNase-seq',
        'ATAC': '&assay_title=ATAC-seq',
        'total': '&assay_title=DNase-seq&assay_title=ATAC-seq',
        }

    total_query = '&status=released&status=submitted&status=started&status=proposed&status=ready+for+review&status!=deleted&status!=revoked&status!=replaced'
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
        'In processing queue': processing_query,
        'Mismatched file status': mismatched_file_query
    }

    labels = [
        'Total',
        'Released',
        'Released with ERROR',
        'Released with NOT COMPLIANT',
        'Unreleased',
        'Proposed',
        'Processed on GRCh38',
        'Uniformly processed on hg19',
        'Processed on mm10',
        'Cannot be currently processed',
        'In processing queue',
        'Mismatched file status'
    ]

    columns = {
        'ENCODE3': '&award.rfa=ENCODE3',
        'ENCODE2': '&award.rfa=ENCODE2',
        'ENCODE2-Mouse': '&award.rfa=ENCODE2-Mouse',
        'Total': '&award.rfa=ENCODE3&award.rfa=ENCODE2&award.rfa=ENCODE2-Mouse',
        }

    headers = [
        'ENCODE3',
        'ENCODE2',
        'ENCODE2-Mouse',
        'Total'
        ]

    for assay in assays.keys():
        print (assay, '--------')
        matrix = {}
        print ('\t'.join([''] + headers))
        for row in labels:

            matrix[row] = [row]

            for col in headers:
                query = basic_query+assays[assay]+rows[row]+columns[col]
                res = get_ENCODE(query, connection, frame='object')
                link = connection.server + query
                total = res['total']
                func = '=HYPERLINK(' + '"' + link + '",' + repr(total) + ')'
                matrix[row].append(func)

            print ('\t'.join(matrix[row]))

        print (' ')
        print (' ')


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

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

    human_query = '&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens'
    mouse_query = '&replicates.library.biosample.donor.organism.scientific_name=Mus+musculus'
    ENCODE2_query = '&award.rfa=ENCODE2&award.rfa=ENCODE2-Mouse'
    total_query = '&status=released&status=submitted&status=started&status=proposed&status=ready+for+review'
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

    rows = {
        'Total': total_query,
        'Released': released_query,
        'Released with issues': released_query+audits_query,
        'Unreleased': unreleased_query,
        'Proposed': proposed_query,
        'Processed on GRCh38': total_query + grch38_query + uniform_query,
        'Submitted on GRCh38': total_query + grch38_query,
        'Uniformly Processed on hg19': total_query + hg19_query + uniform_query,
        'Submitted on hg19': total_query + hg19_query,
        'Processed on mm10': total_query + mm10_query + uniform_query,
        'Submitted on mm10': total_query + mm10_query,
        'Cannot be currently processed': concerns_query,
        'In processing queue': processing_query
    }

    columns = collections.OrderedDict([
        ('ENCODE3-human', '&award.rfa=ENCODE3' + human_query),
        ('ENCODE3-mouse', '&award.rfa=ENCODE3' + mouse_query),
        ('ENCODE2-human', ENCODE2_query + human_query),
        ('ENCODE2-mouse', ENCODE2_query + mouse_query),
        ('Total', '&award.rfa=ENCODE3' + ENCODE2_query)
        ])

    if args.datatype == 'CHIP':
        make_chip_report(connection, columns)
    elif args.datatype == 'RNA':
<<<<<<< HEAD
        make_rna_report(connection, columns, rows)
    elif args.datatype == 'DNA':
=======
        make_rna_report(connection)
    elif args.datatype == 'Accessibility':
>>>>>>> d0420bd073ed5e0b8efe221df429453a3fcc10bd
        make_dna_report(connection)
    else:
        print ('unimplimented')

if __name__ == '__main__':
    main()
