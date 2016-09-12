#!/usr/bin/env python
# -*- coding: latin-1 -*-
''' Prepare a summary for different datatypes for ENCODE3
'''
import os.path
import argparse
import encodedcc
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


def make_rna_report(connection):

    basic_query = 'search/?type=Experiment'

    labs = {
        'Tom Gingeras': '&lab.title=Thomas+Gingeras%2C+CSHL',
        'Barbara Wold': '&lab.title=Barbara+Wold%2C+Caltech',
        'Ross Hardsion': '&lab.title=Ross+Hardison%2C+PennState',
        'Eric Lecuyer': '&lab.name=eric-lecuyer',
        'Brenton Graveley': '&lab.title=Brenton+Graveley%2C+UConn',
        }

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
        'Uniformely Processed on hg19': total_query + hg19_query + uniform_query,
        'Submitted on hg19': total_query + hg19_query,
        'Processed on mm10': total_query + mm10_query + uniform_query,
        'Submitted on mm10': total_query + mm10_query,
        'Cannot be currently processed': concerns_query,
        'In processing queue': processing_query
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
        'In processing queue'
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


    columns = {
        'ENCODE3': '&award.rfa=ENCODE3',
        'ENCODE2': '&award.rfa=ENCODE2',
        'ENCODE2-Mouse': '&award.rfa=ENCODE2-Mouse',
        'Total': '&award.rfa=ENCODE3&award.rfa=ENCODE2&award.rfa=ENCODE2-Mouse'
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


def make_errors_detail(facets, link):

    issues = {
        "inconsistent biological replicate number",
        "missing file in replicate",
        "biological replicates with identical biosample",
        "mismatched status",
        "mismatched biosample_term_name",
        "mismatched biosample_term_id",
        "mismatched target",
        "mismatched replicate",
        "missing controlled_by",
        "technical replicates with not identical biosample",
    }

    for f in facets:
        if f['field'] == 'audit.ERROR.category':
            errors = f
            break
        else:
            errors = None
    if errors is not None:
        list_of_errs = []
        total = 0
    for term in errors['terms']:
        count = term['doc_count']
        key = term['key']
        if key in issues and count > 0:
            func2 = '=HYPERLINK(' + '"' + link + '",' + repr(count) + ')'
            list_of_errs.append(func2)
            total = total + count
    return total


def make_chip_report(connection):

    basic_query = 'search/?type=Experiment&award.rfa=ENCODE3&assay_term_name=ChIP-seq'

    labs = {
        'Michael Snyder': '&lab.title=Michael+Snyder%2C+Stanford',
        'Bradley Bernstein': '&lab.title=Bradley+Bernstein%2C+Broad',
        'Bing Ren': '&lab.title=Bing+Ren%2C+UCSD',
        'Richard Myers': '&lab.title=Richard+Myers%2C+HAIB',
        'Xiang-Dong': '&lab.title=Xiang-Dong+Fu%2C+UCSD'
        }
    rows = {
        'controls': '&target.investigated_as=control',
        'experiments': '&target.investigated_as%21=control',
        'total': ''
        }

    released_query = '&status=released'
    no_concerns_query = '&internal_status%21=requires+lab+review&internal_status%21=unrunnable'
    concerns_query = '&internal_status=requires+lab+review&internal_status=unrunnable'
    read_depth_query = '&audit.NOT_COMPLIANT.category=insufficient+read+depth'
    read_depth_query_3 = '&audit.WARNING.category=low+read+depth'
    complexity_query = '&audit.NOT_COMPLIANT.category=insufficient+library+complexity'
    read_length_query = '&files.read_length=271272&files.read_length=657265&files.read_length=25&files.read_length=31&files.read_length=30'
    antibody_query = '&audit.NOT_COMPLIANT.category=not+eligible+antibody'
    concordance_query = '&searchTerm=IDR%3Afail'  #'&searchTerm=IDR%3Afail'
    unrunnable_query = '&internal_status=unrunnable'
    controls_query = ''
    submitted_query = '&status=submitted'
    pipeline_query = '&files.analysis_step_version.analysis_step.pipelines.title=Transcription+factor+ChIP-seq'
    processing_query = '&internal_status=pipeline+ready&internal_status=processing'
    unreleased_query = '&status=submitted&status=in+progress&status=ready+for+review&status=release+ready&status=started'
    unreplicated_query = '&replication_type=unreplicated'
    not_pipeline_query = '&files.analysis_step_version.analysis_step.pipelines.title%21=Transcription+factor+ChIP-seq'
    no_peaks_query = '&files.file_type!=bigBed+narrowPeak'
    proposed_query = '&status=proposed&status=preliminary'

    queries = {
        'Total': '&status%21=replaced&status%21=deleted&status%21=revoked',
        'Proposed': proposed_query,
        'Released': released_query,
        'Released in pipeline': released_query+processing_query,
        'Released cannot run in pipeline': released_query+unrunnable_query,
        'Released with no known issues': released_query+no_concerns_query,
        'Released with issues': released_query+concerns_query,
        'Released with failing ENCODE2 read-depth': released_query+read_depth_query,
        'Released with failing ENCODE3 read-depth': released_query+read_depth_query_3,
        'Released with complexity issues': released_query+complexity_query,
        'Released with concordance issues': released_query+concordance_query,
        'Released with antibody issues': released_query+antibody_query,
        'Released with read-length issues': released_query+read_length_query,
        'Released unreplicated': released_query+unreplicated_query,
        'Released missing pipeline': released_query+not_pipeline_query,
        'Unreleased': unreleased_query,
        'Unreleased with no known issues': unreleased_query+no_concerns_query,
        'Unreleased with issues': unreleased_query+concerns_query,
        'Unreleased cannot run in pipeline': unreleased_query+unrunnable_query,
        'Unreleased in pipeline': unreleased_query+processing_query,
        'Unreleased with partial pipeline': unreleased_query+pipeline_query+no_peaks_query,
        'Unreleased with failing ENCODE2 read-depth': unreleased_query+read_depth_query,
        'Unreleased with failing ENCODE3 read-depth': unreleased_query+read_depth_query_3,
        'Unreleased with complexity issues': unreleased_query+complexity_query,
        'Unreleased with concordance issues': unreleased_query+concordance_query,
        'Unreleased with antibody issues': unreleased_query+antibody_query,
        'Unreleased with read-length issues': unreleased_query+read_length_query,
        'Unreleased unreplicated': unreleased_query+unreplicated_query,
    }

    headers = [
        'Total',
        'Proposed',
        'Released',
        'Released cannot run in pipeline',
        'Released in pipeline',
        # 'Released with no known issues',
        'Released with failing ENCODE2 read-depth',
        'Released with failing ENCODE3 read-depth',
        # 'Released with complexity issues',
        'Released with concordance issues',
        'Released with antibody issues',
        'Released with read-length issues',
        'Released unreplicated',
        'Released missing pipeline',
        'Released with issues',
        'Unreleased',
        'Unreleased with issues',
        'Unreleased cannot run in pipeline',
        'Unreleased in pipeline',
        'Unreleased with partial pipeline',
        'Unreleased with failing ENCODE2 read-depth',
        'Unreleased with failing ENCODE3 read-depth',
        'Unreleased with complexity issues',
        'Unreleased with concordance issues',
        'Unreleased with antibody issues',
        'Unreleased with read-length issues',
        'Unreleased unreplicated',
        ]

    for lab in labs.keys():
        print (lab, '--------------------------------------')
        print ('\t'.join([''] + headers))

        matrix = {}

        for row in rows.keys():

            matrix[row] = [row]

            for col in headers:
                query = basic_query+labs[lab]+rows[row]+queries[col]
                res = get_ENCODE(query, connection, frame='embedded')
                link = connection.server + query
                total = res['total']

                #if col == 'Released with antibody issues':
                #    make_antibody_detail(res['@graph'])
                if col in [
                    'XUnreleased with concordance issues',
                    'XReleased with concordance issues',
                    #'Unreleased with complexity issues',
                    #'Released with complexity issues',
                ]:
                    total = 'no audit'
                if col == 'Unreleased with metadata issues':
                    total = make_errors_detail(res['facets'], link)

                func = '=HYPERLINK(' + '"' + link + '",' + repr(total) + ')'
                matrix[row].append(func)
            print ('\t'.join(matrix[row]))
        print (' ')
        print (' ')

def make_dna_report(connection):

    basic_query = 'search/?type=Experiment&award.project=ENCODE'

    labs = {
        'John Stamatoyannopoulos': '&lab.title=John+Stamatoyannopoulos%2C+UW',
        'Barbara Wold': '&lab.title=Barbara+Wold%2C+Caltech',
        'Ross Hardison': '&lab.title=Ross+Hardison%2C+PennState',
        'Michael Snyder': '&lab.title=Michael+Snyder%2C+Stanford',
        'Gregory Crawford': '&lab.title=Gregory+Crawford%2C+Duke',
        }

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
        'In processing queue'
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
    if args.datatype == 'CHIP':
        make_chip_report(connection)
    elif args.datatype == 'RNA':
        make_rna_report(connection)
    elif args.datatype == 'Accessibility':
        make_dna_report(connection)
    else:
        print ('unimplimented')

if __name__ == '__main__':
    main()
