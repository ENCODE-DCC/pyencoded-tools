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
                        help="The datatype of interest: CHIP, WGBS, DNASE, RNA")
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

    basic_query = 'search/?type=Experiment&award.rfa=ENCODE3'

    labs = {
        'Tom Gingeras': '&lab.title=Thomas+Gingeras%2C+CSHL',
        'Barbara Wold': '&lab.title=Barbara+Wold%2C+Caltech',
        'Ross Hardsion': '&lab.title=Ross+Hardison%2C+PennState',
        'Eric Lecuyer': '&lab.name=eric-lecuyer',
        'Brenton Graveley': '&lab.title=Brenton+Graveley%2C+UConn',
    }
    rows = {
        'RAMPAGE': '&assay_term_name=RAMPAGE',
        'Long RNA': '&assay_term_name=RNA-seq&replicates.library.size_range%21=<200&replicates.library.nucleic_acid_starting_quantity_units%21=pg',
        'Low input long RNA': '&assay_term_name=RNA-seq&replicates.library.size_range%21=<200&replicates.library.nucleic_acid_starting_quantity_units=pg',
        'Small RNA': '&assay_term_name=RNA-seq&replicates.library.size_range=<200',
        'micro RNA': '&assay_term_name=microRNA-seq',
        'Nanostring': '&assay_term_name=microRNA+profiling+by+array+assay',
        'shRNA knockdowns': '&assay_term_name=shRNA+knockdown+followed+by+RNA-seq&target.investigated_as%21=control',
        'shRNA controls': '&assay_term_name=shRNA+knockdown+followed+by+RNA-seq&target.investigated_as=control',
        'single cell': '&assay_term_name=single+cell+isolation+followed+by+RNA-seq',
        'total': '&assay_term_name=RAMPAGE&assay_term_name=RNA-seq&assay_term_name=microRNA-seq&assay_term_name=microRNA+profiling+by+array+assay&assay_term_name=shRNA+knockdown+followed+by+RNA-seq'
    }

    released_query = '&status=released'
    proposed_query = '&status=proposed&status=preliminary'
    read_depth_query = '&audit.NOT_COMPLIANT.category=insufficient+read+depth'
    concordance_query = '&audit.NOT_COMPLIANT.category=insufficient+spearman+correlation'
    in_progress_query = '&status=in+progress'
    concerns_query = '&internal_status=requires+lab+review&internal_status=unrunnable'
    submitted_query = '&status=submitted'
    gtex_query = "searchTerm=GTEX"
    unreplicated_query = '&replication_type=unreplicated'
    processing_query = '&internal_status=pipeline+ready&internal_status=processing'
    unreleased_query = '&status=submitted&status=in+progress&status=ready+for+review&status=release+ready&status=started'

    queries = {
        'Total': '&status%21=replaced&status%21=deleted',
        'Released': released_query,
        'Released with metadata issues': released_query + concerns_query,
        'Released with read-depth issues': released_query + read_depth_query,
        'Released with concordance issues': released_query + concordance_query,
        'Released unreplicated': released_query + unreplicated_query,
        'Released GTEX unreplicated': released_query + gtex_query + unreplicated_query,
        'Proposed': proposed_query,
        'Unreleased': unreleased_query,
        'Unreleased with metadata issues': in_progress_query,
        'In pipeline': unreleased_query + processing_query,
        'Unreleased with read-depth issues': unreleased_query + read_depth_query,
        'Unreleased with concordance issues': unreleased_query + concordance_query,
        'Unreleased unreplicated': unreleased_query + unreplicated_query,
        'Unreleased GTEX unreplicated': unreleased_query + gtex_query + unreplicated_query,
    }

    headers = [
        'Total',
        'Released',
        'Released with metadata issues',
        'Released with read-depth issues',
        'Released with concordance issues',
        'Released unreplicated',
        'Released GTEX unreplicated',
        'Proposed',
        'Unreleased',
        'Unreleased with metadata issues',
        'In pipeline',
        'Unreleased with read-depth issues',
        'Unreleased with concordance issues',
        'Unreleased unreplicated',
        'Unreleased GTEX unreplicated'
    ]

    matrix = {}
    print('\t'.join([''] + headers))
    for row in rows.keys():

        matrix[row] = [row]

        for col in headers:
            query = basic_query + rows[row] + queries[col]
            res = get_ENCODE(query, connection, frame='embedded')
            link = connection.server + query
            total = res['total']
            if (col in [
                    'Unreleased with concordance issues',
                    'Released with concordance issues',
                    'Unreleased with read-depth issues',
                'Released with read-depth issues']
                ) and (row in [
                        'micro RNA',
                        'Nanostring',
                    ]):
                total = 'no audit'
            if (col in [
                    'Unreleased with concordance issues',
                    'Released with concordance issues',
                    'Unreleased unreplicated',
                    'Released unreplicated',
            ]
            ) and (row in [
                'single cell',
            ]):
                total = 'no audit'
            # if col == 'Released with metadata issues':
            #    total = make_errors_detail(res['facets'], link)

            if total == 'no audit':
                matrix[row].append(total)
            else:
                func = '=HYPERLINK(' + '"' + link + '",' + repr(total) + ')'
                matrix[row].append(func)

        print('\t'.join(matrix[row]))
    print(' ')
    print(' ')

    print('Long RNA Breakdown by lab --------------------------------------')
    print('\t'.join([''] + headers))
    for lab in labs.keys():

        matrix[lab] = [lab]

        for col in headers:
            query = basic_query + labs[lab] + rows['Long RNA'] + queries[col]
            res = get_ENCODE(query, connection, frame='embedded')
            link = connection.server + query
            total = res['total']
            # if col == 'Released with metadata issues':
            #    total = make_errors_detail(res['facets'], link)
            if total == 'no audit':
                matrix[lab].append(total)
            else:
                func = '=HYPERLINK(' + '"' + link + '",' + repr(total) + ')'
                matrix[lab].append(func)

        print('\t'.join(matrix[lab]))
    print(' ')
    print(' ')


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
    concordance_query = '&searchTerm=IDR%3Afail'  # '&searchTerm=IDR%3Afail'
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
        'Released in pipeline': released_query + processing_query,
        'Released cannot run in pipeline': released_query + unrunnable_query,
        'Released with no known issues': released_query + no_concerns_query,
        'Released with issues': released_query + concerns_query,
        'Released with failing ENCODE2 read-depth': released_query + read_depth_query,
        'Released with failing ENCODE3 read-depth': released_query + read_depth_query_3,
        'Released with complexity issues': released_query + complexity_query,
        'Released with concordance issues': released_query + concordance_query,
        'Released with antibody issues': released_query + antibody_query,
        'Released with read-length issues': released_query + read_length_query,
        'Released unreplicated': released_query + unreplicated_query,
        'Released missing pipeline': released_query + not_pipeline_query,
        'Unreleased': unreleased_query,
        'Unreleased with no known issues': unreleased_query + no_concerns_query,
        'Unreleased with issues': unreleased_query + concerns_query,
        'Unreleased cannot run in pipeline': unreleased_query + unrunnable_query,
        'Unreleased in pipeline': unreleased_query + processing_query,
        'Unreleased with partial pipeline': unreleased_query + pipeline_query + no_peaks_query,
        'Unreleased with failing ENCODE2 read-depth': unreleased_query + read_depth_query,
        'Unreleased with failing ENCODE3 read-depth': unreleased_query + read_depth_query_3,
        'Unreleased with complexity issues': unreleased_query + complexity_query,
        'Unreleased with concordance issues': unreleased_query + concordance_query,
        'Unreleased with antibody issues': unreleased_query + antibody_query,
        'Unreleased with read-length issues': unreleased_query + read_length_query,
        'Unreleased unreplicated': unreleased_query + unreplicated_query,
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
        print(lab, '--------------------------------------')
        print('\t'.join([''] + headers))

        matrix = {}

        for row in rows.keys():

            matrix[row] = [row]

            for col in headers:
                query = basic_query + labs[lab] + rows[row] + queries[col]
                res = get_ENCODE(query, connection, frame='embedded')
                link = connection.server + query
                total = res['total']

                # if col == 'Released with antibody issues':
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
            print('\t'.join(matrix[row]))
        print(' ')
        print(' ')


def main():
    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    if args.datatype == 'CHIP':
        make_chip_report(connection)
    elif args.datatype == 'RNA':
        make_rna_report(connection)
    else:
        print('unimplimented')


if __name__ == '__main__':
    main()
