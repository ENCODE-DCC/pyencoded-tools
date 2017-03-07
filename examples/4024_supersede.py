#!/usr/bin/env python2

from __future__ import print_function
import encodedcc
import logging
import re
import sys
import subprocess
import json
import dxpy
from pprint import pprint
from operator import itemgetter, attrgetter, methodcaller

EPILOG = """
    for each experiment
    build graphs based on derived_from
    validate graphs
    optionally replace one graph with another
"""

logger = logging.getLogger(__name__)

ACCESSIONED_OUTPUTS = {
    "tf_chip_seq": [
        "ENCODE Peaks.rep1_pvalue_signal",
        "ENCODE Peaks.rep1_fc_signal",
        "ENCODE Peaks.rep2_pvalue_signal",
        "ENCODE Peaks.rep2_fc_signal",
        "ENCODE Peaks.pooled_pvalue_signal",
        "ENCODE Peaks.pooled_fc_signal",
        "SPP Peaks.rep1_peaks",
        "SPP Peaks.rep2_peaks",
        "SPP Peaks.pooled_peaks",
        "SPP Peaks.rep1_peaks_bb",
        "SPP Peaks.rep2_peaks_bb",
        "SPP Peaks.pooled_peaks_bb",
        "Final IDR peak calls.optimal_set",
        "Final IDR peak calls.conservative_set",
        "Final IDR peak calls.optimal_set_bb",
        "Final IDR peak calls.conservative_set_bb"
    ]
}


def get_args():
    import argparse
    import os
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    def t_or_f(arg):
        ua = str(arg).upper()
        if ua == 'TRUE'[:len(ua)]:
            return True
        elif ua == 'FALSE'[:len(ua)]:
            return False
        else:
            assert not (True or False), "Cannot parse %s to boolean" % (arg)

    parser.add_argument('experiments', help='List of ENCSR accessions to report on', nargs='*', default=None)
    parser.add_argument('--infile', help='File containing ENCSR accessions', type=argparse.FileType('r'), default=None)
    parser.add_argument('--key', help="The keypair identifier from the keyfile.", default='www')
    parser.add_argument('--keyfile', help="The keyfile.", default=os.path.expanduser("~/keypairs.json"))
    parser.add_argument('--query', help="Query URI to generate the list of experiments", default=(
            'search/?type=experiment'
            '&assay_term_name=ChIP-seq'
            '&award.rfa=ENCODE3&limit=all'))
    parser.add_argument('--assembly', help='Look only at files with these assemblies', required=True)
    parser.add_argument('--debug', help="Print debug messages", default=False, action='store_true')
    parser.add_argument('--supersede', help="REVOKE older peak files and set supersedes metadata", default=False, action='store_true')

    return parser.parse_args()


def uri_to_accession(uri):
    m = re.match("/.*/(.*)/$", uri)
    if m:
        return m.group(1)
    else:
        return None


def enc_obj(o, connection):
    if isinstance(o, (str, unicode)):
        return encodedcc.get_ENCODE(o, connection)
    else:
        return o


def parent_analysis(file, connection):
    file_obj = enc_obj(file, connection)
    step_run = enc_obj(file_obj.get('step_run'), connection)
    job_aliases = [d['dx_job_id'] for d in step_run['dx_applet_details']]
    parent_analyses = []
    for alias in job_aliases:
        jobid = alias.partition(':')[2]
        parentAnalysis_id = dxpy.describe(jobid, fields={'parentAnalysis': True}).get('parentAnalysis')
        parentAnalysis = dxpy.describe(parentAnalysis_id, fields={'name': True, 'created': True, 'executableName': True})
        parent_analyses.append({
            'id': parentAnalysis_id,
            'created': parentAnalysis.get('created'),
            'executableName': parentAnalysis.get('executableName'),
            'name': parentAnalysis.get('name'),
        })
    parent_analysis_ids = set([parent_analysis.get('id') for parent_analysis in parent_analyses])
    if len(parent_analysis_ids) != 1:
        logger.error("%s parent_analysis: dx_applet_details resolve to %s analysis ID's" % (file_obj.get('accession'), len(parent_analysis_ids)))
        return None
    else:
        return parent_analyses.pop()


def experiment_bioreps(experiment, connection):
    replicate_objects = []
    for r in experiment.get('replicates', []):
        replicate_objects.append(enc_obj(r, connection))
    return list(set([r.get('biological_replicate_number') for r in replicate_objects]))


def get_experiments_by_accession(accession_list, connection):
    if len(accession_list) < 100:
        uri = "search/?type=Experiment"
        for acc in accession_list:
            uri += "&accession=%s" % (acc)
        experiments = encodedcc.get_ENCODE(uri, connection).get('@graph', [])
    else:
        experiments = []
        for acc in accession_list:
            uri = "experiments/%s/" % (acc)
            experiments.append(encodedcc.get_ENCODE(uri, connection))
    return experiments


def get_experiments(args, connection):
    if args.experiments:
        experiments = get_experiments_by_accession(args.experiments, connection)
    elif args.infile:
        experiments = get_experiments_by_accession(
            [i.strip() for i in args.infile if not i.startswith('#')],
            connection)
    else:
        uri = args.query
        experiments = encodedcc.get_ENCODE(uri, connection).get('@graph', [])
    logger.debug("get_experiments: Found %d experiments" % (len(experiments)))
    return experiments


def get_files(experiment, assembly, connection):
    DEPRECATED_FILE_STATUSES = ['deleted', 'revoked', 'replaced']
    uri = (
        "search/?type=File&dataset=/experiments/%s/&assembly=%s&lab.name=encode-processing-pipeline"
        % (experiment.get('accession'), assembly))
    for deprecated_status in DEPRECATED_FILE_STATUSES:
        uri += "&status!=%s" % (deprecated_status)
    files = encodedcc.get_ENCODE(uri, connection).get('@graph', [])
    for f in files:
        f.update({'dx_analysis': parent_analysis(f, connection)})

    reads_uri = (
        "search/?type=File&dataset=/experiments/%s/&output_type=reads"
        % (experiment.get('accession')))
    for deprecated_status in DEPRECATED_FILE_STATUSES:
        reads_uri += "&status!=%s" % (deprecated_status)
    files.extend(encodedcc.get_ENCODE(reads_uri, connection).get('@graph', []))
    logger.debug(
        "get_files:Found %s %s files in %s with statuses %s"
        % (len(files), assembly, experiment.get('accession'), set([f.get('status') for f in files])))
    return files


def get_graph(experiment, assembly, connection):
    files = get_files(experiment, assembly, connection)
    logger.debug(
        "get_graph: Found %s %s files in %s with statuses %s"
        % (len(files), assembly, experiment.get('accession'), set([f.get('status') for f in files])))
    graph = {}
    for file in files:
        file_accession = file.get('accession')
        for derived_from_uri in file.get('derived_from', []):
            derived_from_accession = uri_to_accession(derived_from_uri)
            if derived_from_accession in graph:
                graph[derived_from_accession].append(file_accession)
            else:
                graph.update({derived_from_accession: [file_accession]})
    logger.debug("%s nodes in graph: %s" % (len(graph), graph))
    return graph


# adapted from https://www.python.org/doc/essays/graphs/
def find_path(graph, start, end, path=[]):
    path = path + [start]
    if start == end:
        return path
    if start not in graph:
        return None
    for node in graph[start]:
        if node not in path:
            newpath = find_path(graph, node, end, path)
            if newpath:
                return newpath
    return None


# adapted from https://www.python.org/doc/essays/graphs/
def find_all_paths(graph, start, end, path=[]):
    path = path + [start]
    if start == end:
        return [path]
    if start not in graph:
        return []
    paths = []
    for node in graph[start]:
        if node not in path:
            newpaths = find_all_paths(graph, node, end, path)
            for newpath in newpaths:
                paths.append(newpath)
    return paths


def get_dx_output(output_key, analysis):
    dx_analysis = dxpy.describe(analysis['id'])
    stages = [stage['execution'] for stage in dx_analysis['stages']]
    stage_name, dot, output_name = output_key.partition(".")
    stage_outputs = next(s.get('output') for s in stages if s['name'] == stage_name)
    return dxpy.get_handler(stage_outputs[output_name])


def get_md5(dxfileh):
    property_md5 = dxfileh.get_properties().get('md5sum')
    if property_md5:
        return property_md5
    else:
        return None


def get_enc(output_key, analysis, connection):
    dx_output = get_dx_output(output_key, analysis)
    md5 = get_md5(dx_output)
    if md5:
        return encodedcc.get_ENCODE('/files/md5:%s' % (md5), connection)
    else:
        possible_tag_matches = [re.findall("ENCFF.{6}", tag)[0] for tag in dx_output.tags]
        if not possible_tag_matches:
            print("%s: Cannot find md5 or accession in tag" % (dx_output.get_id()))
            return None
        elif len(possible_tag_matches) > 1:
            print("%s: Has multiple ENCFF in tags: %s" % (dx_output.get_id(), possible_tag_matches))
            return None
        else:
            return encodedcc.get_ENCODE('/files/%s/' % (possible_tag_matches[0]), connection)


def patch(batch, connection):
    for patch_job in batch:
        accession, payload = next(patch_job.iteritems())
        response = encodedcc.patch_ENCODE(accession, connection, payload)
        # response = {'status': 'skipped'}
        print("%s\t%s\t%s" % (accession, payload, response.get('status')))


def supersede(analyses, connection, do_supersede):
    if len(analyses) == 1:
        print("Only one analysis found, nothing to supersede")
        return
    assert len(set([analysis.get('executableName') for analysis in analyses])) == 1, "Cannot supersede different analysis types"
    analyses.sort(key=itemgetter('created'), reverse=True)
    executable_name = analyses[0].get('executableName')
    patch_batch = []
    for output_key in ACCESSIONED_OUTPUTS[executable_name]:
        enc_files = [get_enc(output_key, analysis, connection) for analysis in analyses]
        print(("%s\t%s->%s" % (output_key, enc_files[0].get('accession'), [f.get('accession') for f in enc_files[1:]])), end='')
        new_file = enc_files[0]
        new_file_accession = new_file.get('accession')
        supersedes_files = [f.get('accession') for f in enc_files[1:] if f.get('accession')]
        if new_file_accession in supersedes_files:
            supersedes_files.remove(new_file_accession)
        if supersedes_files:
            supersedes_metadata = {'supersedes': list(set(["/files/%s/" % (acc) for acc in supersedes_files] + (new_file.get('supersedes') or [])))}
            patch_batch.append({new_file_accession: supersedes_metadata})
            for old_file_accession in supersedes_files:
                patch_batch.append({old_file_accession: {'status': 'revoked'}})
            print("\tqueued to patch %s" % (json.dumps(supersedes_metadata)))
        else:
            print("\tnothing to patch")
    if do_supersede:
        patch(patch_batch, connection)
    else:
        print("\tdryrun so no change")


def run(args, connection):
    experiments = get_experiments(args, connection)
    for experiment in experiments:
        files = get_files(experiment, args.assembly, connection)
        peaks_analyses = []
        for f in files:
            analysis = f.get('dx_analysis')
            if analysis and analysis.get('executableName') == "tf_chip_seq" and analysis not in peaks_analyses:
                peaks_analyses.append(analysis)
        if len(peaks_analyses) > 1:
            supersede(peaks_analyses, connection, args.supersede)

        # analyses = {}
        # for f in files:
        #     analysis = f.get('dx_analysis')
        #     if not analysis:
        #         continue
        #     if analysis.get('id') in analyses:
        #         analyses[analysis.get('id')]['files'].append(f.get('accession'))
        #     else:
        #         analyses.update({analysis.get('id'): {
        #             'files': [f.get('accession')],
        #             'name': analysis.get('name')
        #             }
        #         })
        # pprint(analyses)
        # graph = get_graph(experiment, args.assembly, connection)
        # biorepns = experiment_bioreps(experiment, connection)
        # for repn in biorepns:
        #     alignments = [f for f in files if f.get('output_category') == 'alignment' and repn in f.get('biological_replicates')]
        #     print("rep%d has %d alignments/analysis: %s" % (repn, len(alignments), [(a.get('accession'), parent_analysis(a, connection)) for a in alignments]))

        # leaves = {
        #     'peak_bbs': {
        #         'pool': [],
        #         'reps': {},
        #     },
        #     'signal_fcs': {
        #         'pool': [],
        #         'reps': {},
        #     },
        #     'signal_pvs': {
        #         'pool': [],
        #         'reps': {},
        #     },
        #     'idr_conservative_bb': [],
        #     'idr_optimal_bb': []
        # }
        # A = 'ENCFF001RUL'
        # B = 'ENCFF079EFI'
        # paths = find_all_paths(graph, A, B)
        # print("%d paths from %s to %s: %s" % (len(paths), A, B, paths))
    return True


def main():
    args = get_args()

    if args.debug:
        logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s', level=logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    else:  # use the defaulf logging level
        logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
        logger.setLevel(logging.INFO)

    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)

    return run(args, connection)


if __name__ == '__main__':
        main()
