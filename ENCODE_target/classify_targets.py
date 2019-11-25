#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import sys

import requests

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(name)s:%(levelname)s: %(message)s',
)
logger = logging.getLogger(__name__)


def encode_go_term_classes():
    go_obo_url = 'http://purl.obolibrary.org/obo/go.obo'
    encode_go_class_dict = {
        'GO:0003700': 'transcription factor',
        'GO:0003723': 'RNA binding protein',
        'GO:0001070': 'RNA binding protein',
        'GO:0008134': 'cofactor',
        'GO:0003712': 'cofactor',
        'GO:0005736': 'RNA polymerase complex',
        'GO:0016591': 'RNA polymerase complex',
        'GO:0005666': 'RNA polymerase complex',
        'GO:0006325': 'chromatin remodeler',
        'GO:0000118': 'chromatin remodeler',
        'GO:0007062': 'cohesin',
        'GO:0006260': 'DNA replication',
        'GO:0006281': 'DNA repair',
        'GO:0000788': 'histone',
        'GO:0003677': 'backup label-transcription factor',
        'GO:0003682': 'backup label-transcription factor',
        'GO:0043167': 'backup label-transcription factor',
    }
    term_dict = {}
    with requests.get(go_obo_url, stream=True) as go_obo:
        # https://owlcollab.github.io/oboformat/doc/GO.format.obo-1_4.html#S.2.2
        for stanza in go_obo.iter_lines(decode_unicode=True, delimiter='\n\n'):
            if not stanza:
                continue
            lines = stanza.split('\n')
            if len(lines) < 2:
                raise ValueError(
                    'Stanza "{}" has less than 2 lines.'.format(stanza)
                )
            stanza_type = lines[0]
            if stanza_type != '[Term]':
                continue
            if not lines[1].startswith('id: '):
                raise ValueError(
                    'Stanza "{}" fails to start with an id tag.'.format(stanza)
                )
            term_id = lines[1].split(': ')[1]
            term_dict[term_id] = set()
            for tag_value_pair in lines[2:]:
                tag, value = tag_value_pair.split(': ', maxsplit=1)
                if tag in ['is_a', 'alt_id', 'replaced_by']:
                    term_dict[term_id].add(value.split(' ', maxsplit=1)[0])
                    continue
                if tag in ['relationship', 'intersection_of']:
                    relationship, go_id, _ = value.split(' ', maxsplit=2)
                    # The following relations are defined by [Typedef] stanzas
                    if relationship in [
                        'part_of',
                        'regulates',
                        'negatively_regulates',
                        'positively_regulates',
                        'occurs_in'
                    ]:
                        term_dict[term_id].add(go_id)
    term_classifications = {}
    for term_id in term_dict:
        new_terms = {term_id} - term_dict[term_id]
        # Extend related terms until there is no new terms to be added
        # Self term ID is not added in the loop above so that there will be at
        # least one round of checking
        while new_terms:
            term_dict[term_id] |= new_terms
            new_terms = set()
            for related_term_id in term_dict[term_id]:
                new_terms |= term_dict.get(
                    related_term_id, set()
                ) - term_dict[term_id]
        # Get ENCODE classifications
        encode_go_classes = {
            encode_go_class_dict[related_term_id]
            for related_term_id in term_dict[term_id]
            if related_term_id in encode_go_class_dict
        }
        if not encode_go_classes:
            continue
        if (
            'backup label-transcription factor' in encode_go_classes
            and len(encode_go_classes) > 1
        ):
            encode_go_classes.remove('backup label-transcription factor')
        if encode_go_classes == {'Chromatin remodeler', 'Histone'}:
            encode_go_classes = {'Histone'}
        if encode_go_classes == {'chromatin remodeler', 'DNA replication'}:
            encode_go_classes = {'chromatin remodeler'}
        if encode_go_classes == {'DNA repair', 'DNA replication'}:
            encode_go_classes = {'DNA repair'}
        term_classifications[term_id] = list(encode_go_classes)
    fname = 'ENCODE_GO_map.json'
    logger.info('Save ENCODE GO category map to {}'.format(fname))
    with open(fname, 'x') as f:
        json.dump(term_classifications, f)
    return term_classifications


def classify_go_evidence(go_evidence_list, encode_go_classes):
    # Get best evidenced classes and their evidence count
    evidence_weight = {
        'EXP': 2,
        'IDA': 2,
        'IMP': 2,
        'IGI': 2,
        'IEP': 2,
        'HTP': 1,
        'HDA': 1,
        'HMP': 1,
        'HGI': 1,
        'HEP': 1,
        'TAS': 1,
        'IEA': -1
    }
    best_weight = -1
    best_evidenced_classes = {}
    for go_id, evidence in go_evidence_list:
        weight = evidence_weight.get(evidence, -1)
        if weight < best_weight:
            continue
        clazz = encode_go_classes.get(go_id)
        if not clazz:
            continue
        if weight > best_weight:
            best_weight = weight
            for c in clazz:
                best_evidenced_classes = {c: 1}
        elif weight == best_weight:
            for c in clazz:
                if c in best_evidenced_classes:
                    best_evidenced_classes[c] += 1
                else:
                    best_evidenced_classes[c] = 1
    if 'backup label-transcription factor' in best_evidenced_classes:
        if len(best_evidenced_classes) == 1:
            return ['transcription factor']
        best_evidenced_classes.pop('backup label-transcription factor')
    return [
        clz for clz in best_evidenced_classes
        if best_evidenced_classes[clz] == max(best_evidenced_classes.values())
    ]


def main():
    parser = argparse.ArgumentParser(
        description='Calculate ENCODE category for a target.'
    )
    parser.add_argument(
        '--uniprots',
        nargs='+',
        help='One or more UniProt ID(s). For example, Q9H9Z2'
    )
    parser.add_argument(
        '--mgis',
        nargs='+',
        help='One or more MGI ID(s). For example, 1890546'
    )
    parser.add_argument(
        '--fbs',
        nargs='+',
        help='One or more FlyBase ID(s). For example, FBgn0035626'
    )
    parser.add_argument(
        '--wbs',
        nargs='+',
        help='One or more WormBase ID(s). For example, WBGene00003014'
    )
    parser.add_argument(
        '--encode-go-map',
        default='ENCODE_GO_map.json',
        help='A JSON file mapping GO terms to ENCODE target categories.'
    )
    parser.add_argument(
        '--get-new-encode-go-map',
        action='store_true',
        help='A JSON file mapping GO terms to ENCODE target categories.'
    )
    args = parser.parse_args()
    if not (args.uniprots or args.mgis or args.fbs or args.wbs):
        parser.print_help()
        parser.exit(
            status=1,
            message='ERROR: At lease one ID from UniProtKB, MGI, FlyBase or '
            'WormBase is required!\n'
        )
    if args.get_new_encode_go_map:
        encode_go_dict = encode_go_term_classes()
    else:
        try:
            with open(args.encode_go_map) as f:
                encode_go_dict = json.load(f)
        except Exception as e:
            raise ValueError(
                'Fail to load ENCODE GO category map at {}. Please either '
                'provide a valid ENCODE GO category map in JSON format or use '
                '"--get-new-encode-go-map" and ensure internet connection.'
            ) from e
    bioentity_ids = set()
    if args.uniprots:
        bioentity_ids |= {'"UniProtKB:{}"'.format(i) for i in args.uniprots}
    if args.mgis:
        bioentity_ids |= {'"MGI:MGI:{}"'.format(i) for i in args.mgis}
    if args.fbs:
        bioentity_ids |= {'"FB:{}"'.format(i) for i in args.fbs}
    if args.wbs:
        bioentity_ids |= {'"WB:{}"'.format(i) for i in args.wbs}
    filter_query = ' OR '.join(bioentity_ids)
    golr_base_url = 'http://golr-aux.geneontology.io/solr/select?fq=document_category:"annotation"&q=*:*&fq=bioentity:({})&rows={}&wt=json'  # noqa: E501
    rows_count = requests.get(
        golr_base_url.format(filter_query, 1)
    ).json()['response']['numFound']
    print(golr_base_url.format(filter_query, rows_count))
    go_evidence = [
        (annotation['annotation_class'], annotation['evidence_type'])
        for annotation in requests.get(
            golr_base_url.format(filter_query, rows_count)
        ).json()['response']['docs']
    ]
    logger.info(classify_go_evidence(go_evidence, encode_go_dict))


if __name__ == "__main__":
    # execute only if run as a script
    main()
