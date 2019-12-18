#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import os

from lxml import etree
from xmldiff import main as xmldiff_main
from xmldiff import formatting


def main():
    parser = argparse.ArgumentParser(
        description='Compare/Diff two folders of EpiRR XML submission.'
    )
    parser.add_argument(
        'old_dir', type=str, help='The directory for old submission.'
    )
    parser.add_argument(
        'new_dir', type=str, help='The directory for new submission.'
    )
    args = parser.parse_args()
    if set(os.listdir(args.old_dir)) != set(os.listdir(args.new_dir)):
        print(
            'New submission has a different set of reference epigenomes '
            'comparing to the old submission. Please check.'
        )
    for f in os.listdir(args.old_dir):
        old_file = os.path.join(args.old_dir, f)
        new_file = os.path.join(args.new_dir, f)
        if f.endswith('.refepi.json'):
            with open(old_file) as f:
                old_json = json.load(f)
            old_json['raw_data'] = sorted(
                old_json['raw_data'], key=lambda d: d['primary_id']
            )
            with open(new_file) as f:
                new_json = json.load(f)
            new_json['raw_data'] = sorted(
                new_json['raw_data'], key=lambda d: d['primary_id']
            )
            if old_json != new_json:
                print(f)
            continue

        if not f.endswith('.validated.xml'):
            continue

        diff = xmldiff_main.diff_files(
            old_file, new_file, formatter=formatting.DiffFormatter()
        )
        if diff:
            old_tree = etree.parse(
                os.path.join(args.old_dir, f),
                etree.XMLParser(remove_blank_text=True)
            )
            old_root = old_tree.getroot()
            old_root[:] = sorted(old_root, key=lambda e: e.get('accession'))
            new_tree = etree.parse(
                os.path.join(args.new_dir, f),
                etree.XMLParser(remove_blank_text=True)
            )
            new_root = new_tree.getroot()
            new_root[:] = sorted(new_root, key=lambda e: e.get('accession'))
            sorted_diff = xmldiff_main.diff_trees(
                old_tree,
                new_tree,
                formatter=formatting.DiffFormatter()
            )
            if sorted_diff:
                print(f)
                print(sorted_diff)
                # break


if __name__ == '__main__':
    # execute only if run as a script
    main()
