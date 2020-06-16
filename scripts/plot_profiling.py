#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import datetime
import glob
import logging
import os
import sys

import numpy as np

from matplotlib import pyplot as plt
from textwrap import wrap

from qancode import QANCODE


_ITEM_TYPES = [
    "/search/?type=Experiment&biosample_ontology.term_name=whole+organism",
    "/search/?type=Experiment&target.label=H3K4me3",
    "/search/?type=Experiment&assay_slims=DNA+methylation",
    "/search/?type=Experiment&status=released",
    "/matrix/?type=Experiment&status=released",
    "/search/?searchTerm=hippocampus",
    "/experiments/ENCSR079YAP/",
    "/experiments/ENCSR296ASC/",
    "/publication-data/ENCSR089EOA/",
]


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('dut_tag', action='store', help='Device under test tag. encd-52 or v101rc1')
    parser.add_argument('dut_url', action='store', help='Device undert test url.')
    parser.add_argument(
        'std_url',
        action='store',
        default='https://test.encodedcc.org',
        help='URL of standard to compare dut.'
    )
    parser.add_argument('-i', '--item-types', nargs='+', default=_ITEM_TYPES, help='Items to check.')
    parser.add_argument('-n', '--number-of-trials', type=int, default=10, help='Number of runs.')
    # Flags
    parser.add_argument('-a', '--alt-format', action='store_true', help='Create alt output txt')
    parser.add_argument('-d', '--dry-run', action='store_true', help='Print info, setup output dir.')
    parser.add_argument('-o', '--output-append', action='store_true', help='Single output file')
    parser.add_argument('-p', '--plot-data', action='store_true', help='Create plot png')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print more!')
    args = parser.parse_args()
    return args


def _setup_data_dir(dut_tag, all_data_dir='./profiling_output', output_append=False):
    all_data_dir = os.path.expanduser(all_data_dir)
    if not os.path.exists(all_data_dir):
        os.mkdir(all_data_dir)
    tag_data_dir = f"{all_data_dir}/{dut_tag}"
    if not os.path.exists(tag_data_dir):
        os.mkdir(tag_data_dir)
    next_run_number=0
    highest=next_run_number
    for file_path in glob.glob(f"{tag_data_dir}/[0-9]*_*"):
        run_number = int(os.path.basename(file_path).split('_', 1)[0])
        if run_number > next_run_number:
            next_run_number = run_number
    if not output_append:
        next_run_number += 1
    tag_data_dir = f"{tag_data_dir}/{next_run_number}"
    return tag_data_dir
   

def _save_pyplt(output_path, std_url, dut_url, item_types, info):
    std_url_short = std_url.split('/')[2]
    dut_url_short = dut_url.split('/')[2]
    xticklabels = []
    response_time_types = [
        'es_time', 'queue_time', 'render_time', 'wsgi_time', 'total_time'
    ]

    fig, axes = plt.subplots(
        5, 2, sharex=True, sharey='row', figsize=(max(12, 3*len(item_types)), 20)
    )
    for i in range(0,5):
        axes[i, 0].grid(True, axis='y', color='#B5B5B5', linestyle='--', linewidth=1)
        axes[i, 1].grid(True, axis='y', color='#B5B5B5', linestyle='--', linewidth=1)

    fig.add_subplot(
        111, frameon=False, xticks=[0], yticks=[0]
    )  # Large plot for the Title
    plt.tick_params(
        labelcolor='none', top=False, bottom=False, left=False, right=False
    )
    plt.grid(False)
    plt.xlabel('Item type', fontsize=20, labelpad=60)
    plt.ylabel('Response time, ms', fontsize=20, labelpad=30)
    plt.title('Profiling Results', fontsize=20, pad=30)
    x = np.arange(0, float(len(item_types)))

    # Check response time returns a nested dict organized in this structure:
    # {item_type: {url: {response_type: (average, stdev)}}}
    # This iterates over each item_type and then response_type
    i = 0
    for item in info:
        xticklabels.append('\n'.join(wrap(item, 13)))
        for j in range(0, 5):
            response_time_type = response_time_types[j]
            ax1 = axes[j, 0]
            ax2 = axes[j, 1]
            try:
                ax1.errorbar(
                    x[i],
                    info[item][std_url_short][response_time_type][0],
                    yerr=info[item][std_url_short][response_time_type][1],
                    fmt='b_',
                    linestyle=''
                )
                ax2.errorbar(
                    x[i],
                    info[item][dut_url_short][response_time_type][0],
                    yerr=info[item][dut_url_short][response_time_type][1],
                    fmt='b_',
                    linestyle=''
                )
            except KeyError:
                # format=json queries don't have render time - this will fill in a
                # 0 value for those.
                ax1.errorbar(x[i], 0, yerr=0, fmt='b_', linestyle='')
                ax2.errorbar(x[i], 0, yerr=0, fmt='b_', linestyle='')
        ax1.autoscale(axis='y')
        i = i + 1

    axes[0, 0].set_title(std_url)
    axes[0, 1].set_title(dut_url)
    axes[0, 0].set_ylabel('ES time')
    axes[1, 0].set_ylabel('Queue time')
    axes[2, 0].set_ylabel('Render time')
    axes[3, 0].set_ylabel('WSGI time')
    axes[4, 0].set_ylabel('Total time')
    axes[4, 0].set_xticks(x)
    axes[4, 0].set_xticklabels(xticklabels)
    axes[4, 1].set_xticklabels(xticklabels)

    plt.savefig(output_path, bbox_inches='tight')


def main():
    args = _parse_args()
    # Set data output directory
    tag_data_dir = _setup_data_dir(args.dut_tag, output_append=args.output_append)
    check_response_output_path = f"{tag_data_dir}_check-response-time.txt"
    check_response_output_plot_path = f"{tag_data_dir}_check-response-time-plot"
    # Create qancode
    qancode = QANCODE(rc_url=args.dut_url, prod_url=args.std_url)
    # Info output
    print(f"\nComparing {args.dut_url} to {args.std_url} with {args.number_of_trials} trials")
    if args.verbose:
        print(f"tag_data_dir: {tag_data_dir}")
        print(f"check_response_output_path: {check_response_output_path}")
        print(f"check_response_output_plot_path: {check_response_output_plot_path}")
    if args.dry_run:
        print('Dry Run Over')
        sys.exit(0)
    # Start run
    start_time = datetime.datetime.now()
    print(f"\nStart Time: {start_time}")
    info = qancode.check_response_time(
        item_types=args.item_types, 
        n=args.number_of_trials, 
        output_path=check_response_output_path,
        alt_format=args.alt_format,
        append_output=args.output_append,
    )
    end_time = datetime.datetime.now()
    end_time_str=end_time.strftime('%Y-%m-%d_%H-%M-%S_%f')
    print(f"End Time: {end_time}")
    print(f"Ran in {end_time - start_time}")
    # Plot Data
    if args.plot_data:
        _save_pyplt(
            f"{check_response_output_plot_path}_{end_time_str}.png",
            args.std_url,
            args.dut_url,
            args.item_types,
            info,
        )


if __name__ == '__main__':
    main()
