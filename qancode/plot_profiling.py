#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import os
import sys
import shutil

from matplotlib import pyplot as plt
import numpy as np
from textwrap import wrap

sys.path.insert(0, os.path.expanduser('~/pyencoded-tools'))
from qancode.qancode import QANCODE


DEFAULT_ITEM_TYPES = [
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


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("test_tag", action='store', help="Test demo tag, encd-5277 or v101rc1")
    parser.add_argument("test_url", action='store', help="""URL of the RC or demo.""")
    parser.add_argument(
        "compare_url",
        action='store',
        default='https://test.encodedcc.org',
        help="""URL of standard to compare test url."""
    )
    parser.add_argument("--dry-run", action='store_true', help="""Just print some stuff.""")
    parser.add_argument("-n", '--number-of-trials', type=int, default=50, help="Number of runs.")
    parser.add_argument("-i", "--items", nargs='+', default=DEFAULT_ITEM_TYPES, help="""Items to check.""")
    return parser


def _setup_data_dir(rc_tag, all_data_dir='~/profiling_output'):
    import glob
    all_data_dir = os.path.expanduser(all_data_dir)
    if not os.path.exists(all_data_dir):
        os.mkdir(all_data_dir)
    tag_data_dir = f"{all_data_dir}/{rc_tag}"
    if not os.path.exists(tag_data_dir):
        os.mkdir(tag_data_dir)
    next_run_number=0
    highest=next_run_number
    for file_path in glob.glob(f"{tag_data_dir}/[0-9]*_*"):
        run_number = int(os.path.basename(file_path).split('_', 1)[0])
        if run_number > next_run_number:
            next_run_number = run_number
    next_run_number += 1
    tag_data_dir = f"{tag_data_dir}/{next_run_number}"
    return tag_data_dir
   

def _save_pyplt(output_path, prod_url, rc_url, item_types, info):
    prod_url_short = prod_url.split('/')[2]
    test_url_short = rc_url.split('/')[2]
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
                    info[item][prod_url_short][response_time_type][0],
                    yerr=info[item][prod_url_short][response_time_type][1],
                    fmt='b_',
                    linestyle=''
                )
                ax2.errorbar(
                    x[i],
                    info[item][test_url_short][response_time_type][0],
                    yerr=info[item][test_url_short][response_time_type][1],
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

    axes[0, 0].set_title(prod_url)
    axes[0, 1].set_title(rc_url)
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
    parser = get_parser()
    args = parser.parse_args()
    rc_tag = args.test_tag
    rc_url = args.test_url
    prod_url = args.compare_url
    num_trials = 10 # args.number_of_trials
    item_types = args.items
    tag_data_dir = _setup_data_dir(rc_tag)
    check_response_output_path = f"{tag_data_dir}_check-response-time.txt"
    check_response_output_plot_path = f"{tag_data_dir}_check-response-time-plot"
    print(f"Comparing {rc_url} to {prod_url} with {num_trials} trials")
    print(f"tag_data_dir: {tag_data_dir}")
    print(f"check_response_output_path: {check_response_output_path}")
    print(f"check_response_output_plot_path: {check_response_output_plot_path}")
    qa = QANCODE(rc_url=rc_url, prod_url=prod_url)
    if args.dry_run:
        print('Dry Run Over')
        sys.exit(0)
    start_time = datetime.datetime.now()
    print(f"Start Time: {start_time}")
    info = qa.check_response_time(
        item_types=item_types, 
        n=num_trials, 
        output_path=check_response_output_path,
    )
    end_time = datetime.datetime.now()
    end_time_str=end_time.strftime('%Y-%m-%d_%H-%M-%S_%f')
    _save_pyplt(f"{check_response_output_plot_path}_{end_time_str}.png", prod_url, rc_url, item_types, info)
    print(f"End Time: {end_time}")
    print(f"Ran in {end_time - start_time}")


if __name__ == '__main__':
    main()
