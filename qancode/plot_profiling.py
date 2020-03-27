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


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("testurl", action='store', help="""URL of the RC or demo.""")
    parser.add_argument("-i", "--items", action='store', help="""Items to check.""")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    rc_url = args.testurl
    item_types = args.items
    if item_types is not None:
        item_types = item_types.split(',')
    else:
        item_types = [
            "/search/?type=Experiment&biosample_ontology.term_name=whole+organism",
            "/search/?type=Experiment&target.label=H3K4me3",
            "/search/?type=Experiment&assay_slims=DNA+methylation",
            "/search/?type=Experiment&status=released",
            "/matrix/?type=Experiment&status=released",
            "/search/?searchTerm=hippocampus",
            "/experiments/ENCSR079YAP/",
            "/experiments/ENCSR296ASC/",
            "/publication-data/ENCSR089EOA/"
        ]
    prod_url = "https://test.encodedcc.org"

    if os.path.exists(os.path.expanduser('~/profiling_output')):
        if os.path.exists(os.path.expanduser('~/profiling_output_archive')):
            shutil.rmtree(os.path.expanduser('~/profiling_output_archive'))
        shutil.copytree(os.path.expanduser('~/profiling_output'), os.path.expanduser('~/profiling_output_archive'))
        shutil.rmtree(os.path.expanduser('~/profiling_output'))
    os.mkdir(os.path.expanduser('~/profiling_output'))

    print(datetime.datetime.now())
    qa = QANCODE(rc_url=rc_url, prod_url=prod_url)
    num_trials = 50
    info = qa.check_response_time(
        item_types=item_types, n=num_trials, output_path=os.path.expanduser('~/profiling_output/check_response_time.txt')
    )

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

    plt.savefig(
        os.path.expanduser('~/profiling_output/profiling_plot_{}.png'.format(
            datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')
        )),
        bbox_inches='tight'
    )
    print(datetime.datetime.now())


if __name__ == '__main__':
    main()