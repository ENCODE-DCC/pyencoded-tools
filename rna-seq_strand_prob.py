#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import io

from qcmodule import SAM


def main():
    sample_size_default = 2000000
    map_quality_default = 30
    parser = argparse.ArgumentParser(
        description='Script to guess strandness of RNA-seq from BAM'
    )
    parser.add_argument(
        '-i',
        '--input',
        help='One input file in TSV format with the following four columns:'
        ' BAM path, gene model BED path, sample size, map quality cutoff.'
        ' There should be no header and columns must be ordered as above.'
        ' There must be four columns, i.e. three tabs. Sample size and map'
        ' quality cutoff can be empty which means default value will be used.'
    )
    parser.add_argument(
        '-a', '--alignment', help='Input alignment file in SAM or BAM format.'
    )
    parser.add_argument(
        '-r', '--refgene', help='Reference gene model in bed fomat.'
    )
    parser.add_argument(
        '-s',
        '--sample-size',
        type=int,
        default=sample_size_default,
        help='Number of reads sampled from SAM/BAM file. default=%(default)d.'
        ' Will be ignored if an input file is provided.'
    )	
    parser.add_argument(
        '-q',
        '--mapq',
        type=int,
        default=map_quality_default,
        help='Minimum mapping quality (phred scaled) for an alignment to be'
        ' considered as "uniquely mapped". default=%(default)d. Will be'
        ' ignored if an input file is provided.'
    )
    args = parser.parse_args()
    # Require either an input file or one set of alignment + gene model
    if (not args.input) and (not (args.alignment or args.refgene)):
        parser.error(
            'Need either an input file or an alignment with gene model.'
        )
    if args.input and (args.alignment or args.refgene):
        parser.error(
            'Cannot have both an input file and an alignment with gene model;'
            ' only one is allowed.'
        )
    if (
        (args.alignment and not args.refgene)
        or (not args.alignment and args.refgene)
    ):
        parser.error('Alignment and gene model must be provided together.')

    if args.input:
        input_stream = open(args.input)
    else:
        input_stream = io.StringIO(
            f'{args.alignment}\t{args.refgene}\t{args.sample_size}\t{args.mapq}'
        )
    with input_stream as input:
        print(
            'Alignment\tGene model\tSample size\tMap quality cutoff'
            '\tRun type\t% Forward\t% reverse\t% undetermined'
        )
        for l in input:
            if not l.rstrip():
                continue
            options = l.rstrip().split('\t')
            if len(options) < 2:
                raise ValueError(
                    'Couldn\'t get valid alignment and gene model:'
                    f' {l.rstrip()}'
                )
            alignment_path = options[0]
            gene_model_path = options[1]
            sample_size = sample_size_default
            if len(options) > 2:
                sample_size = int(options[2])
            map_quality_cutoff = map_quality_default
            if len(options) > 3:
                map_quality_cutoff = int(options[3])
            obj = SAM.ParseBAM(alignment_path)
            protocol, sp1, sp2, other = obj.configure_experiment(
                refbed=gene_model_path,
                sample_size=sample_size,
                q_cut=map_quality_cutoff,
            )
            print(
                f'{alignment_path}\t{gene_model_path}\t{sample_size}'
                f'\t{map_quality_cutoff}\t{protocol}\t{sp1}\t{sp2}\t{other}'
            )


if __name__ == '__main__':
    main()
