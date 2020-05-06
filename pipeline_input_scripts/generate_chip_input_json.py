import argparse
import json
import os
import pandas as pd
from pandas.io.json import json_normalize
import requests


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-i', "--infile", required=True, action='store',
                        help="""Path to .txt file containing accessions of experiments to process or list of accessions separated by commas.""")
    parser.add_argument('-o', '--outputpath', action='store', default='',
                        help="""Optional path to output folder. Defaults to current path.""")
    parser.add_argument('-g', '--gcpath', action='store', default='',
                        help="""Optional path where the input.json will be uploaded to the Google Cloud instance. Only affects the list of caper commands that is generated.""")
    parser.add_argument('--wdl', action='store', default=False,
                        help="""Path to .wdl file.""")
    parser.add_argument('-s', '--server', action='store', default='https://www.encodeproject.org/',
                        help="""Optional specification of server using the full URL. Defaults to production server.""")
    parser.add_argument('--use-s3-uris', action='store_true', default=False,
                        help="""Optional flag to use s3_uri links. Otherwise, defaults to using @@download links from the ENCODE portal.""")
    parser.add_argument('--align-only', action='store', default=False,
                        help="""Pipeline will end after alignments step if True.""")
    parser.add_argument('--custom-message', action='store',
                        help="""An additional custom string to be appended to the messages in the caper submit commands.""")
    return parser


def check_path_trailing_slash(path):
    if path.endswith('/') or len(path) == 0:
        return path
    else:
        return path + '/'


def build_experiment_report_query(experiment_list, server):
    joined_list = '&accession='.join(experiment_list)
    return server + '/report/?type=Experiment' + \
        '&accession={}'.format(joined_list) + \
        '&field=@id' + \
        '&field=accession' + \
        '&field=assay_title' + \
        '&field=control_type' + \
        '&field=possible_controls' + \
        '&field=files.s3_uri' + \
        '&field=files.href' + \
        '&field=replicates.library.biosample.organism.scientific_name' + \
        '&limit=all' + \
        '&format=json'


def build_file_report_query(experiment_list, server):
    joined_list = '&dataset='.join(experiment_list)
    return server + '/report/?type=File' + \
        '&dataset={}'.format(joined_list) + \
        '&status=released' + \
        '&status=in+progress' + \
        '&award.rfa=ENCODE4' + \
        '&award.rfa=ENCODE3' + \
        '&assembly!=hg19' + \
        '&assembly!=mm9' + \
        '&file_format=fastq' + \
        '&file_format=bam' + \
        '&output_type=reads' + \
        '&output_type=alignments' + \
        '&field=@id' + \
        '&field=dataset' + \
        '&field=file_format' + \
        '&field=biological_replicates' + \
        '&field=paired_end' + \
        '&field=paired_with' + \
        '&field=run_type' + \
        '&field=read_length' + \
        '&field=cropped_read_length' + \
        '&field=status' + \
        '&field=s3_uri' + \
        '&field=href' + \
        '&field=replicate.status' + \
        '&limit=all' + \
        '&format=json'


def parse_infile(infile):
    try:
        infile_df = pd.read_csv(infile, '\t')
        infile_df['align_only'].astype('bool')
        return infile_df
    except FileNotFoundError as e:
        print(e)
        exit()


def strs2bool(strings):
    out = []
    for string in strings:
        if string == "True":
            out.append(True)
        elif string == "False":
            out.append(False)
    return out


def main():
    keypair = (os.environ.get('DCC_API_KEY'), os.environ.get('DCC_SECRET_KEY'))
    parser = get_parser()
    args = parser.parse_args()
    allowed_statuses = ['released', 'in progress']

    output_path = check_path_trailing_slash(args.outputpath)
    wdl_path = args.wdl
    gc_path = args.gcpath

    server= args.server
    use_s3 = args.use_s3_uris
    if use_s3:
        link_prefix = ''
        link_src = 's3_uri'
    else:
        link_prefix = server
        link_src = 'href'

    if args.infile.endswith('.txt') or args.infile.endswith('.tsv'):
        infile_df = parse_infile(args.infile)
        infile_df.sort_values(by=['accession'], inplace=True)
    else:
        accession_list = args.infile.split(',')
        align_only = strs2bool(args.align_only.split(','))
        message = args.custom_message.split(',')
        infile_df = pd.DataFrame({
            'accession':accession_list,
            'align_only': align_only,
            'custom_message': message
        })
        infile_df.sort_values(by=['accession'], inplace=True)

    # Arrays to store lists of potential errors.
    ERROR_no_fastqs = []
    ERROR_control_error_detected = []
    ERROR_not_matching_endedness = []

    '''
    Fetch data from the ENCODE portal
    '''

    # Retrieve experiment report view json with necessary fields and store as DataFrame.
    experiment_input_df = pd.DataFrame()
    experiment_accessions = infile_df['accession'].tolist()
    chunked_experiment_accessions = [experiment_accessions[x:x+100] for x in range(0, len(experiment_accessions), 100)]
    for chunk in chunked_experiment_accessions:
        experiment_report = requests.get(
            build_experiment_report_query(chunk, server),
            auth=keypair,
            headers={'content-type': 'application/json'})
        experiment_report_json = json.loads(experiment_report.text)
        experiment_df_temp = json_normalize(experiment_report_json['@graph'])
        experiment_input_df = experiment_input_df.append(experiment_df_temp, ignore_index=True, sort=True)
    experiment_input_df.sort_values(by=['accession'], inplace=True)

    # Fill in columns that may be missing
    if 'control_type' not in experiment_input_df:
        experiment_input_df['control_type'] = None

    # Retrieve list of wildtype controls
    wildtype_ctl_query_res = requests.get(
        link_prefix+'/search/?type=Experiment&assay_title=Control+ChIP-seq&assay_title=Control+Mint-ChIP-seq&assay_title=Mint-ChIP-seq&control_type=*&replicates.library.biosample.applied_modifications%21=%2A&limit=all',
        auth=keypair,
        headers={'content-type': 'application/json'})
    wildtype_ctl_ids = [ctl['@id'] for ctl in json.loads(wildtype_ctl_query_res.text)['@graph']]

    # Gather list of controls from the list of experiments to query for their files.
    datasets_to_retrieve = experiment_input_df.get('@id').tolist()
    for ctl in experiment_input_df.get('possible_controls'):
        for item in ctl:
            datasets_to_retrieve.append(item['@id'])

    # Retrieve file report view json with necessary fields and store as DataFrame.
    file_input_df = pd.DataFrame()
    chunked_dataset_accessions = [datasets_to_retrieve[x:x+100] for x in range(0, len(datasets_to_retrieve), 100)]
    for chunk in chunked_dataset_accessions:
        file_report = requests.get(
            build_file_report_query(chunk, server),
            auth=keypair,
            headers={'content-type': 'application/json'})
        file_report_json = json.loads(file_report.text)
        file_df_temp = json_normalize(file_report_json['@graph'])
        file_input_df = file_input_df.append(file_df_temp, ignore_index=True, sort=True)
    file_input_df.set_index(link_src, inplace=True)
    if 'paired_end' not in file_input_df:
        file_input_df['paired_end'] = None
        file_input_df['paired_with'] = None
    file_input_df['biorep_scalar'] = [x[0] for x in file_input_df['biological_replicates']]

    # Create output_df to store all data for the final input.json files.
    output_df = pd.DataFrame()
    output_df['chip.title'] = infile_df['accession']
    output_df['chip.align_only'] = infile_df['align_only']
    if 'message' in infile_df:
        output_df['custom_message'] = infile_df['message']
        output_df['custom_message'].fillna('', inplace=True)
    else:
        output_df['custom_message'] = ''
    output_df.set_index('chip.title', inplace=True, drop=False)

    # Assign blacklist(s) and genome reference file.
    blacklist = []
    blacklist2 = []
    genome_tsv = []
    chrom_sizes = []
    for assay, replicates in zip(experiment_input_df.get('assay_title'), experiment_input_df.get('replicates')):
        organism = set()
        for rep in replicates:
            organism.add(rep['library']['biosample']['organism']['scientific_name'])

        if ''.join(organism) == 'Homo sapiens':
            genome_tsv.append('gs://encode-pipeline-genome-data/genome_tsv/v2/hg38_gcp.tsv')
            chrom_sizes.append('https://www.encodeproject.org/files/GRCh38_EBV.chrom.sizes/@@download/GRCh38_EBV.chrom.sizes.tsv')
            if assay == 'Mint-ChIP-seq':
                blacklist.append('https://www.encodeproject.org/files/ENCFF356LFX/@@download/ENCFF356LFX.bed.gz')
                blacklist2.append('https://www.encodeproject.org/files/ENCFF023CZC/@@download/ENCFF023CZC.bed.gz')
            elif assay in ['Histone ChIP-seq', 'TF ChIP-seq', 'Control ChIP-seq']:
                blacklist.append('https://www.encodeproject.org/files/ENCFF356LFX/@@download/ENCFF356LFX.bed.gz')
                blacklist2.append(None)
        elif ''.join(organism) == 'Mus musculus':
            genome_tsv.append('gs://encode-pipeline-genome-data/genome_tsv/v1/mm10_gcp.tsv')
            chrom_sizes.append('https://www.encodeproject.org/files/mm10_no_alt.chrom.sizes/@@download/mm10_no_alt.chrom.sizes.tsv')
            if assay == 'Mint-ChIP-seq':
                blacklist.append(None)
                blacklist2.append(None)
            elif assay in ['Histone ChIP-seq', 'TF ChIP-seq', 'Control ChIP-seq']:
                blacklist.append('https://www.encodeproject.org/files/ENCFF547MET/@@download/ENCFF547MET.bed.gz')
                blacklist2.append(None)
    output_df['chip.blacklist'] = blacklist
    output_df['chip.blacklist2'] = blacklist2
    output_df['chip.genome_tsv'] = genome_tsv
    output_df['chip.chrsz'] = chrom_sizes

    '''
    Experiment sorting section
    '''

    # Determine pipeline types.
    pipeline_type = []
    for assay, ctl_type in zip(experiment_input_df.get('assay_title'), experiment_input_df.get('control_type')):
        if pd.notna(ctl_type) or assay in ['Control ChIP-seq', 'Control Mint-ChIP-seq']:
            pipeline_type.append('control')
        elif assay == 'TF ChIP-seq':
            pipeline_type.append('tf')
        elif assay in ['Histone ChIP-seq', 'Mint-ChIP-seq']:
            pipeline_type.append('histone')

    # Arrays which will be added to the master Dataframe for all experiments
    crop_length = []
    fastqs_by_rep_R1_master = {
        1: [], 2: [],
        3: [], 4: [],
        5: [], 6: [],
        7: [], 8: [],
        9: [], 10: []
    }
    fastqs_by_rep_R2_master = {
        1: [], 2: [],
        3: [], 4: [],
        5: [], 6: [],
        7: [], 8: [],
        9: [], 10: []
    }
    # Store experiment read lengths and run types for comparison against controls
    experiment_min_read_lengths = []
    experiment_run_types = []

    for experiment_files, experiment_id in zip(experiment_input_df['files'], experiment_input_df['@id']):
        # Arrays for files within each experiment
        fastqs_by_rep_R1 = {
            1: [], 2: [],
            3: [], 4: [],
            5: [], 6: [],
            7: [], 8: [],
            9: [], 10: []
        }
        fastqs_by_rep_R2 = {
            1: [], 2: [],
            3: [], 4: [],
            5: [], 6: [],
            7: [], 8: [],
            9: [], 10: []
        }
        experiment_read_lengths = []
        run_types = set()

        for file in experiment_files:
            link = file[link_src]
            if link.endswith('fastq.gz') \
                    and link in file_input_df.index \
                    and file_input_df.loc[link].at['status'] in allowed_statuses \
                    and file_input_df.loc[link].at['replicate.status'] in allowed_statuses:
                if file_input_df.loc[link].at['paired_end'] == '1':
                    pair = file_input_df.loc[link].at['paired_with']
                    for rep_num in fastqs_by_rep_R1:
                        if file_input_df.loc[link].at['biorep_scalar'] == rep_num:
                            fastqs_by_rep_R1[rep_num].append(link_prefix + link)
                            fastqs_by_rep_R2[rep_num].append(link_prefix + file_input_df[file_input_df['@id'] == pair].index.values[0])
                elif pd.isnull(file_input_df.loc[link].at['paired_end']):
                    for rep_num in fastqs_by_rep_R1:
                        if file_input_df.loc[link].at['biorep_scalar'] == rep_num:
                            fastqs_by_rep_R1[rep_num].append(link_prefix + link)
                            fastqs_by_rep_R2[rep_num].append(None)

                # Collect read_lengths and run_types
                experiment_read_lengths.append(file_input_df.loc[link].at['read_length'])
                run_types.add(file_input_df.loc[link].at['run_type'])

        # Record error if no fastqs for found for any replicate.
        if all(val == [] for val in fastqs_by_rep_R1.values()):
            print('ERROR: no fastqs were found for {}.'.format(experiment_id))
            ERROR_no_fastqs.append(experiment_id)

        # Fix ordering of reps to prevent non-consecutive numbering.
        for k in list(range(1, 11)):
            if fastqs_by_rep_R1[k] == []:
                for i in list(range(k+1, 11)):
                    if fastqs_by_rep_R1[i] != []:
                        fastqs_by_rep_R1[k] = fastqs_by_rep_R1[i]
                        fastqs_by_rep_R2[k] = fastqs_by_rep_R2[i]
                        fastqs_by_rep_R1[i] = []
                        fastqs_by_rep_R2[i] = []
                        break
                    else:
                        continue

        # Add the replicates to the master list.
        for rep_num in fastqs_by_rep_R1_master:
            fastqs_by_rep_R1_master[rep_num].append(fastqs_by_rep_R1[rep_num])
            fastqs_by_rep_R2_master[rep_num].append(fastqs_by_rep_R2[rep_num])

        experiment_min_read_lengths.append(min(experiment_read_lengths))
        experiment_run_types.append(run_types)

    '''
    Control sorting section
    '''

    ctl_nodup_bams = []
    control_run_types = []
    for control, experiment, is_control, experiment_read_length in zip(
            experiment_input_df['possible_controls'],
            experiment_input_df['accession'],
            pipeline_type,
            experiment_min_read_lengths
    ):
        if is_control == 'control':
            ctl_nodup_bams.append(None)
            control_run_types.append(None)
            crop_length.append(experiment_read_length)
        elif control == []:
            print('ERROR: No controls in possible_controls for experiment {}.'.format(experiment))
            ERROR_control_error_detected.append(experiment)
            ctl_nodup_bams.append(None)
            control_run_types.append(None)
            crop_length.append(None)
        else:
            # Go through the list of controls and identify the wildtype control.
            control_id = None
            for ctl in control:
                if ctl['@id'] in wildtype_ctl_ids:
                    control_id = ctl['@id']
                    break
            if control_id is not None:
                pass
            else:
                print('ERROR: Could not locate wildtype control for {}.'.format(experiment))
                ERROR_control_error_detected.append(experiment)
                ctl_nodup_bams.append(None)
                control_run_types.append(None)
                continue

            # Identify run_types in the control
            run_types = set(file_input_df[
                    (file_input_df['dataset'] == control_id) &
                    (file_input_df['file_format'] == 'fastq')
                    ].get('run_type'))
            control_run_types.append(run_types)

            # Collect read_lengths in the control
            control_read_lengths = file_input_df[
                    (file_input_df['dataset'] == control_id) &
                    (file_input_df['file_format'] == 'fastq')
                    ].get('read_length').tolist()

            # Select the minimum read length out of the files in the experiment and its control, and store the value.
            combined_minimum_read_length = min([experiment_read_length] + control_read_lengths)
            crop_length.append(combined_minimum_read_length)

            # Gather control bams based on matching read_length
            try:
                ctl_nodup_temp_collector = []
                for rep_num in list(range(1, 11)):
                    ctl_search = file_input_df[
                        (file_input_df['dataset'] == control_id) &
                        (file_input_df['biorep_scalar'] == rep_num) &
                        (file_input_df['file_format'] == 'bam') &
                        (file_input_df['cropped_read_length'] < combined_minimum_read_length + 2) &
                        (file_input_df['cropped_read_length'] > combined_minimum_read_length - 2)
                        ]
                    if not ctl_search.empty:
                        ctl_nodup_temp_collector.append(link_prefix + ctl_search.index.values[0])
                ctl_nodup_bams.append(ctl_nodup_temp_collector)
            except:
                print('ERROR: no bams found for {}.'.format(experiment))
                ctl_nodup_bams.append(None)
                ERROR_control_error_detected.append(experiment)

    '''
    Assign all remaining missing properties in the master dataframe.
    '''

    # Check that all fastqs in experiment and its control have the same endedness.
    paired_end_final = []
    for expt_rt, ctl_rt, experiment_accession in zip(experiment_run_types, control_run_types, experiment_input_df.get('accession')):
        if len(expt_rt) > 1 or (ctl_rt is not None and (expt_rt != ctl_rt or len(ctl_rt) > 1)):
            ERROR_not_matching_endedness.append(experiment_accession)
            print('ERROR: Experiment {} and/or its control have mismatched endedness.'.format(experiment_accession))
            paired_end_final.append(None)
        elif next(iter(expt_rt)) == 'paired-ended':
            paired_end_final.append(True)
        elif next(iter(expt_rt)) == 'single-ended':
            paired_end_final.append(False)
    output_df['chip.paired_end'] = paired_end_final

    output_df['chip.crop_length'] = [int(x) for x in crop_length]
    output_df['chip.ctl_nodup_bams'] = ctl_nodup_bams
    output_df['chip.pipeline_type'] = pipeline_type
    output_df['chip.always_use_pooled_ctl'] = [True if x != 'control' else None for x in output_df['chip.pipeline_type']]

    # Populate the lists of fastqs.
    for val in list(range(1, 11)):
        output_df['chip.fastqs_rep{}_R1'.format(val)] = fastqs_by_rep_R1_master[val]
        output_df['chip.fastqs_rep{}_R2'.format(val)] = fastqs_by_rep_R2_master[val]

    # Build descriptions using the other parameters.
    description_strings = []
    for accession, crop_length, is_paired_end, pipeline_type, align_only in zip(
            output_df['chip.title'],
            output_df['chip.crop_length'],
            output_df['chip.paired_end'],
            output_df['chip.pipeline_type'],
            output_df['chip.align_only']
    ):
        description_strings.append('{}_{}_{}_{}_{}'.format(
            accession,
            ('PE' if is_paired_end == True else 'SE'),
            str(crop_length) + '_crop',
            pipeline_type,
            ('alignonly' if align_only == True else 'peakcall')
            ))
    output_df['chip.description'] = description_strings

    # Clean up the pipeline_type data - submit all 'controls' as 'tf'
    output_df['chip.pipeline_type'].replace(to_replace='control', value='tf', inplace=True)

    # Variables same for all
    output_df['chip.ref_fa'] = 'https://www.encodeproject.org/files/GRCh38_no_alt_analysis_set_GCA_000001405.15/@@download/GRCh38_no_alt_analysis_set_GCA_000001405.15.fasta.gz'
    output_df['chip.bowtie2_idx_tar'] = 'https://www.encodeproject.org/files/ENCFF110MCL/@@download/ENCFF110MCL.tar.gz'
    output_df['chip.bwa_idx_tar'] = 'https://www.encodeproject.org/files/ENCFF643CGH/@@download/ENCFF643CGH.tar.gz'

    # Remove any experiments with errors from the table.
    output_df.drop(
        ERROR_control_error_detected + ERROR_no_fastqs + ERROR_not_matching_endedness,
        inplace=True)

    # Output rows of dataframes as input json files.
    output_dict = output_df.to_dict('index')
    command_output = ''
    for experiment in output_dict:
        # Build strings of caper commands.
        command_output = command_output + 'caper submit {} -i {}{} -s {}{} \n'.format(
            wdl_path,
            gc_path,
            output_dict[experiment]['chip.description'] + '.json',
            output_dict[experiment]['chip.description'],
            ('_' + output_dict[experiment]['custom_message'] if output_dict[experiment]['custom_message'] != '' else ''))

        # Remove empty properties and the custom message property.
        for prop in list(output_dict[experiment]):
            if output_dict[experiment][prop] in (None, [], [None]):
                output_dict[experiment].pop(prop)
        output_dict[experiment].pop('custom_message')

        file_name = '{}.json'.format(output_dict[experiment]['chip.description'])
        with open(file_name, 'w') as output_file:
            output_file.write(json.dumps(output_dict[experiment], indent=4))

    # Output .txt with caper commands.
    if command_output != '':
        with open('{}caper_submit_commands.txt'.format(output_path), 'w') as command_output_file:
            command_output_file.write(command_output)


if __name__ == '__main__':
    main()
