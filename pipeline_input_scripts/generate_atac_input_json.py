import argparse
import json
import os
import pandas as pd
import requests


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-i', "--infile", required=True, action='store',
                        help="""Path to .txt file containing accessions of experiments to process or list of accessions separated by commas. The txt file must contain two columns with 1 header row, one labeled 'accession' and another labeled 'align_only'. It can optionally include a 3rd column for 'custom_message'.""")
    parser.add_argument('-o', '--outputpath', action='store', default='',
                        help="""Optional path to output folder. Defaults to current path.""")
    parser.add_argument('-g', '--gcpath', action='store', default='',
                        help="""Optional path where the input.json will be uploaded to the Google Cloud instance. Only affects the list of caper commands that is generated.""")
    parser.add_argument('--wdl', action='store', default=False,
                        help="""Path to .wdl file.""")
    parser.add_argument('-s', '--server', action='store', default='https://www.encodeproject.org',
                        help="""Optional specification of server using the full URL. Defaults to production server.""")
    parser.add_argument('--use-s3-uris', action='store_true', default=False,
                        help="""Optional flag to use s3_uri links. Otherwise, defaults to using @@download links from the ENCODE portal.""")
    parser.add_argument('--align-only', action='store', default=False,
                        help="""Pipeline will end after alignments step if True.""")
    parser.add_argument('--custom-message', action='store',
                        help="""An additional custom string to be appended to the messages in the caper submit commands.""")
    parser.add_argument('--caper-commands-file-message', action='store', default='',
                        help="""An additional custom string to be appended to the file name of the caper submit commands.""")
    return parser


def check_path_trailing_slash(path):
    if path.endswith('/'):
        return path.strip('/')
    else:
        return path


def build_experiment_report_query(experiment_list, server):
    joined_list = '&accession='.join(experiment_list)
    return server + '/report/?type=Experiment' + \
        f'&accession={joined_list}' + \
        '&field=@id' + \
        '&field=accession' + \
        '&field=files.s3_uri' + \
        '&field=files.href' + \
        '&field=replicates.library.biosample.organism.scientific_name' + \
        '&limit=all' + \
        '&format=json'


def build_file_report_query(experiment_list, server):
    joined_list = '&dataset='.join(experiment_list)
    return server + '/report/?type=File' + \
        f'&dataset={joined_list}' + \
        '&status=released' + \
        '&status=in+progress' + \
        '&award.rfa=ENCODE4' + \
        '&award.rfa=ENCODE3' + \
        '&assembly!=hg19' + \
        '&assembly!=mm9' + \
        '&file_format=fastq' + \
        '&output_type=reads' + \
        '&field=@id' + \
        '&field=dataset' + \
        '&field=file_format' + \
        '&field=biological_replicates' + \
        '&field=replicate.library.adapters' + \
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
    except KeyError:
        print('Missing required align_only column in input file.')
        exit()


def strs2bool(strings):
    out = []
    for string in strings:
        if string == "True":
            out.append(True)
        elif string == "False":
            out.append(False)
    return out


# Simple function to count the number of replicates per input.json
def count_reps(row):
    x = 0
    for value in row:
        if None in value or value == []:
            continue
        else:
            x = x+1
    return x


def get_data_from_portal(infile_df, server, keypair, link_prefix, link_src):
    # Retrieve experiment report view json with necessary fields and store as DataFrame.
    experiment_input_df = pd.DataFrame()
    experiment_accessions = infile_df['accession'].tolist()
    # Chunk the list to avoid sending queries longer than the character limit
    chunked_experiment_accessions = [experiment_accessions[x:x+100] for x in range(0, len(experiment_accessions), 100)]
    for chunk in chunked_experiment_accessions:
        experiment_report = requests.get(
            build_experiment_report_query(chunk, server),
            auth=keypair,
            headers={'content-type': 'application/json'})
        experiment_report_json = json.loads(experiment_report.text)
        experiment_df_temp = pd.json_normalize(experiment_report_json['@graph'])
        experiment_input_df = experiment_input_df.append(experiment_df_temp, ignore_index=True, sort=True)
    experiment_input_df.sort_values(by=['accession'], inplace=True)

    # Gather list of controls from the list of experiments to query for their files.
    datasets_to_retrieve = experiment_input_df.get('@id').tolist()

    # Retrieve file report view json with necessary fields and store as DataFrame.
    file_input_df = pd.DataFrame()
    chunked_dataset_accessions = [datasets_to_retrieve[x:x+100] for x in range(0, len(datasets_to_retrieve), 100)]
    for chunk in chunked_dataset_accessions:
        file_report = requests.get(
            build_file_report_query(chunk, server),
            auth=keypair,
            headers={'content-type': 'application/json'})
        file_report_json = json.loads(file_report.text)
        file_df_temp = pd.json_normalize(file_report_json['@graph'])
        file_input_df = file_input_df.append(file_df_temp, ignore_index=True, sort=True)
    file_input_df.set_index(link_src, inplace=True)
    if 'paired_end' not in file_input_df:
        file_input_df['paired_end'] = None
        file_input_df['paired_with'] = None
    file_input_df['biorep_scalar'] = [x[0] for x in file_input_df['biological_replicates']]

    return experiment_input_df, file_input_df


def main():
    keypair = (os.environ.get('DCC_API_KEY'), os.environ.get('DCC_SECRET_KEY'))
    parser = get_parser()
    args = parser.parse_args()
    allowed_statuses = ['released', 'in progress']

    output_path = check_path_trailing_slash(args.outputpath)
    wdl_path = args.wdl
    gc_path = args.gcpath
    caper_commands_file_message = args.caper_commands_file_message

    server = check_path_trailing_slash(args.server)
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
            'accession': accession_list,
            'align_only': align_only,
            'custom_message': message
        })
        infile_df.sort_values(by=['accession'], inplace=True)

    # Arrays to store lists of potential errors.
    ERROR_no_fastqs = []
    ERROR_not_matching_endedness = []

    # Fetch data from the ENCODE portal
    experiment_input_df, file_input_df = get_data_from_portal(infile_df, server, keypair, link_prefix, link_src)

    # Create output_df to store all data for the final input.json files.
    output_df = pd.DataFrame()
    output_df['atac.title'] = infile_df['accession']
    output_df['atac.align_only'] = infile_df['align_only']
    if 'custom_message' in infile_df:
        output_df['custom_message'] = infile_df['custom_message']
        output_df['custom_message'].fillna('', inplace=True)
    else:
        output_df['custom_message'] = ''
    output_df.set_index('atac.title', inplace=True, drop=False)

    '''
    Experiment sorting section
    '''

    # Assign blacklist(s) and genome reference files.
    blacklist = []
    genome_tsv = []
    chrom_sizes = []
    gensz = []
    ref_fa = []
    mito_ref_fa = []
    tss = []
    prom = []
    enh = []
    bowtie2 = []
    bowtie2_mito = []

    for replicates in experiment_input_df.get('replicates'):
        organism = set()
        for rep in replicates:
            organism.add(rep['library']['biosample']['organism']['scientific_name'])

        if ''.join(organism) == 'Homo sapiens':
            genome_tsv.append('https://storage.googleapis.com/encode-pipeline-genome-data/genome_tsv/v3/hg38.tsv')
            chrom_sizes.append('https://www.encodeproject.org/files/GRCh38_EBV.chrom.sizes/@@download/GRCh38_EBV.chrom.sizes.tsv')
            gensz.append('hs')

            ref_fa.append('https://www.encodeproject.org/files/GRCh38_no_alt_analysis_set_GCA_000001405.15/@@download/GRCh38_no_alt_analysis_set_GCA_000001405.15.fasta.gz')
            mito_ref_fa.append('https://www.encodeproject.org/files/GRCh38_no_alt_analysis_set_GCA_000001405.15_mito_only/@@download/GRCh38_no_alt_analysis_set_GCA_000001405.15_mito_only.fasta.gz')

            blacklist.append('https://www.encodeproject.org/files/ENCFF356LFX/@@download/ENCFF356LFX.bed.gz')
            tss.append('https://www.encodeproject.org/files/ENCFF493CCB/@@download/ENCFF493CCB.bed.gz')
            prom.append('https://www.encodeproject.org/files/ENCFF140XLU/@@download/ENCFF140XLU.bed.gz')
            enh.append('https://www.encodeproject.org/files/ENCFF212UAV/@@download/ENCFF212UAV.bed.gz')

            bowtie2.append('https://www.encodeproject.org/files/ENCFF110MCL/@@download/ENCFF110MCL.tar.gz')
            bowtie2_mito.append('https://www.encodeproject.org/files/GRCh38_no_alt_analysis_set_GCA_000001405.15_mito_only_bowtie2_index/@@download/GRCh38_no_alt_analysis_set_GCA_000001405.15_mito_only_bowtie2_index.tar.gz')

        elif ''.join(organism) == 'Mus musculus':
            genome_tsv.append('https://storage.googleapis.com/encode-pipeline-genome-data/genome_tsv/v3/mm10.tsv')
            chrom_sizes.append('https://www.encodeproject.org/files/mm10_no_alt.chrom.sizes/@@download/mm10_no_alt.chrom.sizes.tsv')
            gensz.append('mm')

            ref_fa.append('https://www.encodeproject.org/files/mm10_no_alt_analysis_set_ENCODE/@@download/mm10_no_alt_analysis_set_ENCODE.fasta.gz')
            mito_ref_fa.append('https://www.encodeproject.org/files/mm10_no_alt_analysis_set_ENCODE_mito_only/@@download/mm10_no_alt_analysis_set_ENCODE_mito_only.fasta.gz')

            blacklist.append('https://www.encodeproject.org/files/ENCFF547MET/@@download/ENCFF547MET.bed.gz')
            tss.append('https://www.encodeproject.org/files/ENCFF498BEJ/@@download/ENCFF498BEJ.bed.gz')
            prom.append('https://www.encodeproject.org/files/ENCFF206BQS/@@download/ENCFF206BQS.bed.gz')
            enh.append('https://www.encodeproject.org/files/ENCFF580RGZ/@@download/ENCFF580RGZ.bed.gz')

            bowtie2.append('https://www.encodeproject.org/files/ENCFF309GLL/@@download/ENCFF309GLL.tar.gz')
            bowtie2_mito.append('https://www.encodeproject.org/files/mm10_no_alt_analysis_set_ENCODE_mito_only_bowtie2_index/@@download/mm10_no_alt_analysis_set_ENCODE_mito_only_bowtie2_index.tar.gz')

    output_df['atac.blacklist'] = blacklist
    output_df['atac.genome_tsv'] = genome_tsv
    output_df['atac.chrsz'] = chrom_sizes
    output_df['atac.gensz'] = gensz
    output_df['atac.ref_fa'] = ref_fa
    output_df['atac.ref_mito_fa'] = mito_ref_fa
    output_df['atac.tss'] = tss
    output_df['atac.prom'] = prom
    output_df['atac.enh'] = enh
    output_df['atac.bowtie2_idx_tar'] = bowtie2
    output_df['atac.bowtie2_mito_idx_tar'] = bowtie2_mito

    # Arrays which will be added to the master Dataframe for all experiments
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
    adapters_by_rep_R1_master = {
        1: [], 2: [],
        3: [], 4: [],
        5: [], 6: [],
        7: [], 8: [],
        9: [], 10: []
    }
    adapters_by_rep_R2_master = {
        1: [], 2: [],
        3: [], 4: [],
        5: [], 6: [],
        7: [], 8: [],
        9: [], 10: []
    }
    # Store experiment run types
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
        adapters_by_rep_R1 = {
            1: [], 2: [],
            3: [], 4: [],
            5: [], 6: [],
            7: [], 8: [],
            9: [], 10: []
        }
        adapters_by_rep_R2 = {
            1: [], 2: [],
            3: [], 4: [],
            5: [], 6: [],
            7: [], 8: [],
            9: [], 10: []
        }
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

                            for adapter in file_input_df.loc[link].at['replicate.library.adapters']:
                                if adapter['type'] == "read1 3' adapter":
                                    adapter_sequence = adapter['sequence']
                                else:
                                    continue
                            adapters_by_rep_R1[rep_num].append(adapter_sequence)

                            for adapter in file_input_df.loc[file_input_df[file_input_df['@id'] == pair].index.values[0]].at['replicate.library.adapters']:
                                if adapter['type'] == "read2 3' adapter":
                                    adapter_sequence = adapter['sequence']
                                else:
                                    continue
                            adapters_by_rep_R2[rep_num].append(adapter_sequence)

                elif pd.isnull(file_input_df.loc[link].at['paired_end']):
                    for rep_num in fastqs_by_rep_R1:
                        if file_input_df.loc[link].at['biorep_scalar'] == rep_num:
                            fastqs_by_rep_R1[rep_num].append(link_prefix + link)
                            fastqs_by_rep_R2[rep_num].append(None)

                            for adapter in file_input_df.loc[link].at['replicate.library.adapters']:
                                if adapter['type'] == "read1 3' adapter":
                                    adapter_sequence = adapter['sequence']
                                else:
                                    continue
                            adapters_by_rep_R1[rep_num].append(adapter_sequence)
                            adapters_by_rep_R2[rep_num].append(None)

                run_types.add(file_input_df.loc[link].at['run_type'])

        # Record error if no fastqs for found for any replicate.
        if all(val == [] for val in fastqs_by_rep_R1.values()):
            print(f'ERROR: no fastqs were found for {experiment_id}.')
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
                        adapters_by_rep_R1[k] = adapters_by_rep_R1[i]
                        adapters_by_rep_R2[k] = adapters_by_rep_R2[i]
                        adapters_by_rep_R1[i] = []
                        adapters_by_rep_R2[i] = []
                        break
                    else:
                        continue

        # Add the replicates to the master list.
        for rep_num in fastqs_by_rep_R1_master:
            fastqs_by_rep_R1_master[rep_num].append(fastqs_by_rep_R1[rep_num])
            fastqs_by_rep_R2_master[rep_num].append(fastqs_by_rep_R2[rep_num])
            adapters_by_rep_R1_master[rep_num].append(adapters_by_rep_R1[rep_num])
            adapters_by_rep_R2_master[rep_num].append(adapters_by_rep_R2[rep_num])

        if 'single-ended' in run_types:
            experiment_run_types.append('single-ended')
        elif next(iter(run_types)) == 'paired-ended':
            experiment_run_types.append('paired-ended')

    '''
    Assign all remaining missing properties in the master dataframe.
    '''

    # Check that all fastqs in experiment and its control have the same endedness.
    paired_end_final = []
    for expt_rt, experiment_accession in zip(experiment_run_types, experiment_input_df.get('accession')):
        if expt_rt == 'paired-ended':
            paired_end_final.append(True)
        elif expt_rt == 'single-ended':
            paired_end_final.append(False)
        else:
            print(f'ERROR: Could not determine endedness for {experiment_accession}.')
            paired_end_final.append(None)
    output_df['atac.paired_end'] = paired_end_final

    # Populate the lists of fastqs and adapters.
    for val in list(range(1, 11)):
        output_df[f'atac.fastqs_rep{val}_R1'] = fastqs_by_rep_R1_master[val]
        output_df[f'atac.fastqs_rep{val}_R2'] = fastqs_by_rep_R2_master[val]
        output_df[f'atac.adapters_rep{val}_R1'] = adapters_by_rep_R1_master[val]
        output_df[f'atac.adapters_rep{val}_R2'] = adapters_by_rep_R2_master[val]
    R1_cols = [col for col in output_df.columns if col.endswith('_R1') and 'fastqs' in col]
    output_df['number_of_replicates'] = output_df[R1_cols].apply(lambda x: count_reps(x), axis=1)

    # Build descriptions using the other parameters.
    description_strings = []
    for accession, is_paired_end, align_only, num_reps in zip(
            output_df['atac.title'],
            output_df['atac.paired_end'],
            output_df['atac.align_only'],
            output_df['number_of_replicates']
    ):
        description_strings.append('{}_{}_{}rep_{}'.format(
            accession,
            ('PE' if is_paired_end else 'SE'),
            num_reps,
            ('alignonly' if align_only else 'peakcall')
            ))
    output_df['atac.description'] = description_strings

    # Same values for all.
    output_df['atac.multimapping'] = 4
    output_df['atac.pipeline_type'] = 'atac'

    # Remove any experiments with errors from the table.
    output_df.drop(
        ERROR_no_fastqs +
        ERROR_not_matching_endedness,
        inplace=True)

    # Output rows of dataframes as input json files.
    output_dict = output_df.to_dict('index')
    command_output = ''
    # Order for parameters in the input.jsons
    desired_key_order = [
        'custom_message',
        'atac.title',
        'atac.description',
        'atac.pipeline_type',
        'atac.align_only',
        'atac.paired_end',
        'atac.multimapping',
        'atac.genome_tsv',
        'atac.ref_fa',
        'atac.ref_mito_fa',
        'atac.bowtie2_idx_tar',
        'atac.bowtie2_mito_idx_tar',
        'atac.chrsz',
        'atac.blacklist',
        'atac.tss',
        'atac.prom',
        'atac.enh',
        'atac.gensz',
    ]
    for val in list(range(1, 11)):
        desired_key_order.extend([f'atac.fastqs_rep{val}_R1', f'atac.fastqs_rep{val}_R2'])
    for val in list(range(1, 11)):
        desired_key_order.extend([f'atac.adapters_rep{val}_R1', f'atac.adapters_rep{val}_R2'])

    for experiment in output_dict:
        output_dict[experiment] = {key: output_dict[experiment][key] for key in desired_key_order}
        # Build strings of caper commands.
        command_output = command_output + 'caper submit {} -i {}{} -s {}{}\nsleep 1\n'.format(
            wdl_path,
            (gc_path + '/' if not gc_path.endswith('/') else gc_path),
            output_dict[experiment]['atac.description'] + '.json',
            output_dict[experiment]['atac.description'],
            ('_' + output_dict[experiment]['custom_message'] if output_dict[experiment]['custom_message'] != '' else ''))

        # Remove empty properties and the custom message property.
        for prop in list(output_dict[experiment]):
            if output_dict[experiment][prop] in (None, [], '') or (type(output_dict[experiment][prop]) == list and None in output_dict[experiment][prop]):
                output_dict[experiment].pop(prop)
        output_dict[experiment].pop('custom_message')

        file_name = f'{output_path}{"/" if output_path else ""}{output_dict[experiment]["atac.description"]}.json'
        with open(file_name, 'w') as output_file:
            output_file.write(json.dumps(output_dict[experiment], indent=4))

    # Output .txt with caper commands.
    if command_output != '':
        commands_file_path = (
            f'{output_path}{"/" if output_path else ""}'
            f'caper_submit{"_" if caper_commands_file_message else ""}{caper_commands_file_message}.sh'
        )
        with open(commands_file_path, 'w') as command_output_file:
            command_output_file.write(command_output)


if __name__ == '__main__':
    main()
