import argparse
import json
import os
import pandas as pd
import requests


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('-i', "--infile", action='store',
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
    input_group.add_argument("--accessions", action='store',
                        help="""List of accessions separated by commas.""")
    parser.add_argument('--custom-message', action='store',
                        help="""An additional custom string to be appended to the messages in the caper submit commands.""")
    parser.add_argument('--caper-commands-file-message', action='store', default='',
                        help="""An additional custom string to be appended to the file name of the caper submit commands.""")
    return parser


def check_path_trailing_slash(path):
    if path.endswith('/'):
        return path.rstrip('/')
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
        '&file_format=fastq' + \
        '&output_type=reads' + \
        '&field=@id' + \
        '&field=dataset' + \
        '&field=biological_replicates' + \
        '&field=replicate.library.adapters' + \
        '&field=paired_end' + \
        '&field=paired_with' + \
        '&field=run_type' + \
        '&field=read_length' + \
        '&field=status' + \
        '&field=s3_uri' + \
        '&field=href' + \
        '&field=replicate.status' + \
        '&limit=all' + \
        '&format=json'


def parse_infile(infile):
    try:
        infile_df = pd.read_csv(infile, '\t')
        return infile_df
    except FileNotFoundError as e:
        print(e)
        exit()


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

    if args.infile:
        infile_df = parse_infile(args.infile)
        infile_df.sort_values(by=['accession'], inplace=True)
    elif args.accessions:
        accession_list = args.accessions.split(',')
        message = args.custom_message.split(',')
        infile_df = pd.DataFrame({
            'accession': accession_list,
            'custom_message': message
        })
        infile_df.sort_values(by=['accession'], inplace=True)

    # Arrays to store lists of potential errors.
    ERROR_no_fastqs = []
    ERROR_missing_adapters = []

    # Fetch data from the ENCODE portal
    experiment_input_df, file_input_df = get_data_from_portal(infile_df, server, keypair, link_prefix, link_src)

    # Create output_df to store all data for the final input.json files.
    output_df = pd.DataFrame()
    output_df['accession'] = infile_df['accession']
    if 'custom_message' in infile_df:
        output_df['custom_message'] = infile_df['custom_message']
        output_df['custom_message'].fillna('', inplace=True)
    else:
        output_df['custom_message'] = ''
    output_df.set_index('accession', inplace=True, drop=False)

    '''
    Retrieve info for dnase.replicates
    '''
    replicate_collector_master = []
    for experiment_files, experiment_id in zip(experiment_input_df['files'], experiment_input_df['accession']):
        replicate_collector = dict()

        for file in experiment_files:
            link = file[link_src]
            if link.endswith('fastq.gz') \
                    and link in file_input_df.index \
                    and file_input_df.loc[link].at['status'] in allowed_statuses \
                    and file_input_df.loc[link].at['replicate.status'] in allowed_statuses:
                
                rep_num = file_input_df.loc[link].at['biorep_scalar']
                if rep_num not in replicate_collector:
                        replicate_collector[rep_num] = {}

                if file_input_df.loc[link].at['paired_end'] == '1':
                    pair = file_input_df.loc[link].at['paired_with']
                    fastq_pair = {
                        'R1': link_prefix + link,
                        'R2': link_prefix + file_input_df[file_input_df['@id'] == pair].index.values[0]
                    }
                    if 'pe_fastqs' in replicate_collector[rep_num]:
                        replicate_collector[rep_num]['pe_fastqs'].append(fastq_pair)
                    else:
                        replicate_collector[rep_num]['pe_fastqs'] = [fastq_pair]

                    adapter_R1_sequence = None
                    adapter_R2_sequence = None
                    if type(file_input_df.loc[link].at['replicate.library.adapters']) == list:
                        for adapter in file_input_df.loc[link].at['replicate.library.adapters']:
                            if adapter['type'] == "read1 3' adapter":
                                adapter_R1_sequence = adapter['sequence']
                                break
                            else:
                                continue
                        for adapter in file_input_df.loc[file_input_df[file_input_df['@id'] == pair].index.values[0]].at['replicate.library.adapters']:
                            if adapter['type'] == "read2 3' adapter":
                                adapter_R2_sequence = adapter['sequence']
                                break
                            else:
                                continue

                    replicate_collector[rep_num]['adapters'] = {
                        'sequence_R1': adapter_R1_sequence,
                        'sequence_R2': adapter_R2_sequence
                    }
                elif pd.isnull(file_input_df.loc[link].at['paired_end']):
                    if 'se_fastqs' in replicate_collector[rep_num]:
                        replicate_collector[rep_num]['se_fastqs'].append(link_prefix + link)
                    else:
                        replicate_collector[rep_num]['se_fastqs'] = [link_prefix + link]

                if 'read_length' in replicate_collector[rep_num]:
                    replicate_collector[rep_num]['read_length'].append(file_input_df.loc[link].at['read_length'])
                else:
                    replicate_collector[rep_num]['read_length'] = [file_input_df.loc[link].at['read_length']]

        # Record error if no fastqs for found for any replicate.
        for rep_num in replicate_collector:
            if 'pe_fastqs' not in replicate_collector[rep_num] and 'se_fastqs' not in replicate_collector[rep_num]:
                print(f'ERROR: no fastqs were found for {experiment_id}.')
                ERROR_no_fastqs.append(experiment_id)
                continue
            if 'pe_fastqs' in replicate_collector[rep_num] and None in replicate_collector[rep_num]['adapters'].values():
                print(f'ERROR: no adapters were found for {experiment_id}.')
                ERROR_missing_adapters.append(experiment_id)

        replicate_key_order = [
            'accession',
            'number',
            'read_length',
            'adapters',
            'pe_fastqs',
            'se_fastqs'
        ]
        for rep in replicate_collector:
            replicate_collector[rep]['accession'] = experiment_id
            min_read_length = min(replicate_collector[rep]['read_length'])
            replicate_collector[rep]['read_length'] = int(min_read_length)
            replicate_collector[rep]['number'] = int(rep)
            replicate_collector[rep] = {key: replicate_collector[rep][key] for key in replicate_key_order if key in replicate_collector[rep]}
        sorted_rep_num = sorted(list(replicate_collector.keys()))
        replicate_collector_master.append([replicate_collector[rep] for rep in sorted_rep_num])

    '''
    Retrieve info for dnase.references
    '''
    references_master = []
    for replicates, read_len in zip(
            experiment_input_df.get('replicates'),
            [min(set(rep['read_length'] for rep in experiment)) for experiment in replicate_collector_master]):
        organism = set()
        for rep in replicates:
            organism.add(rep['library']['biosample']['organism']['scientific_name'])

        if ''.join(organism) == 'Homo sapiens':
            if read_len == 36:
                hotspot1_link = 'https://www.encodeproject.org/files/ENCFF405EUN/@@download/ENCFF405EUN.tar.gz'
                hotspot2_link = 'https://www.encodeproject.org/files/ENCFF180EJG/@@download/ENCFF180EJG.tar.gz'
            elif read_len == 76:
                hotspot1_link = 'https://www.encodeproject.org/files/ENCFF304SVB/@@download/ENCFF304SVB.tar.gz'
                hotspot2_link = 'https://www.encodeproject.org/files/ENCFF162AKB/@@download/ENCFF162AKB.tar.gz'
            refs = {
                'genome_name': 'GRCh38',
                'indexed_fasta_tar_gz': 'https://www.encodeproject.org/files/ENCFF499QKB/@@download/ENCFF499QKB.tar.gz',
                'bwa_index_tar_gz': 'https://www.encodeproject.org/files/ENCFF884HOA/@@download/ENCFF884HOA.tar.gz',
                'nuclear_chroms_gz': 'https://www.encodeproject.org/files/ENCFF762MJQ/@@download/ENCFF762MJQ.txt.gz',
                'narrow_peak_auto_sql': 'https://www.encodeproject.org/documents/ee0c4be3-1f65-44e1-9539-5a864751a289/@@download/attachment/narrowPeak.as',
                'hotspot1_tar_gz': hotspot1_link,
                'hotspot2_tar_gz': hotspot2_link,
                'bias_model_gz': 'https://www.encodeproject.org/files/ENCFF119KWQ/@@download/ENCFF119KWQ.txt.gz'
            }    
        elif ''.join(organism) == 'Mus musculus':
            if read_len == 36:
                hotspot1_link = 'https://www.encodeproject.org/files/ENCFF785GGO/@@download/ENCFF785GGO.tar.gz'
                hotspot2_link = 'https://www.encodeproject.org/files/ENCFF749IUT/@@download/ENCFF749IUT.tar.gz'
            elif read_len == 76:
                hotspot1_link = 'https://www.encodeproject.org/files/ENCFF950RBV/@@download/ENCFF950RBV.tar.gz'
                hotspot2_link = 'https://www.encodeproject.org/files/ENCFF566YRR/@@download/ENCFF566YRR.tar.gz'
            refs = {
                'genome_name': 'mm10',
                'indexed_fasta_tar_gz': 'https://www.encodeproject.org/files/ENCFF734RZS/@@download/ENCFF734RZS.tar.gz',
                'bwa_index_tar_gz': 'https://www.encodeproject.org/files/ENCFF063EBQ/@@download/ENCFF063EBQ.tar.gz',
                'nuclear_chroms_gz': 'https://www.encodeproject.org/files/ENCFF882SRG/@@download/ENCFF882SRG.txt.gz',
                'narrow_peak_auto_sql': 'https://www.encodeproject.org/documents/ee0c4be3-1f65-44e1-9539-5a864751a289/@@download/attachment/narrowPeak.as',
                'hotspot1_tar_gz': hotspot1_link,
                'hotspot2_tar_gz': hotspot2_link,
                'bias_model_gz': 'https://www.encodeproject.org/files/ENCFF119KWQ/@@download/ENCFF119KWQ.txt.gz'
            }
        references_master.append(refs)

    # Add data to output dataframe.
    output_df['dnase.replicates'] = replicate_collector_master
    output_df['dnase.references'] = references_master

    # Remove any experiments with errors from the table.
    output_df.drop(ERROR_no_fastqs + ERROR_missing_adapters, 
        inplace=True)

    # Output rows of dataframes as input json files.
    output_dict = output_df.to_dict('index')
    command_output = ''
    # Order for parameters in the input.jsons
    desired_key_order = [
        'custom_message',
        'accession',
        'dnase.replicates',
        'dnase.references',
    ]
    for experiment in output_dict:
        output_dict[experiment] = {key: output_dict[experiment][key] for key in desired_key_order}
        accession = output_dict[experiment]['accession']

        # Build strings of caper commands.
        command_output = command_output + 'caper submit {} -i {}{} -s {}{}\nsleep 1\n'.format(
            wdl_path,
            (gc_path + '/' if not gc_path.endswith('/') else gc_path),
            output_dict[experiment]['accession'] + '.json',
            output_dict[experiment]['accession'],
            ('_' + str(output_dict[experiment]['custom_message']) if output_dict[experiment]['custom_message'] != '' else ''))

        # Remove empty properties and the custom message property.
        for prop in list(output_dict[experiment]):
            if output_dict[experiment][prop] in (None, [], '') or (type(output_dict[experiment][prop]) == list and None in output_dict[experiment][prop]):
                output_dict[experiment].pop(prop)
        output_dict[experiment].pop('custom_message')
        output_dict[experiment].pop('accession')

        file_name = f'{output_path}{"/" if output_path else ""}{accession}.json'

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