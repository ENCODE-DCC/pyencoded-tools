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
        '&field=files.platform' + \
        '&field=replicates.library.biosample.organism.scientific_name' + \
        '&field=replicates.library.spikeins_used' + \
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
        '&field=file_format' + \
        '&field=biological_replicates' + \
        '&field=replicate.library.accession' + \
        '&field=status' + \
        '&field=s3_uri' + \
        '&field=href' + \
        '&field=replicate.status' + \
        '&limit=all' + \
        '&format=json'


def parse_infile(infile):
    try:
        infile_df = pd.read_csv(infile, sep='\t')
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

    # Fetch data from the ENCODE portal
    experiment_input_df, file_input_df = get_data_from_portal(infile_df, server, keypair, link_prefix, link_src)

    # Create output_df to store all data for the final input.json files.
    output_df = pd.DataFrame()
    output_df['long_read_rna_pipeline.experiment_prefix'] = infile_df['accession']
    if 'custom_message' in infile_df:
        output_df['custom_message'] = infile_df['custom_message']
        output_df['custom_message'].fillna('', inplace=True)
    else:
        output_df['custom_message'] = ''
    output_df.set_index('long_read_rna_pipeline.experiment_prefix', inplace=True, drop=False)

    '''
    Experiment sorting section
    '''

    # Assign genome reference files.
    ref_genomes = []
    annotations = []
    variants = []
    annotation_names = []
    genome_builds = []
    spike_ins = []
    for replicates in experiment_input_df.get('replicates'):
        organism = set()
        for rep in replicates:
            organism.add(rep['library']['biosample']['organism']['scientific_name'])

        if ''.join(organism) == 'Homo sapiens':
            ref_genomes.append('https://www.encodeproject.org/files/GRCh38_no_alt_analysis_set_GCA_000001405.15/@@download/GRCh38_no_alt_analysis_set_GCA_000001405.15.fasta.gz')
            annotations.append('https://www.encodeproject.org/files/gencode.v29.primary_assembly.annotation_UCSC_names/@@download/gencode.v29.primary_assembly.annotation_UCSC_names.gtf.gz')
            variants.append('https://www.encodeproject.org/files/ENCFF911UGW/@@download/ENCFF911UGW.vcf.gz')
            annotation_names.append('V29')
            genome_builds.append('GRCh38')

        elif ''.join(organism) == 'Mus musculus':
            ref_genomes.append('https://www.encodeproject.org/files/mm10_no_alt_analysis_set_ENCODE/@@download/mm10_no_alt_analysis_set_ENCODE.fasta.gz')
            annotations.append('https://www.encodeproject.org/files/gencode.vM21.primary_assembly.annotation_UCSC_names/@@download/gencode.vM21.primary_assembly.annotation_UCSC_names.gtf.gz')
            variants.append(None)
            annotation_names.append('M21')
            genome_builds.append('mm10')

        spike_in_reference_accessions = []
        spike_in_fasta_links = []
        for rep in replicates:
            spike_in_reference_accessions.extend(rep['library']['spikeins_used'])
        if '/references/ENCSR089MWE/' in spike_in_reference_accessions:
            spike_in_fasta_links.append('https://www.encodeproject.org/files/ENCFF189VLW/@@download/ENCFF189VLW.fasta.gz')
        if '/references/ENCSR156CIL/' in spike_in_reference_accessions:
            spike_in_fasta_links.append('https://www.encodeproject.org/files/ENCFF001RTP/@@download/ENCFF001RTP.fasta.gz')
        spike_ins.append(spike_in_fasta_links)

    output_df['long_read_rna_pipeline.reference_genome'] = ref_genomes
    output_df['long_read_rna_pipeline.annotation'] = annotations
    output_df['long_read_rna_pipeline.genome_build'] = genome_builds
    output_df['long_read_rna_pipeline.annotation_name'] = annotation_names
    output_df['long_read_rna_pipeline.variants'] = variants
    output_df['long_read_rna_pipeline.spikeins'] = spike_ins

    # Specify input_type
    platforms = []
    for files in experiment_input_df.get('files'):
        platform_id = ''
        for file in files:
            if 'platform' in file:
                platform_id = file['platform']['@id']
                break
        if platform_id in ['/platforms/OBI:0002633/', '/platforms/OBI:0002632/', '/platforms/OBI:0002012/']:
            platforms.append('pacbio')
        elif platform_id in [
            '/platforms/NTR:0000451/',  # Nanopore SmidgION
            '/platforms/OBI:0002750/',  # Nanopore MinION
            '/platforms/OBI:0002751/',  # Nanopore GridIONx5
            '/platforms/OBI:0002752/',  # Nanopore PromethION
        ]:
            platforms.append('nanopore')
    output_df['long_read_rna_pipeline.input_type'] = platforms

    # Arrays which will be added to the master Dataframe for all experiments
    fastqs_by_rep_R1_master = []
    libs_by_rep_R1_master = []

    for experiment_files, experiment_id in zip(experiment_input_df['files'], experiment_input_df['@id']):
        # Arrays for files within each experiment
        fastqs_by_rep_R1 = {
            1: [], 2: [],
            3: [], 4: [],
            5: [], 6: [],
            7: [], 8: [],
            9: [], 10: []
        }
        libs_by_rep_R1 = {
            1: [], 2: [],
            3: [], 4: [],
            5: [], 6: [],
            7: [], 8: [],
            9: [], 10: []
        }

        for file in experiment_files:
            link = file[link_src]
            if link.endswith('fastq.gz') \
                    and link in file_input_df.index \
                    and file_input_df.loc[link].at['status'] in allowed_statuses \
                    and file_input_df.loc[link].at['replicate.status'] in allowed_statuses:

                for rep_num in fastqs_by_rep_R1:
                    if file_input_df.loc[link].at['biorep_scalar'] == rep_num:
                        fastqs_by_rep_R1[rep_num].append(link_prefix + link)
                        libs_by_rep_R1[rep_num].append(file_input_df.loc[link].at['replicate.library.accession'])

        # Record error if no fastqs for found for any replicate.
        if all(val == [] for val in fastqs_by_rep_R1.values()):
            print(f'ERROR: no fastqs were found for {experiment_id}.')
            ERROR_no_fastqs.append(experiment_id)

        merged_fastqs_list = []
        merged_libs_list = []
        # Fix ordering of reps to prevent non-consecutive numbering.
        for k in list(range(1, 11)):
            if fastqs_by_rep_R1[k] != []:
                merged_fastqs_list.append(fastqs_by_rep_R1[k])
                for item in libs_by_rep_R1[k]:
                    merged_libs_list.append(item)

        fastqs_by_rep_R1_master.append(merged_fastqs_list)
        libs_by_rep_R1_master.append(merged_libs_list)

    '''
    Assign all remaining missing properties in the master dataframe.
    '''

    # Populate the lists of fastqs and libraries.
    output_df['long_read_rna_pipeline.fastqs'] = fastqs_by_rep_R1_master
    output_df['long_read_rna_pipeline.talon_prefixes'] = libs_by_rep_R1_master

    # Remove any experiments with errors from the table.
    output_df.drop(ERROR_no_fastqs, inplace=True)

    # Output rows of dataframes as input json files.
    output_dict = output_df.to_dict('index')
    command_output = ''
    # Order for parameters in the input.jsons
    desired_key_order = [
        'custom_message',
        'long_read_rna_pipeline.experiment_prefix',
        'long_read_rna_pipeline.fastqs',
        'long_read_rna_pipeline.talon_prefixes',
        "long_read_rna_pipeline.genome_build",
        'long_read_rna_pipeline.reference_genome',
        "long_read_rna_pipeline.annotation_name",
        'long_read_rna_pipeline.annotation',
        'long_read_rna_pipeline.variants',
        "long_read_rna_pipeline.input_type",
        "long_read_rna_pipeline.spikeins"
    ]

    for experiment in output_dict:
        output_dict[experiment] = {key: output_dict[experiment][key] for key in desired_key_order}
        # Build strings of caper commands.
        command_output = command_output + 'caper submit {} -i {}{} -s {}{}\nsleep 1\n'.format(
            wdl_path,
            (gc_path + '/' if not gc_path.endswith('/') else gc_path),
            output_dict[experiment]['long_read_rna_pipeline.experiment_prefix'] + '.json',
            output_dict[experiment]['long_read_rna_pipeline.experiment_prefix'],
            ('_' + output_dict[experiment]['custom_message'] if output_dict[experiment]['custom_message'] != '' else ''))

        # Remove empty properties and the custom message property.
        for prop in list(output_dict[experiment]):
            if output_dict[experiment][prop] in (None, [], '') or (type(output_dict[experiment][prop]) == list and None in output_dict[experiment][prop]):
                output_dict[experiment].pop(prop)
        output_dict[experiment].pop('custom_message')

        file_name = f'{output_path}{"/" if output_path else ""}{output_dict[experiment]["long_read_rna_pipeline.experiment_prefix"]}.json'
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
