'''
Script to generate input jsons for miRNA-seq pipeline
GitHub info about inputs: https://github.com/ENCODE-DCC/mirna-seq-pipeline/blob/master/docs/reference.md#inputs

This script takes experiment accessions as inputs, pulls required metadata for
those experiments, and then generates an input.json file for each one as
output. It will also output a caper_submit_commands.txt file that contains the
Caper commands to process each experiment on Google Cloud.

Example command:
python generate_mirna_input_json.py -i ENCSR000ABC -o /Users/mypath -g gs://my/path -s https://test.encodedcc.org -r 2 3 -e
'''
import argparse
import json
import os
import pandas as pd
import requests


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('-i', "--infile", action='store',
                        help="""Path to .txt file containing accessions of experiments to process.""")
    input_group.add_argument("--accessions", action='store',
                        help="""List of accessions separated by commas.""")
    parser.add_argument('-o', '--outputpath', action='store', default='',
                        help="""Optional path to output folder. Defaults to current path.""")
    parser.add_argument('-g', '--gcpath', action='store', default='',
                        help="""Optional path where the input.json will be uploaded to the Google Cloud instance. Only affects the list of caper commands that is generated.""")
    parser.add_argument('-s', '--server', action='store', default='https://www.encodeproject.org/',
                        help="""Optional specification of server using the full URL. Defaults to production server.""")
    parser.add_argument('--s3_uri', action='store_true', default=False,
                        help="""Optional flag to use s3_uri links from the ENCODE portal. Otherwise, defaults to using http links.""")
    parser.add_argument('--custom-message', action='store',
                        help="""An additional custom string to be appended to the messages in the caper submit commands.""")
    parser.add_argument('--caper-commands-file-message', action='store', default='',
                        help="""An additional custom string to be appended to the file name of the caper submit commands.""")
    parser.add_argument('--wdl', action='store', default=False,
                        help="""Path to .wdl file.""")
    return parser


def parse_infile(infile):
    try:
        infile_df = pd.read_csv(infile, '\t')
        return infile_df
    except FileNotFoundError as e:
        print(e)
        exit()


def build_experiment_report_query(experiment_list, server):
    joined_list = '&accession='.join(experiment_list)
    return server + '/report/?type=Experiment' + \
        '&accession={}'.format(joined_list) + \
        '&field=@id' + \
        '&field=accession' + \
        '&field=assay_title' + \
        '&field=files.@id' + \
        '&field=replicates.status' + \
        '&field=replicates.library.biosample.organism.scientific_name' + \
        '&field=documents' + \
        '&field=files.s3_uri' + \
        '&field=files.href' + \
        '&limit=all' + \
        '&format=json'


def build_file_report_query(experiment_list, server):
    joined_list = '&dataset='.join(experiment_list)
    return server + '/report/?type=File' + \
        '&dataset={}'.format(joined_list) + \
        '&file_format=fastq' + \
        '&field=@id' + \
        '&field=dataset' + \
        '&field=biological_replicates' + \
        '&field=replicate.library.adapters' + \
        '&field=status' + \
        '&field=s3_uri' + \
        '&field=href' + \
        '&field=replicate.status' + \
        '&limit=all' + \
        '&format=json'


def check_path_trailing_slash(path):
    if path.endswith('/'):
        return path.rstrip('/')
    else:
        return path


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
        experiment_input_df = pd.concat([experiment_input_df, experiment_df_temp], ignore_index=True, sort=True)
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
        file_input_df = pd.concat([file_input_df, file_df_temp], ignore_index=True, sort=True)
    file_input_df.set_index(link_src, inplace=True)
    file_input_df['biorep_scalar'] = [x[0] for x in file_input_df['biological_replicates']]

    return experiment_input_df, file_input_df


def main():
    keypair = (os.environ.get('DCC_API_KEY'), os.environ.get('DCC_SECRET_KEY'))
    parser = get_parser()
    args = parser.parse_args()

    # Check encodeurl flag and define the link source to use.
    server = check_path_trailing_slash(args.server)
    use_s3 = args.s3_uri
    if use_s3:
        link_prefix = ''
        link_src = 's3_uri'
    else:
        link_prefix = server
        link_src = 'href'

    # Set the output paths and load the list of experiments to process.
    gc_path = check_path_trailing_slash(args.gcpath)
    output_path = check_path_trailing_slash(args.outputpath)
    caper_commands_file_message = args.caper_commands_file_message
    wdl = args.wdl
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

    # Fetch data from the ENCODE portal
    experiment_input_df, file_input_df = get_data_from_portal(infile_df, server, keypair, link_prefix, link_src)

    # Create output DataFrame to store all data for the input.json files.
    output_df = pd.DataFrame()
    # Assign experiment_prefix values. For simplicity, this is the same as the experiment accession.
    output_df['mirna_seq_pipeline.experiment_prefix'] = experiment_input_df['accession']
    if 'custom_message' in infile_df:
        output_df['custom_message'] = infile_df['custom_message']
        output_df['custom_message'].fillna('', inplace=True)
    else:
        output_df['custom_message'] = ''
    output_df.set_index('mirna_seq_pipeline.experiment_prefix', inplace=True, drop=False)

    # To correctly get the information for fastqs and adapters, this script looks up fastq files associated with each specified experiment in a 2nd table containing only file metadata. 
    final_input_fastq_links = []
    final_input_five_prime_adapters = []
    allowed_statuses = ['released', 'in progress']
    # List to store Experiment accessions with more than 1 fastq found per replicate.
    extra_adapters_detected = []

    for experiment_files in experiment_input_df.get('files'):
        fastqs_by_rep = {
            1: [], 2: [],
            3: [], 4: [],
            5: [], 6: [],
            7: [], 8: [],
            9: [], 10: []
        }
        adapters_by_rep = {
            1: [], 2: [],
            3: [], 4: [],
            5: [], 6: [],
            7: [], 8: [],
            9: [], 10: []
        }

        # Iterate over each file in the current experiment and collect data on fastq files
        for file in experiment_files:
            link = file[link_src]
            # Check fastq files for correct status and replicate #
            if link.endswith('fastq.gz') \
                    and link in file_input_df.index \
                    and file_input_df.loc[link].at['status'] in allowed_statuses \
                    and file_input_df.loc[link].at['replicate.status'] in allowed_statuses:
                for rep_num in fastqs_by_rep:
                    if file_input_df.loc[link].at['biorep_scalar'] == rep_num:
                        fastqs_by_rep[rep_num].append(link_prefix + link)

                        adapter_accession = None
                        for adapter in file_input_df.loc[link].at['replicate.library.adapters']:
                            if adapter['type'] == "read1 5' adapter" and adapter_accession is None:
                                adapter_accession = '{}{}@@download/{}.txt.gz'.format(
                                    server,
                                    adapter['file'],
                                    adapter['file'].split('/')[2])
                            elif adapter['type'] == "read1 3' adapter":
                                continue
                            else:
                                extra_adapters_detected.append(file_input_df.loc[link].at['dataset'][13:24])
                        adapters_by_rep[rep_num].append(adapter_accession)

        formatted_fastq_links = []
        formatted_adapter_links = []
        for key in fastqs_by_rep:
            if len(fastqs_by_rep[key]) > 0:
                formatted_fastq_links.append(fastqs_by_rep[key])
                formatted_adapter_links.append(adapters_by_rep[key])

        # Append the list of lists to (yet another) list, which is the experiment level one.
        final_input_fastq_links.append(formatted_fastq_links)
        final_input_five_prime_adapters.append(formatted_adapter_links)

    output_df['mirna_seq_pipeline.fastqs'] = final_input_fastq_links
    output_df['mirna_seq_pipeline.five_prime_adapters'] = final_input_five_prime_adapters
    # Same 3' adapters were used for all experiments.
    output_df['mirna_seq_pipeline.three_prime_adapters'] = server + '/files/ENCFF937TEF/@@download/ENCFF937TEF.txt.gz'

    # Specify star_index, mirna_annotation, and chrom_size.
    star_indices = []
    mirna_annotations = []
    chrom_sizes = []

    for replicates in experiment_input_df.get('replicates'):
        organism = set()
        for rep in replicates:
            # Store species name.
            organism.add(rep['library']['biosample']['organism']['scientific_name'])

        if ''.join(organism) == 'Homo sapiens':
            star_indices.append(server + '/files/ENCFF123QLG/@@download/ENCFF123QLG.tar.gz')
            mirna_annotations.append(server + '/files/ENCFF470CZH/@@download/ENCFF470CZH.gtf.gz')
            chrom_sizes.append(server + '/files/GRCh38_EBV.chrom.sizes/@@download/GRCh38_EBV.chrom.sizes.tsv')
        elif ''.join(organism) == 'Mus musculus':
            star_indices.append(server + '/files/ENCFF795AAA/@@download/ENCFF795AAA.tar.gz')
            mirna_annotations.append(server + '/files/ENCFF094ICJ/@@download/ENCFF094ICJ.gtf.gz')
            chrom_sizes.append(server + '/files/mm10_no_alt.chrom.sizes/@@download/mm10_no_alt.chrom.sizes.tsv')
        else:
            star_indices.append('')
            mirna_annotations.append('')
            chrom_sizes.append('')

    output_df['mirna_seq_pipeline.star_index'] = star_indices
    output_df['mirna_seq_pipeline.mirna_annotation'] = mirna_annotations
    output_df['mirna_seq_pipeline.chrom_sizes'] = chrom_sizes

    # Assign other parameters, which are identical for all runs.
    output_df['mirna_seq_pipeline.cutadapt_ncpus'] = 2
    output_df['mirna_seq_pipeline.cutadapt_ramGB'] = 7
    output_df['mirna_seq_pipeline.cutadapt_disk'] = 'local-disk 200 SSD'
    output_df['mirna_seq_pipeline.star_ncpus'] = 16
    output_df['mirna_seq_pipeline.star_ramGB'] = 60
    output_df['mirna_seq_pipeline.star_disk'] = 'local-disk 200 SSD'
    output_df['mirna_seq_pipeline.wigtobigwig_ncpus'] = 2
    output_df['mirna_seq_pipeline.wigtobigwig_ramGB'] = 7
    output_df['mirna_seq_pipeline.wigtobigwig_disk'] = 'local-disk 200 SSD'

    # Identify missing or incorrect data.
    missing_fastqs_filter = output_df['mirna_seq_pipeline.fastqs'].apply(lambda x: [] in x)
    missing_fastqs_detected = output_df[missing_fastqs_filter]['mirna_seq_pipeline.experiment_prefix'].to_list()
    no_adapters_filter = output_df['mirna_seq_pipeline.five_prime_adapters'].apply(lambda x: [None] in x)
    no_adapters_detected = output_df[no_adapters_filter]['mirna_seq_pipeline.experiment_prefix'].to_list()
    no_organism_detected = output_df.loc[output_df['mirna_seq_pipeline.star_index'] == '']['mirna_seq_pipeline.experiment_prefix'].to_list()

    # Print error messages to terminal.
    for accession in missing_fastqs_detected:
        print('ERROR: Missing fastqs in experiment {}.'.format(accession))
    for accession in extra_adapters_detected:
        print('ERROR: More than 1 5\' adapter detected for library in experiment {}.'.format(accession))
    for accession in no_adapters_detected:
        print('ERROR: Missing adapters in experiment {}.'.format(accession))
    for accession in no_organism_detected:
        print('ERROR: Missing star_index, mirna_annotation, and chrom_sizes in experiment {}.'.format(accession))

    # Drop items which had errors from the table.
    output_df.drop(
        missing_fastqs_detected +
        extra_adapters_detected +
        no_adapters_detected +
        no_organism_detected,
        inplace=True)

    # Convert data frame to a dictionary in which each key corresponds to one row (experiment) of the table
    output_dict = output_df.to_dict('index')
    command_output = ''
    for experiment in output_dict:
        accession = output_dict[experiment]['mirna_seq_pipeline.experiment_prefix']
        file_name = f'{accession}_mirna_{len(output_dict[experiment]["mirna_seq_pipeline.fastqs"])}rep.json'

        # Write a corresponding caper command.
        command_output = command_output + 'caper submit {} -i {}{} -s {}{}\nsleep 1\n'.format(
            wdl,
            (gc_path + '/' if not gc_path.endswith('/') else gc_path),
            file_name,
            file_name[:-5],
            ('_' + output_dict[experiment]['custom_message'] if output_dict[experiment]['custom_message'] != '' else '')
        )

        # Write as .json file
        for prop in list(output_dict[experiment]):
            if output_dict[experiment][prop] in (None, [], '') or (type(output_dict[experiment][prop]) == list and None in output_dict[experiment][prop]):
                output_dict[experiment].pop(prop)
        output_dict[experiment].pop('custom_message')

        with open(f'{output_path}{"/" if output_path else ""}{file_name}', 'w') as output_file:
            output_file.write(json.dumps(output_dict[experiment], indent=4))

    if command_output != '':
        commands_file_path = (
            f'{output_path}{"/" if output_path else ""}'
            f'caper_submit{"_" if caper_commands_file_message else ""}{caper_commands_file_message}.sh'
        )
        with open(commands_file_path, 'w') as command_output_file:
            command_output_file.write(command_output)


if __name__ == '__main__':
    main()
