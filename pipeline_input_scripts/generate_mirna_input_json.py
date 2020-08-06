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
from pandas.io.json import json_normalize
import requests


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-i', "--infile", required=True, action='store',
                        help="""Path to .txt file containing accessions of experiments to process or list of accessions separated by commas.""")
    parser.add_argument('-o', '--outputpath', action='store', default='',
                        help="""Optional path to output folder. Defaults to current path.""")
    parser.add_argument('-g', '--gcpath', action='store', default='',
                        help="""Optional path where the input.json will be uploaded to the Google Cloud instance. Only affects the list of caper commands that is generated.""")
    parser.add_argument('-s', '--server', action='store', default='https://www.encodeproject.org/',
                        help="""Optional specification of server using the full URL. Defaults to production server.""")
    parser.add_argument('-r', '--replicates', action='store', default=[1,2], nargs='*', type=int,
                        help="""Optional specification of biological replicates to use. Any number of biological replicates can be added, such as: -r 1 2 4 8. For each biological replicate, the script will collect every associated technical replicate. These biological replicates will be used for every experiment. The script assumes the user is entering valid replicate numbers in the correct order, so errors will occur if nonexistent replicates are specified. It is also up to the user to ensure that the adapters match the given replicates. Defaults to [1,2].""")
    parser.add_argument('--s3_uri', action='store_true', default=False,
                        help="""Optional flag to use s3_uri links from the ENCODE portal. Otherwise, defaults to using http links.""")
    parser.add_argument('--caper-commands-file-message', action='store', default='',
                        help="""An additional custom string to be appended to the file name of the caper submit commands.""")
    parser.add_argument('--wdl', action='store', default=False,
                        help="""Path to .wdl file.""")
    return parser


def parse_infile(infile):
    if infile.endswith('.txt'):
        try:
            accs_list = []
            with open(infile, 'r') as file:
                for line in file:
                    accs_list.append(line.strip())
            return accs_list
        except:
            raise Exception('Input file could not be read.')
            exit()
    else:
        accs_list = infile.split(',')
        return accs_list


def build_experiment_report_query(experiment_list, server):
    joined_list = '&accession='.join(experiment_list)
    return server + 'report/?type=Experiment' + \
        '&accession={}'.format(joined_list) + \
        '&field=@id' + \
        '&field=accession' + \
        '&field=assay_title' + \
        '&field=files.@id' + \
        '&field=replicates.biological_replicate_number' + \
        '&field=replicates.technical_replicate_number' + \
        '&field=replicates.status' + \
        '&field=replicates.library.biosample.organism.scientific_name' + \
        '&field=replicates.library.adapters' + \
        '&field=documents' + \
        '&field=files.s3_uri' + \
        '&field=files.href' + \
        '&limit=all' + \
        '&format=json'


def build_file_report_query(experiment_list, server):
    joined_list = '&dataset='.join(experiment_list)
    return server + 'report/?type=File' + \
        '&dataset={}'.format(joined_list) + \
        '&file_format=fastq' + \
        '&field=@id' + \
        '&field=dataset' + \
        '&field=replicate.technical_replicate_number' + \
        '&field=replicate.biological_replicate_number' + \
        '&field=replicate.library.adapters' + \
        '&field=status' + \
        '&field=s3_uri' + \
        '&field=href' + \
        '&field=replicate.status' + \
        '&limit=all' + \
        '&format=json'


def check_path_trailing_slash(path):
    if path.endswith('/') or len(path) == 0:
        return path
    else:
        return path + '/'


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

    # Store the provided replicate numbers.
    replicate_nums = args.replicates

    # Set the output paths and load the list of experiments to process.
    gc_path = check_path_trailing_slash(args.gcpath)
    output_path = check_path_trailing_slash(args.outputpath)
    caper_commands_file_message = args.caper_commands_file_message
    wdl = args.wdl
    infile = args.infile
    experiment_list = parse_infile(infile)

    # Retrieve experiment report view json with necessary fields and store as DataFrame.
    experiment_report = requests.get(
        build_experiment_report_query(experiment_list, server),
        auth=keypair,
        headers={'content-type': 'application/json'})
    experiment_report_json = json.loads(experiment_report.text)
    experiment_input_df = json_normalize(experiment_report_json['@graph'])

    # Retrieve file report view json with necessary fields and store as DataFrame.
    file_report = requests.get(
        build_file_report_query(experiment_input_df.get('@id'), server),
        auth=keypair,
        headers={'content-type': 'application/json'})
    file_report_json = json.loads(file_report.text)
    file_input_df = json_normalize(file_report_json['@graph'])
    file_input_df.set_index(link_src, inplace=True)

    # Create output DataFrame to store all data for the input.json files.
    output_df = pd.DataFrame()
    # Assign experiment_prefix values. For simplicity, this is the same as the experiment accession.
    output_df['mirna_seq_pipeline.experiment_prefix'] = experiment_input_df['accession']
    output_df.set_index('mirna_seq_pipeline.experiment_prefix', inplace=True, drop=False)

    # To correctly get the information for fastqs and adapters, this script looks up fastq files associated with each specified experiment in a 2nd table containing only file metadata. 
    final_input_fastq_links = []
    final_input_five_prime_adapters = []
    allowed_statuses = ['released', 'in progress']
    # List to store Experiment accessions with more than 1 fastq found per replicate.
    extra_adapters_detected = []

    for experiment_files in experiment_input_df.get('files'):
        # Arrays to store the list of arrays of fastqs/adapters grouped by biological replicates.
        links_sorted_by_rep = []
        adapters_sorted_by_rep = []

        # Arrays to store metadata for all fastqs in an experiment.
        fastq_links_by_rep = []
        bio_reps = []
        tech_reps = []
        file_to_dataset = []
        adapters_by_rep = []
        file_rep_sorter = pd.DataFrame()

        # Iterate over each file in the current experiment and collect data on fastq files
        for file in experiment_files:
            link = file[link_src]
            # Check fastq files for correct status and replicate #
            if link.endswith('fastq.gz') \
                    and link in file_input_df.index \
                    and file_input_df.loc[link].at['status'] in allowed_statuses \
                    and file_input_df.loc[link].at['replicate.status'] in allowed_statuses:

                fastq_links_by_rep.append(link_prefix + link)
                bio_reps.append(file_input_df.loc[link].at['replicate.biological_replicate_number'])
                tech_reps.append(file_input_df.loc[link].at['replicate.technical_replicate_number'])
                file_to_dataset.append(file_input_df.loc[link].at['dataset'][13:24])

                adapter_accession = None
                for adapter in file_input_df.loc[link].at['replicate.library.adapters']:
                    if adapter['type'] == "5' adapter" and adapter_accession is None:
                        adapter_accession = '{}{}@@download/{}.txt.gz'.format(
                            server,
                            adapter['file'][1:],
                            adapter['file'][7:18])
                    elif adapter['type'] == "3' adapter":
                        continue
                    else:
                        extra_adapters_detected.append(file_input_df.loc[link].at['dataset'][13:24])
                adapters_by_rep.append(adapter_accession)

        # Add the collected data to a new DataFrame
        file_rep_sorter['fastq'] = fastq_links_by_rep
        file_rep_sorter['biorep'] = bio_reps
        file_rep_sorter['techrep'] = tech_reps
        file_rep_sorter['repstrings'] = file_rep_sorter['biorep'].astype(str).str.cat(others=file_rep_sorter['techrep'].astype(str), sep=',')
        file_rep_sorter['dataset'] = file_to_dataset
        file_rep_sorter['adapter'] = adapters_by_rep

        # Sort the DataFrame so that files are now ordered by replicate: 1_1, 1_2, 1_3, 2_1, 2_2...
        file_rep_sorter.sort_values(by=['biorep', 'techrep'], inplace=True)
        file_rep_sorter.reset_index(drop=True, inplace=True)

        for rep_num in replicate_nums:
            # Iterate over the biological replicates, appending an array of all technical replicates associated with that biological replicate.
            links_sorted_by_rep.append(list(file_rep_sorter.loc[file_rep_sorter['biorep'] == rep_num]['fastq']))
            adapters_sorted_by_rep.append(list(file_rep_sorter.loc[file_rep_sorter['biorep'] == rep_num]['adapter']))

        # Lastly, append the list of lists to (yet another) list, which is the experiment level one.
        final_input_fastq_links.append(links_sorted_by_rep)
        final_input_five_prime_adapters.append(adapters_sorted_by_rep)

    output_df['mirna_seq_pipeline.fastqs'] = final_input_fastq_links
    output_df['mirna_seq_pipeline.five_prime_adapters'] = final_input_five_prime_adapters

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
            star_indices.append(server + 'files/ENCFF123QLG/@@download/ENCFF123QLG.tar.gz')
            mirna_annotations.append(server + 'files/ENCFF470CZH/@@download/ENCFF470CZH.gtf.gz')
            chrom_sizes.append(server + 'files/GRCh38_EBV.chrom.sizes/@@download/GRCh38_EBV.chrom.sizes.tsv')
        elif ''.join(organism) == 'Mus musculus':
            star_indices.append(server + 'files/ENCFF795AAA/@@download/ENCFF795AAA.tar.gz')
            mirna_annotations.append(server + 'files/ENCFF094ICJ/@@download/ENCFF094ICJ.gtf.gz')
            chrom_sizes.append(server + 'files/mm10_no_alt.chrom.sizes/@@download/mm10_no_alt.chrom.sizes.tsv')
        else:
            star_indices.append('')
            mirna_annotations.append('')
            chrom_sizes.append('')

    output_df['mirna_seq_pipeline.star_index'] = star_indices
    output_df['mirna_seq_pipeline.mirna_annotation'] = mirna_annotations
    output_df['mirna_seq_pipeline.chrom_sizes'] = chrom_sizes

    # Assign other parameters, which are identical for all runs.
    output_df['mirna_seq_pipeline.three_prime_adapters'] = server + 'files/ENCFF463QEL/@@download/ENCFF463QEL.txt'
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
        # Write as .json file
        with open('{}input_{}.json'.format(output_path, accession), 'w') as output_file:
            output_file.write(json.dumps(output_dict[experiment], indent=4))
        # Write a corresponding caper command.
        command_output = command_output + 'caper submit {} -i {}{} -s {}_final_run\n'.format(
            wdl,
            gc_path,
            '{}input_{}.json'.format(output_path, accession),
            accession)

    if command_output != '':
        with open(f'{output_path}caper_submit{"_" if caper_commands_file_message else ""}{caper_commands_file_message}.sh', 'w') as command_output_file:
            command_output_file.write(command_output)


if __name__ == '__main__':
    main()
