import argparse
import json
import os
import pandas as pd
import requests


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-i', "--infile", required=True, action='store',
                        help="""Path to .txt file containing accessions of experiments to process or list of accessions separated by commas. The txt file must contain one column with 1 header row and should be labeled 'accession'. It can optionally include a 2nd column for 'custom_message'.""")

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


def parse_infile(infile):
    try:
        infile_df = pd.read_csv(infile, '\t')
        return infile_df
    except FileNotFoundError as e:
        print(e)
        exit()


def build_experiment_report_query(experiment_list, server):
    joined_list = '&accession='.join(experiment_list)
    query = server + '/report/?type=Experiment' + \
        f'&accession={joined_list}' + \
        '&field=@id' + \
        '&field=accession' + \
        '&field=assay_title' + \
        '&field=files.s3_uri' + \
        '&field=files.href' + \
        '&field=files.status' + \
        '&field=files.file_format' + \
        '&field=files.biological_replicates' + \
        '&field=files.paired_end' + \
        '&field=files.paired_with' + \
        '&field=files.run_type' + \
        '&field=files.replicate.status' + \
        '&field=replicates.library.strand_specificity' + \
        '&field=files.run_type' + \
        '&field=replicates.library.average_fragment_size' + \
        '&field=replicates.library.fragment_length_CV' + \
        '&field=replicates.library.biosample.organism.scientific_name' + \
        '&field=replicates.biological_replicate_number' + \
        '&field=replicates.technical_replicate_number' + \
        '&field=replicates.library.uuid' + \
        '&limit=all' + \
        '&format=json'
    print(query)
    return query


def build_file_report_query(experiment_list, server):
    joined_list = '&dataset='.join(experiment_list)
    query = server + '/report/?type=File' + \
        f'&dataset={joined_list}' + \
        '&status=released' + \
        '&status=in+progress' + \
        '&file_format=fastq' + \
        '&output_type=reads' + \
        '&field=@id' + \
        '&field=dataset' + \
        '&field=file_format' + \
        '&field=biological_replicates' + \
        '&field=paired_end' + \
        '&field=paired_with' + \
        '&field=run_type' + \
        '&field=status' + \
        '&field=s3_uri' + \
        '&field=href' + \
        '&field=replicate.status' + \
        '&field=replicate.library.uuid' + \
        '&field=replicate.library.strand_specificity' + \
        '&field=replicate.library.average_fragment_size' + \
        '&field=replicate.library.fragment_length_CV' + \
        '&limit=all' + \
        '&format=json'
    print(query)
    return query


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

    # Gather the list of experiments to query for their files.
    datasets_to_retrieve = experiment_input_df.get('@id').tolist()

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
    # These columns must be present in the File dataframe.
    if 'paired_end' not in file_input_df:
        file_input_df['paired_end'] = None
        file_input_df['paired_with'] = None
    if 'replicate.library.average_fragment_size' not in file_input_df:
        file_input_df['replicate.library.average_fragment_size'] = None
    if 'replicate.library.strand_specificity' not in file_input_df:
        file_input_df['replicate.library.strand_specificity'] = None
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
    ERROR_mixed_endedness = []
    ERROR_mixed_strand_specificity = []

    # Fetch data from the ENCODE portal
    experiment_input_df, file_input_df = get_data_from_portal(infile_df, server, keypair, link_prefix, link_src)

    # Create output_df to store all data for the final input.json files.
    output_df = pd.DataFrame()
    output_df['accession'] = infile_df['accession']
    output_df['rna.bamroot'] = infile_df['accession']
    if 'custom_message' in infile_df:
        output_df['custom_message'] = infile_df['custom_message']
        output_df['custom_message'].fillna('', inplace=True)
    else:
        output_df['custom_message'] = ''
    output_df.set_index('accession', inplace=True, drop=False)

    '''
    Experiment sorting section
    '''

    library_to_rep_num_map = dict()
    # Create a map of library to replicate number to control structuring of fastqs later in the script.
    for experiment, replicates in zip(experiment_input_df.get('accession'), experiment_input_df.get('replicates')):
        library_to_rep_num_map[experiment] = dict()
        for rep in replicates:
            rep_num_reformatted = float(f'{rep["biological_replicate_number"]}.{rep["technical_replicate_number"]}')
            if rep['library']['uuid'] not in library_to_rep_num_map[experiment]:
                library_to_rep_num_map[experiment][rep['library']['uuid']] = rep_num_reformatted
            elif rep_num_reformatted < library_to_rep_num_map[experiment][rep['library']['uuid']]:
                library_to_rep_num_map[experiment][rep['library']['uuid']] = rep_num_reformatted

    # Assign genome reference file.
    align_index = []
    rsem_index = []
    chrom_sizes = []
    kallisto_index = []
    rna_qc_tr_id_to_gene_type_tsv = []

    for assay, replicates in zip(experiment_input_df.get('assay_title'), experiment_input_df.get('replicates')):
        organism = set()

        for rep in replicates:
            organism.add(rep['library']['biosample']['organism']['scientific_name'])

        if ''.join(organism) == 'Homo sapiens':
            align_index.append('https://www.encodeproject.org/files/ENCFF598IDH/@@download/ENCFF598IDH.tar.gz')
            rsem_index.append('https://www.encodeproject.org/files/ENCFF285DRD/@@download/ENCFF285DRD.tar.gz')
            chrom_sizes.append('https://www.encodeproject.org/files/GRCh38_EBV.chrom.sizes/@@download/GRCh38_EBV.chrom.sizes.tsv')
            kallisto_index.append('https://www.encodeproject.org/files/ENCFF471EAM/@@download/ENCFF471EAM.idx')
            rna_qc_tr_id_to_gene_type_tsv.append('https://www.encodeproject.org/files/ENCFF110VAV/@@download/ENCFF110VAV.tsv')

        elif ''.join(organism) == 'Mus musculus':
            align_index.append('https://www.encodeproject.org/files/ENCFF795ZFF/@@download/ENCFF795ZFF.tar.gz')
            rsem_index.append('https://www.encodeproject.org/files/ENCFF363TFV/@@download/ENCFF363TFV.tar.gz')

            chrom_sizes.append('https://www.encodeproject.org/files/mm10_no_alt.chrom.sizes/@@download/mm10_no_alt.chrom.sizes.tsv')
            kallisto_index.append('https://www.encodeproject.org/files/ENCFF383TUX/@@download/ENCFF383TUX.idx')
            rna_qc_tr_id_to_gene_type_tsv.append('https://www.encodeproject.org/files/ENCFF449TPD/@@download/ENCFF449TPD.tsv')

    output_df['rna.align_index'] = align_index
    output_df['rna.rsem_index'] = rsem_index
    output_df['rna.chrom_sizes'] = chrom_sizes
    output_df['rna.kallisto_index'] = kallisto_index
    output_df['rna.rna_qc_tr_id_to_gene_type_tsv'] = rna_qc_tr_id_to_gene_type_tsv

    # To correctly get the information for fastqs and adapters, this script looks up fastq files associated with each specified experiment in a 2nd table containing only file metadata.
    final_input_fastq_links_R1 = []
    final_input_fastq_links_R2 = []
    allowed_statuses = ['released', 'in progress']
    experiment_run_types = []
    experiment_strandedness = []
    experiment_strandedness_direction = []
    run_kallisto = []
    kallisto_fragment_length = []
    kallisto_sd_of_fragment_length = []

    for experiment_files, experiment_id in zip(experiment_input_df.get('files'), experiment_input_df['accession']):
        fastqs_by_rep_R1 = dict()
        fastqs_by_rep_R2 = dict()
        for lib in library_to_rep_num_map[experiment_id]:
            fastqs_by_rep_R1[lib] = []
            fastqs_by_rep_R2[lib] = []

        run_types = set()
        strand_specificity = set()
        average_fragment_size = dict()
        sd_of_fragment_length = dict()
        # Iterate over each file in the current experiment and collect data on fastq files, get strand_specificity and strandedness
        for file in experiment_files:
            link = file[link_src]

            if link.endswith('fastq.gz') \
                    and link in file_input_df.index \
                    and file_input_df.loc[link].at['status'] in allowed_statuses \
                    and file_input_df.loc[link].at['replicate.status'] in allowed_statuses:
                if file_input_df.loc[link].at['paired_end'] == '1':
                    pair = file_input_df.loc[link].at['paired_with']

                    for lib_uuid in fastqs_by_rep_R1:
                        if file_input_df.loc[link].at['replicate.library.uuid'] == lib_uuid:
                            fastqs_by_rep_R1[lib_uuid].append(link_prefix + link)
                            fastqs_by_rep_R2[lib_uuid].append(link_prefix + file_input_df[file_input_df['@id'] == pair].index.values[0])
                elif pd.isnull(file_input_df.loc[link].at['paired_end']):
                    for lib_uuid in fastqs_by_rep_R1:
                        if file_input_df.loc[link].at['replicate.library.uuid'] == lib_uuid:
                            fastqs_by_rep_R1[lib_uuid].append(link_prefix + link)

                # Collect read_lengths and run_types
                run_types.add(file_input_df.loc[link].at['run_type'])

                if pd.isnull(file_input_df.loc[link].at['replicate.library.strand_specificity']):
                    strand_specificity.add('')
                else:
                    strand_specificity.add(file_input_df.loc[link].at['replicate.library.strand_specificity'])

                if not pd.isnull(file_input_df.loc[link].at['replicate.library.average_fragment_size']) and \
                        file_input_df.loc[link].at['replicate.library.uuid'] not in average_fragment_size and \
                        file_input_df.loc[link].at['run_type'] == 'single-end':
                    lib_uuid = file_input_df.loc[link].at['replicate.library.uuid']
                    avg_frag_size = file_input_df.loc[link].at['replicate.library.average_fragment_size']
                    frag_cv = file_input_df.loc[link].at['replicate.library.fragment_length_CV']

                    average_fragment_size[lib_uuid] = round(float(avg_frag_size), 2)
                    sd_of_fragment_length[lib_uuid] = round(float((avg_frag_size*frag_cv)/100), 2)

        # Record error if no fastqs for found for any replicate.
        if all(val == [] for val in fastqs_by_rep_R1.values()):
            print(f'ERROR: no fastqs were found for {experiment_id}.')
            ERROR_no_fastqs.append(experiment_id)

        single_ended = True
        if 'single-ended' in run_types and 'paired-ended' not in run_types:
            experiment_run_types.append('single')
        if 'single-ended' in run_types and 'paired-ended' in run_types:
            experiment_run_types.append('single')
            ERROR_mixed_endedness.append(experiment_id)
        elif next(iter(run_types)) == 'paired-ended':
            single_ended = False
            experiment_run_types.append('paired')

        formatted_fastq_links_R1 = []
        formatted_fastq_links_R2 = []
        # Append the list of lists to (yet another) list, which is the experiment level one.
        # Sort library uuids by replicate numbering.
        sorted_library_to_rep_num = {k: v for k, v in sorted(library_to_rep_num_map[experiment_id].items(), key=lambda x: x[1])}
        for key in sorted_library_to_rep_num:
            if len(fastqs_by_rep_R1[key]) > 0:
                if single_ended:
                    formatted_fastq_links_R1.append(fastqs_by_rep_R1[key])
                else:
                    formatted_fastq_links_R1.append(fastqs_by_rep_R1[key])
                    formatted_fastq_links_R2.append(fastqs_by_rep_R2[key])

        if not single_ended:
            kallisto_fragment_length.append('')
            kallisto_sd_of_fragment_length.append('')
            run_kallisto.append(True)
        elif len(average_fragment_size) == 0 and single_ended:
            kallisto_fragment_length.append('')
            kallisto_sd_of_fragment_length.append('')
            run_kallisto.append(False)
        else:
            sorted_average_fragment_size = []
            sorted_sd_fragment_length = []
            for key in sorted_library_to_rep_num:
                sorted_average_fragment_size.append(average_fragment_size[key])
                sorted_sd_fragment_length.append(sd_of_fragment_length[key])
            kallisto_fragment_length.append(sorted_average_fragment_size)
            kallisto_sd_of_fragment_length.append(sorted_sd_fragment_length)
            run_kallisto.append(True)

        # Append the list of lists to (yet another) list, which is the experiment level one.
        final_input_fastq_links_R1.append(formatted_fastq_links_R1)
        final_input_fastq_links_R2.append(formatted_fastq_links_R2)

        if len(strand_specificity) != 1:
            print(f'ERROR: mixed strand specificities in {experiment_id}.')
            ERROR_mixed_strand_specificity.append(experiment_id)
            experiment_strandedness.append('')
            experiment_strandedness_direction.append('')
        else:
            if 'reverse' in strand_specificity:
                experiment_strandedness.append('stranded')
                experiment_strandedness_direction.append('reverse')
                
            elif 'forward' in strand_specificity:
                experiment_strandedness.append('stranded')
                experiment_strandedness_direction.append('forward')
            elif 'strand-specific' in strand_specificity:
                experiment_strandedness.append('stranded')
                experiment_strandedness_direction.append('reverse')  # Assuming all old experiments are reverse
            elif 'unstranded' in strand_specificity:
                experiment_strandedness.append('unstranded')
                experiment_strandedness_direction.append('unstranded')

    output_df['rna.fastqs_R1'] = final_input_fastq_links_R1
    output_df['rna.fastqs_R2'] = final_input_fastq_links_R2
    output_df['rna.endedness'] = experiment_run_types
    output_df['rna.strandedness'] = experiment_strandedness
    output_df['rna.strandedness_direction'] = experiment_strandedness_direction

    output_df['rna.run_kallisto'] = run_kallisto
    output_df['rna.kallisto_fragment_length'] = kallisto_fragment_length
    output_df['rna.kallisto_sd_of_fragment_length'] = kallisto_sd_of_fragment_length

    # Add pipeline params
    output_df['rna.bam_to_signals_ncpus'] = 8
    output_df['rna.bam_to_signals_ramGB'] = 30
    output_df['rna.kallisto_number_of_threads'] = 8
    output_df['rna.kallisto_ramGB'] = 30
    output_df['rna.align_ncpus'] = 32
    output_df['rna.align_ramGB'] = 120
    output_df['rna.rsem_ncpus'] = 16
    output_df['rna.rsem_ramGB'] = 60
    output_df['rna.align_disk'] = 'local-disk 500 HDD'
    output_df['rna.kallisto_disk'] = 'local-disk 200 HDD'
    output_df['rna.rna_qc_disk'] = 'local-disk 200 HDD'
    output_df['rna.mad_qc_disk'] = 'local-disk 200 HDD'
    output_df['rna.bam_to_signals_disk'] = 'local-disk 200 HDD'
    output_df['rna.rsem_disk'] = 'local-disk 500 HDD'

    output_df.drop(
        ERROR_no_fastqs +
        ERROR_mixed_endedness +
        ERROR_mixed_strand_specificity,
        inplace=True)

    # Output rows of dataframes as input json files.
    output_dict = output_df.to_dict('index')
    command_output = ''

    for experiment in output_dict:
        accession = output_df['accession']
        file_name = f'{accession}_bulk_rna_.json'
        # Build strings of caper commands.
        command_output = command_output + 'caper submit {} -i {}{} -s {}{}\nsleep 1\n'.format(
            wdl_path,
            (gc_path + '/' if not gc_path.endswith('/') else gc_path),
            output_dict[experiment]['accession'] + '.json',
            output_dict[experiment]['accession'],
            ('_' + output_dict[experiment]['custom_message'] if output_dict[experiment]['custom_message'] != '' else ''))

        # Remove empty properties and the custom message property.
        for prop in list(output_dict[experiment]):
            if output_dict[experiment][prop] in (None, [], '', [[]]) or (type(output_dict[experiment][prop]) == list and None in output_dict[experiment][prop]):
                output_dict[experiment].pop(prop)
        output_dict[experiment].pop('accession')
        output_dict[experiment].pop('custom_message')

        file_name = f'{output_path}{"/" if output_path else ""}{output_dict[experiment]["rna.bamroot"]}.json'
        with open(file_name, 'w') as output_file:
            output_file.write(json.dumps(output_dict[experiment], indent=4))

    # Output .txt with caper commands.
    if command_output != '':
        with open(f'{output_path}{"/" if output_path else ""}caper_submit{"_" if caper_commands_file_message else ""}{caper_commands_file_message}.sh', 'w') as command_output_file:
            command_output_file.write(command_output)


if __name__ == '__main__':
    main()
