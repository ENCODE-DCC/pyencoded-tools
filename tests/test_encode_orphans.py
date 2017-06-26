import os
import pytest

import ENCODE_orphans


def test_get_id_type():
    assert ENCODE_orphans.get_id_type('ENCFF931XGV') == 'accession'
    assert ENCODE_orphans.get_id_type(
        '7eb14a04-460f-4a9d-a6de-6d77595b8fad') == 'uuid'
    assert ENCODE_orphans.get_id_type(
        '/analysis-steps/chia_pet_preprocessing_step/') == '@id'
    with pytest.raises(ValueError):
        assert ENCODE_orphans.get_id_type([])
        assert ENCODE_orphans.get_id_type('sdfsd')


def test_extract_profile():
    accession_data = ENCODE_orphans.extract_profile('ENCFF252XCG', test_data)
    assert accession_data['accession'] == 'ENCFF252XCG'
    assert accession_data['lab'] == 'encode-processing-pipeline'

    id_data = ENCODE_orphans.extract_profile(
        '/documents/728fc31a-b30c-494e-a41e-0c4b5179c36c/', test_data)
    assert id_data['accession'] == '/documents/728fc31a-b30c-494e-a41e-0c4b5179c36c/'

    uuid_data = ENCODE_orphans.extract_profile(
        '123a9667-abb2-442a-93ed-3715c67534ea', test_data)
    assert uuid_data['accession'] == '123a9667-abb2-442a-93ed-3715c67534ea'


def test_parse_data():
    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[0]], 'accession')
    assert len(data) == 1
    assert len(childless_parents) == 0

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[1]], 'no_field')
    assert len(data) == 0
    assert len(childless_parents) == 1

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[2]], 'biosample.accession')
    assert len(data) == 1
    assert len(childless_parents) == 0

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[3]], 'donor.@id')
    assert len(data) == 2
    assert len(childless_parents) == 0

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[4]], 'donor.@id')
    assert len(data) == 0
    assert len(childless_parents) == 1

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[5]], 'donor.@id')
    assert len(data) == 0
    assert len(childless_parents) == 1

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[6]], 'donor.@id')
    assert len(data) == 4
    assert len(childless_parents) == 0

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[7]], 'donor.library.accession')
    assert len(data) == 1
    assert len(childless_parents) == 0

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[8]], 'donor.library.accession')
    assert len(data) == 1
    assert len(childless_parents) == 0

    data, childless_parents = ENCODE_orphans.parse_data(
        [parse_data_test_data[9]], 'donor.library.biosample.accession')
    assert len(data) == 2
    assert len(childless_parents) == 0


test_data = [{'file_size': 6674976, 'derived_from': ['/files/ENCFF042GNN/', '/files/ENCFF837IOW/', '/files/ENCFF339TGE/', '/files/ENCFF958RTR/'], 'assembly': 'hg19', 'technical_replicates': ['1_1', '2_1'], 'title': 'ENCFF252XCG', '@type': ['File', 'Item'], 'file_format': 'bed', '@id': '/files/ENCFF252XCG/', 'award': {'project': 'ENCODE'}, 'date_created': '2017-06-20T21:31:53.638510+00:00', 'analysis_step_version': {'@id': '/analysis-step-versions/63326924-00a7-414f-ba7d-923c0f25da92/', 'software_versions': [{'download_checksum': '66cfe40711edb914cd0fd69778a54e37', 'schema_version': '3', 'notes': "ccQualityControl.v.1.1 IS phantompeakqualtools 2.0 (as determined by 'grep Version phantompeakqualtools/README.txt')", 'uuid': '88756cca-1e79-4086-92ad-5f15ebb1fd5a', 'downloaded_url': 'https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/phantompeakqualtools/ccQualityControl.v.1.1.tar.gz', 'software': {'schema_version': '4', 'purpose': ['data QC', 'ChIP-seq', 'DNase-seq', 'RNA-seq'], 'lab': '/labs/encode-consortium/', 'versions': ['/software-versions/88756cca-1e79-4086-92ad-5f15ebb1fd5a/'], 'uuid': 'dcf9781d-1038-47c8-9943-e285c7a6c785', 'title': 'Phantompeakqualtools', 'source_url': 'http://code.google.com/p/phantompeakqualtools/', 'software_type': ['quality metric', 'filtering'], 'name': 'phantompeakqualtools', '@id': '/software/phantompeakqualtools/', 'award': '/awards/ENCODE/', 'description': 'Used to generate three quality metrics: NSC, RSC, and PBC.  The NSC (Normalized strand cross-correlation) and RSC (relative strand cross-correlation) metrics use cross-correlation of stranded read density profiles to measure enrichment independently of peak calling. The PBC (PCR bottleneck coefficient) is an approximate measure of library complexity. PBC is the ratio of (non-redundant, uniquely mappable reads)/(uniquely mappable reads).', 'date_created': '2014-08-13T01:30:18.335561+00:00', '@type': ['Software', 'Item'], 'aliases': [], 'references': ['/publications/6d54a249-990c-48c1-82a8-685bb3fd3b2b/', '/publications/f291aded-5b34-4238-9611-eedc4e1ef6f5/'], 'used_by': ['ENCODE'], 'status': 'released', 'submitted_by': '/users/ce2bde01-07ec-4b8a-b179-554ef95b71dd/'}, '@type': ['SoftwareVersion', 'Item'], '@id': '/software-versions/88756cca-1e79-4086-92ad-5f15ebb1fd5a/', 'date_created': '2015-10-16T01:06:56.970496+00:00', 'aliases': ['encode:phantompeakqualtools 1.1', 'dnanexus:ccQualityControl.v.1.1'], 'version': '1.1', 'status': 'released', 'submitted_by': '/users/6800d05f-7213-48b1-9ad8-254c73c5b83f/'}, {'download_checksum': '4de207d570999170c1bf45bcba8c6d2d', 'schema_version': '3', 'uuid': 'd7608aac-8fc1-49ef-ba54-72c79aa57061', 'downloaded_url': 'https://github.com/hms-dbmi/spp/archive/1.14.zip', 'version': '1.14', '@type': ['SoftwareVersion', 'Item'], '@id': '/software-versions/d7608aac-8fc1-49ef-ba54-72c79aa57061/', 'date_created': '2016-10-18T04:41:33.714800+00:00', 'aliases': ['encode:spp 1.14'], 'status': 'released', 'software': {'schema_version': '4', 'purpose': ['ChIP-seq', 'DNase-seq'], 'lab': '/labs/encode-consortium/', 'versions': ['/software-versions/beb915d5-157c-48d2-9867-7d2bbf8917fe/', '/software-versions/021b7765-014b-48b5-8258-436c8f378e3c/', '/software-versions/e1ed8453-f332-49c8-b8c0-bb33c1494da4/', '/software-versions/ffb5a817-e963-4694-9aae-cd89a832e1a1/', '/software-versions/d7608aac-8fc1-49ef-ba54-72c79aa57061/'], 'uuid': 'e0c8602f-bd34-49cf-ae0a-e85f13e82470', 'title': 'SPP', 'source_url': 'http://compbio.med.harvard.edu/Supplements/ChIP-seq/', 'software_type': ['peak caller'], 'name': 'spp', '@id': '/software/spp/', 'award': '/awards/ENCODE/', 'description': 'A ChIP-seq peak calling algorithm, implemented as an R package, that accounts for the offset in forward-strand and reverse-strand reads to improve resolution, compares enrichment in signal to background or control experiments, and can also estimate whether the available number of reads is sufficient to achieve saturation, meaning that additional reads would not allow identification of additional peaks. SPP will be used in the ENCODE 3 uniform peak calling pipeline.', 'date_created': '2014-08-13T01:30:13.985607+00:00', '@type': ['Software', 'Item'], 'aliases': [], 'references': ['/publications/8a6e0ea0-6b0e-4fdc-885e-297c19cd75aa/'], 'used_by': ['ENCODE'], 'status': 'released', 'submitted_by': '/users/ce2bde01-07ec-4b8a-b179-554ef95b71dd/'}, 'submitted_by': '/users/6800d05f-7213-48b1-9ad8-254c73c5b83f/'}], 'date_created': '2016-10-18T04:36:53.223945+00:00', 'schema_version': '3', 'aliases': ['encode:tf-spp-peak-calling-step-v-1.2'], 'uuid': '63326924-00a7-414f-ba7d-923c0f25da92', 'version': 2, 'status': 'released', 'analysis_step': {'documents': [], '@id': '/analysis-steps/tf-spp-peak-calling-step/', 'schema_version': '5', 'pipelines': [{'documents': ['/documents/7009beb8-340b-4e71-b9db-53bb020c7fe2/'], 'assay_term_id': 'OBI:0000716', 'schema_version': '7', 'lab': '/labs/encode-processing-pipeline/', 'assay_term_name': 'ChIP-seq', 'analysis_steps': ['/analysis-steps/tf-spp-peak-calling-step/', '/analysis-steps/tf-macs2-signal-calling-step/', '/analysis-steps/tf-idr-step/', '/analysis-steps/tf-peaks-to-bigbed-step/', '/analysis-steps/tf-idr-peaks-to-bigbed-step/'], 'uuid': 'd7d8ed09-5caa-47bc-8548-430edcbdbcaa', 'title': 'Transcription factor ChIP-seq', 'source_url': 'https://github.com/ENCODE-DCC/chip-seq-pipeline', '@type': ['Pipeline', 'Item'], '@id': '/pipelines/ENCPL138KID/', 'award': '/awards/ENCODE/', 'description': '', 'date_created': '2015-10-28T21:43:04.192637+00:00', 'aliases': [], 'references': [], 'status': 'active', 'submitted_by': '/users/6800d05f-7213-48b1-9ad8-254c73c5b83f/', 'alternate_accessions': [], 'accession': 'ENCPL138KID'}], 'aliases': ['encode:tf-spp-peak-calling-step-v-1'], 'uuid': '7a74345f-f75f-49e5-9001-eca3d3dfdc64', 'versions': ['/analysis-step-versions/8250c103-b2a7-4ac7-842c-65e2ef651fd6/', '/analysis-step-versions/63326924-00a7-414f-ba7d-923c0f25da92/'], 'title': 'TF ChIP SPP peak calling step', 'name': 'tf-spp-peak-calling-step', '@type': ['AnalysisStep', 'Item'], 'output_file_types': [
    'peaks'], 'input_file_types': ['alignments'], 'analysis_step_types': ['peak calling'], 'current_version': '/analysis-step-versions/63326924-00a7-414f-ba7d-923c0f25da92/', 'status': 'released', 'date_created': '2015-10-28T21:42:25.477498+00:00', 'submitted_by': '/users/6800d05f-7213-48b1-9ad8-254c73c5b83f/'}, '@type': ['AnalysisStepVersion', 'Item'], 'submitted_by': '/users/6800d05f-7213-48b1-9ad8-254c73c5b83f/'}, 'lab': {'city': '', 'institute_name': '', 'schema_version': '4', 'phone1': '', 'postal_code': '', 'fax': '', 'uuid': 'a558111b-4c50-4b2e-9de8-73fd8fd3a67d', 'title': 'ENCODE Processing Pipeline', 'name': 'encode-processing-pipeline', '@type': ['Lab', 'Item'], '@id': '/labs/encode-processing-pipeline/', 'phone2': '', 'institute_label': '', 'state': '', 'address1': '', 'awards': ['/awards/U41HG007000/', '/awards/U41HG006992/'], 'status': 'current', 'pi': '/users/8b1f8780-b5d6-4fb7-a5a2-ddcec9054288/', 'country': 'USA'}, 'step_run': {'@id': '/analysis-step-runs/2174cb43-530c-4058-908c-5794fdf1691f/', 'dx_applet_details': [{'dx_status': 'finished', 'dx_job_id': 'dnanexus:job-F54YGbQ03699GQ8PJ2Z5P4xX'}], 'date_created': '2017-06-20T21:32:29.820182+00:00', 'schema_version': '4', 'aliases': ['dnanexus:job-F54YGbQ03699GQ8PJ2Z5P4xX'], 'analysis_step_version': '/analysis-step-versions/63326924-00a7-414f-ba7d-923c0f25da92/', 'status': 'released', 'uuid': '2174cb43-530c-4058-908c-5794fdf1691f', '@type': ['AnalysisStepRun', 'Item'], 'submitted_by': '/users/9851ccbc-2df9-4529-a4f3-90edee981fc0/'}, 'biological_replicates': [1, 2], 'dataset': '/experiments/ENCSR000BIF/', 'output_category': 'annotation', 'file_format_type': 'narrowPeak', 'output_type': 'peaks', 'file_type': 'bed narrowPeak', 'accession': 'ENCFF252XCG', 'href': '/files/ENCFF252XCG/@@download/ENCFF252XCG.bed.gz', 'status': 'released', 'quality_metrics': []}, {'@id': '/documents/728fc31a-b30c-494e-a41e-0c4b5179c36c/', 'award': {'@id': '/awards/U54HG006998/', 'name': 'U54HG006998'}, 'description': 'Secondary antibody characterization of ENCAB284TTY', 'lab': {'title': 'Richard Myers, HAIB'}, 'attachment': {'type': 'application/pdf', 'download': 'EP300-H3115-WL.xlsx - Sheet1.pdf', 'href': '@@download/attachment/EP300-H3115-WL.xlsx%20-%20Sheet1.pdf'}, 'document_type': 'standards document', '@type': ['Document', 'Item'], 'submitted_by': {'title': 'Mark Mackiewicz'}}, {'documents': [], '@id': '/analysis-steps/tf-unreplicated-idr-peaks-to-bigbed-step/', 'schema_version': '5', 'pipelines': ['/pipelines/ENCPL493SGC/'], 'versions': ['/analysis-step-versions/fa044b11-91ff-4320-a212-5f98f9f7e165/'], 'uuid': '123a9667-abb2-442a-93ed-3715c67534ea', 'title': 'TF unreplicated ChIP IDR peaks to bigBed file format conversion step', 'name': 'tf-unreplicated-idr-peaks-to-bigbed-step', '@type': ['AnalysisStep', 'Item'], 'output_file_types': ['bigBed'], 'input_file_types': ['pseudoreplicated idr thresholded peaks'], 'analysis_step_types': ['file format conversion'], 'aliases': ['encode:tf-unreplicated-idr-peaks-to-bigbed-step-v-1'], 'current_version': {'@id': '/analysis-step-versions/fa044b11-91ff-4320-a212-5f98f9f7e165/', 'date_created': '2017-03-21T00:14:31.444896+00:00', 'schema_version': '3', 'uuid': 'fa044b11-91ff-4320-a212-5f98f9f7e165', 'version': 1, 'software_versions': [{'download_checksum': '5b9b69b5d01fa8cc43e61bb58df537f1', 'schema_version': '3', 'aliases': ['encode:bedToBigBed 2.6'], 'uuid': '143e5445-7f10-48fe-9455-5f1a777773ca', 'downloaded_url': 'https://github.com/ENCODE-DCC/kentUtils/blob/v302.1.0/bin/linux.x86_64/bedToBigBed', 'version': '2.6', '@type': ['SoftwareVersion', 'Item'], '@id': '/software-versions/143e5445-7f10-48fe-9455-5f1a777773ca/', 'date_created': '2015-02-19T19:19:37.771038+00:00', 'software': {'schema_version': '4', 'lab': '/labs/encode-consortium/', 'versions': ['/software-versions/143e5445-7f10-48fe-9455-5f1a777773ca/', '/software-versions/8605897a-3732-42a1-9ad9-bb916f383fcb/'], 'uuid': '1431a1b1-a52e-4170-92b7-dfbd4bb5b8fe', 'title': 'bedToBigBed', 'source_url': 'http://hgdownload.cse.ucsc.edu/admin/exe/', '@type': ['Software', 'Item'], 'name': 'bedToBigBed', '@id': '/software/bedToBigBed/', 'award': '/awards/ENCODE/', 'description': 'bedToBigBed takes a standard bed file or a non-standard bed file with associated .as file to create a compressed bigBed version.  Description of Big Binary Indexed (BBI) files and visualization of next-generation sequencing experiment results explained by W.J. Kent, PMCID: PMC2922891.', 'date_created': '2015-02-18T01:10:01.762666+00:00', 'software_type': ['file format conversion'], 'aliases': [], 'references': [], 'used_by': ['ENCODE'], 'url': 'http://genome.ucsc.edu/goldenpath/help/bigBed.html', 'status': 'released', 'submitted_by': '/users/81a6cc12-2847-4e2e-8f2c-f566699eb29e/'}, 'status': 'released', 'submitted_by': '/users/81a6cc12-2847-4e2e-8f2c-f566699eb29e/'}], 'analysis_step': '/analysis-steps/tf-unreplicated-idr-peaks-to-bigbed-step/', 'status': 'released', 'aliases': [], '@type': ['AnalysisStepVersion', 'Item'], 'submitted_by': '/users/6800d05f-7213-48b1-9ad8-254c73c5b83f/'}, 'parents': [{'documents': [], '@id': '/analysis-steps/tf-unreplicated-idr-step/', 'schema_version': '5', 'pipelines': ['/pipelines/ENCPL493SGC/'], 'versions': ['/analysis-step-versions/abd4401d-96aa-4c37-894d-8e0fee9273ec/'], 'uuid': '19dec341-2dd2-45aa-a9ad-48dd035a8cf0', 'title': 'TF unreplicated ChIP IDR step', 'name': 'tf-unreplicated-idr-step', '@type': ['AnalysisStep', 'Item'], 'output_file_types': ['pseudoreplicated idr thresholded peaks'], 'input_file_types': ['peaks'], 'analysis_step_types': ['unreplicated IDR'], 'aliases': ['encode:tf-unreplicated-idr-step-v-1'], 'current_version': '/analysis-step-versions/abd4401d-96aa-4c37-894d-8e0fee9273ec/', 'parents': ['/analysis-steps/tf-unreplicated-spp-peak-calling-step/'], 'status': 'released', 'date_created': '2017-03-21T00:14:31.005535+00:00', 'submitted_by': '/users/6800d05f-7213-48b1-9ad8-254c73c5b83f/'}], 'status': 'released', 'date_created': '2017-03-21T00:14:31.136373+00:00', 'submitted_by': '/users/6800d05f-7213-48b1-9ad8-254c73c5b83f/'}]


parse_data_test_data = [test_data[0],
                        {'accession': 'ENCFF339TGE'},
                        {'biosample': [{'accession': 'ENCFF339TGE'}]},
                        {'donor': [{'@id': 'id_1'}, {'@id': 'id_2'}]},
                        {'donor': []},
                        {'donor': [{'no_fieild': 'nothing'}]},
                        {'donor': ['id1', 'id2', 'id3', 'id4']},
                        {'donor': {'library': {'accession': 'extract_me'}}},
                        {'donor': {'library': {'accession': ['extract_me',
                                                             'extract_me']}}},
                        {'donor': {'library': {'biosample': [{'accession': 'extract_me'},
                                                             {'accession': 'extract_me_too'}]}}}]
