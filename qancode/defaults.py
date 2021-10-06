from qancode.clickpaths import (DownloadBEDFileFromModal,
                         DownloadBEDFileFromTable,
                         DownloadDocuments,
                         DownloadDocumentsFromAntibodyPage,
                         DownloadFileFromButton,
                         DownloadFileFromFilePage,
                         DownloadFileFromModal,
                         DownloadFileFromTable,
                         DownloadGraphFromExperimentPage,
                         DownloadMetaDataFromSearchPage,
                         DownloadTSVFromReportPage,
                         OpenUCSCGenomeBrowser,
                         OpenUCSCGenomeBrowserCE10,
                         OpenUCSCGenomeBrowserCE11,
                         OpenUCSCGenomeBrowserDM3,
                         OpenUCSCGenomeBrowserDM6,
                         OpenUCSCGenomeBrowserGRCh38,
                         OpenUCSCGenomeBrowserHG19,
                         OpenUCSCGenomeBrowserMM10,
                         OpenUCSCGenomeBrowserMM10Minimal,
                         OpenUCSCGenomeBrowserMM9,
                         OpenUCSCGenomeBrowserCE10fromExperiment,
                         OpenUCSCGenomeBrowserCE11fromExperiment,
                         OpenUCSCGenomeBrowserDM3fromExperiment,
                         OpenUCSCGenomeBrowserDM6fromExperiment,
                         OpenUCSCGenomeBrowserGRCh38fromExperiment,
                         OpenUCSCGenomeBrowserHG19fromExperiment,
                         OpenUCSCGenomeBrowserMM10fromExperiment,
                         OpenUCSCGenomeBrowserMM10MinimalfromExperiment,
                         OpenUCSCGenomeBrowserMM9fromExperiment,
                         ClickSearchResultItem,
                         ClickSearchResultItemAndMakeExperimentPagesLookTheSame,
                         MakeExperimentPagesLookTheSameByClickingFileTab,
                         MakeExperimentPagesLookTheSameByHidingGraph
                         )

# Default browsers.
BROWSERS = ['Chrome',
            'Firefox',
            'Safari']

# Default users.
USERS = ['Public',
         'encoded.test@gmail.com',
         'encoded.test2@gmail.com',
         'encoded.test4@gmail.com']


class bcolors:
    """
    Helper class for text color definitions.
    """
    OKBLUE = '\x1b[36m'
    OKGREEN = '\x1b[1;32m'
    WARNING = '\x1b[33m'
    FAIL = '\x1b[31m'
    ENDC = '\x1b[0m'


class ActionTuples:
    """
    Stores default action lists as attributes.
    Uses form .{method_name}_default_actions. 
    """

    def __init__(self):
        self._init_check_requests_users()
        self._init_default_actions()

    def _init_check_requests_users(self):
        # Define users for check_requests default list.
        self.view_only_lab = USERS[1]
        self.lab_submitter = USERS[2]
        # Don't want disabled account to be part of default USERS list.
        self.disabled_user = 'encoded.test3@gmail.com'
        self.view_only_admin = USERS[3]
        # Common response pattern for check_requests, extracted here to avoid
        # repetition.
        self._released_expected_response = [('Public', 200),
                                            (self.view_only_lab, 200),
                                            (self.lab_submitter, 200),
                                            (self.disabled_user, 401),
                                            (self.view_only_admin, 200),
                                            ('admin', 200)]
        # For objects owned by lab.
        self._unreleased_internal_expected_response = [('Public', 403),
                                                       (self.view_only_lab, 200),
                                                       (self.lab_submitter, 200),
                                                       (self.disabled_user, 401),
                                                       (self.view_only_admin, 200),
                                                       ('admin', 200)]
        self._unreleased_other_expected_response = [('Public', 403),
                                                    (self.view_only_lab, 403),
                                                    (self.lab_submitter, 403),
                                                    (self.disabled_user, 401),
                                                    (self.view_only_admin, 200),
                                                    ('admin', 200)]
        self.edit_by_lab = [(self.view_only_lab, 422),
                            (self.lab_submitter, 200)]
        self.no_edit_by_lab = [(self.view_only_lab, 422),
                               (self.lab_submitter, 422)]
        self.no_edit_non_object_page = [(self.view_only_lab, 403),
                                        (self.lab_submitter, 403)]

    # Default action lists for QANCODE to check.
    def _init_default_actions(self):
        self.compare_facets_default_actions = [
            '/search/?type=Experiment',
            '/search/?type=FunctionalCharacterizationExperiment',
            '/search/?type=File',
            '/search/?type=Library',
            '/search/?type=AntibodyLot',
            '/search/?type=Biosample',
            '/search/?type=BiosampleType',
            '/search/?type=Donor',
            '/search/?type=GeneticModification',
            '/search/?type=FileSet',
            '/search/?type=Annotation',
            '/search/?type=Series',
            '/search/?type=OrganismDevelopmentSeries',
            '/search/?type=UcscBrowserComposite',
            '/search/?type=ReferenceEpigenome',
            '/search/?type=Project',
            '/search/?type=ReplicationTimingSeries',
            '/search/?type=PublicationData',
            '/search/?type=MatchedSet',
            '/search/?type=TreatmentConcentrationSeries',
            '/search/?type=TreatmentTimeSeries',
            '/search/?type=Target',
            '/search/?type=Pipeline',
            '/search/?type=Publication',
            '/search/?type=Software',
            '/matrix/?type=Experiment',
            '/matrix/?type=Annotation'
        ]
        self.find_differences_default_actions = [
            ('/', None),
            ('/help/rest-api/', None),
            ('/summary/?type=Experiment', None),
            ('/chip-seq/histone/', None),
            ('/tutorials/encode-users-meeting-2016/lodging/', None),
            ('/annotations/ENCSR790GQB/', None),
            ('/antibodies/ENCAB000AIW/', None),
            ('/antibodies/ENCAB014HCW/', None),
            ('/search/?type=AntibodyLot&status=in+progress&sort=date_created', ClickSearchResultItem),
            ('/awards/U01HG007919/', None),
            ('/biosamples/ENCBS286AAA/', None),
            ('/biosamples/ENCBS464EKT/', None),
            ('/biosamples/ENCBS829SAQ/', None),
            ('/biosamples/ENCBS096SHR', None),
            ('/search/?type=Biosample&status=in+progress&audit=*&sort=date_created', ClickSearchResultItem),
            ('/biosamples/ENCBS349CLX/', None),
            ('/biosamples/ENCBS682JHS/', None),
            ('/cart-manager/', None),
            ('/cart-view/', None),
            ('/encyclopedia/?type=File&annotation_type=candidate+Cis-Regulatory+Elements&assembly=GRCh38&file_format=bigBed&file_format=bigWig&encyclopedia_version=current', None),
            ('/experiment-series/ENCSR447BSF/', None),
            ('/experiment-series/ENCSR770EIH/', None),
            ('/experiments/ENCSR178NTX/', MakeExperimentPagesLookTheSameByClickingFileTab),
            ('/experiments/ENCSR255XZG/', MakeExperimentPagesLookTheSameByClickingFileTab),
            ('/experiments/ENCSR651NGR/', MakeExperimentPagesLookTheSameByClickingFileTab),
            ('/experiments/ENCSR000AEH/', MakeExperimentPagesLookTheSameByHidingGraph),
            ('/experiments/ENCSR714GXC/', MakeExperimentPagesLookTheSameByHidingGraph),
            (
                '/search/?type=Experiment&status=in+progress&audit.INTERNAL_ACTION.category=missing+RIN&sort=date_created',
                ClickSearchResultItemAndMakeExperimentPagesLookTheSame
            ),
            (
                '/search/?type=Experiment&status=submitted&sort=date_submitted',
                ClickSearchResultItemAndMakeExperimentPagesLookTheSame
            ),
            ('/search/?type=FunctionalCharacterizationExperiment&control_type!=*&examined_loci.gene.symbol=GATA1', None),
            ('/search/?type=FunctionalCharacterizationSeries&related_datasets.examined_loci.gene.symbol=MYB', None),
            ('/search/?type=TransgenicEnhancerExperiment&tissue_with_enhancer_activity=ear+%28UBERON:0001690%29', None),
            ('/search/?type=File&status=in+progress&derived_from=%2A&quality_metrics=%2A&sort=date_created', ClickSearchResultItem),
            ('/files/ENCFF703RFN/', None),
            ('/files/ENCFF933XVP/', None),
            ('/genetic-modifications/ENCGM320XEE/', None),
            ('/search/?type=GeneticModification&status=in+progress&sort=date_created', ClickSearchResultItem),
            ('/genetic-modifications/ENCGM859RQC/', None),
            ('/human-donors/ENCDO999JZG/', None),
            ('/genetic-modifications/ff643980-ebee-41f8-b937-6a5fd9d673c6/', None),
            ('/genetic-modifications/ENCGM485KFI/', None),
            ('/matrix/?type=Experiment', None),
            ('/matrix/?type=Experiment&internal_tags=ENCORE', None),
            ('/entex-matrix/?type=Experiment&status=released&internal_tags=ENTEx', None),
            ('/mouse-development-matrix/?type=Experiment&status=released&related_series.@type=OrganismDevelopmentSeries&replicates.library.biosample.organism.scientific_name=Mus+musculus', None),
            ('/chip-seq-matrix/?type=Experiment&replicates.library.biosample.donor.organism.scientific_name=Homo%20sapiens&assay_title=Histone%20ChIP-seq&status=released', None),
            ('/pipelines/', None),
            ('/pipelines/ENCPL001DNS/', None),
            ('/pipelines/e02448b1-9706-4e7c-b31b-78c921d58f0b/', None),
            ('/pipelines/ENCPL002LPE/', None),
            ('/pipelines/ENCPL568PWV/', None),
            ('/publication-data/ENCSR727WCB/', None),
            ('/publications/67e606ae-abe7-4510-8ebb-cfa1fefe5cfa/', None),
            ('/search/?type=ReferenceEpigenome&status=in+progress&sort=date_created', ClickSearchResultItem),
            ('/reference-epigenomes/ENCSR191PVZ/', None),
            ('/reference-epigenomes/ENCSR256OEH/', None),
            ('/reference-epigenome-matrix/?type=Experiment&related_series.@type=ReferenceEpigenome&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens', None),
            ('/reference-epigenome-matrix/?type=Experiment&related_series.@type=ReferenceEpigenome&replicates.library.biosample.donor.organism.scientific_name=Mus+musculus', None),
            ('/region-search/?region=chr7%3A0-30000&genome=GRCh38', None),
            ('/report/?type=Experiment&target.label=H3K4me3&target.label=H3K27me3&target.label%21=H3K4me1&award.project%21=ENCODE&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens&replicates.library.biosample.donor.accession%21=ENCDO793LXB&status=released&advancedQuery=%40type%3AExperiment+date_released%3A%5B2015-07-01+TO+2017-10-31%5D', None),
            ('/rnaget?genes=REM1&units=tpm&biosample_organ=kidney', None),
            ('/search/?searchTerm=ENCAB000AEH&type=AntibodyLot', None),
            ('/search/?searchTerm=ENCBS030ENC', None),
            ('/search/?searchTerm=ENCBS808BUA&type=Biosample', None),
            ('/search/?searchTerm=ENCSR000CPG&type=Experiment', None),
            ('/search/?searchTerm="PMID%3A25164756"', None),
            ('/search/?searchTerm=puf60&type=Target', None),
            ('/search/?searchTerm=WASP&type=Software', None),
            ('/search/?type=Biosample&status=deleted&sort=accession', None),
            ('/sescc-stem-cell-matrix/?type=Experiment&internal_tags=SESCC', None),
            ('/search/?type=Target&name=AARS-human', None),
            ('/series-search/', None),
            ('/summary/?type=Experiment&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens', None),
            ('/targets/?status=deleted', None),
            ('/targets/CG15455-dmelanogaster/', None),
            ('/targets/AARS-human/', None),
            ('/treatment-time-series/ENCSR210PYP/', None),
            ('/ucsc-browser-composites/ENCSR707NXZ/', None),
            ('/software/dnase-eval-bam-se/', None),
            ('/search/?type=Software&status=in+progress&sort=date_created', ClickSearchResultItem),
            ('/software/bigwigaverageoverbed/', None)
        ]
        self.check_trackhubs_default_actions = [
            ('/experiments/ENCSR502NRF/',
             OpenUCSCGenomeBrowserGRCh38fromExperiment),
            ('/experiments/ENCSR502NRF/',
             OpenUCSCGenomeBrowserHG19fromExperiment),
            ('/experiments/ENCSR985KAT/',
             OpenUCSCGenomeBrowserHG19fromExperiment),
            ('/experiments/ENCSR426UUG/',
             OpenUCSCGenomeBrowserGRCh38fromExperiment),
            ('/experiments/ENCSR000AXN/',
             OpenUCSCGenomeBrowserMM9fromExperiment),
            ('/experiments/ENCSR335LKF/',
             OpenUCSCGenomeBrowserMM10fromExperiment),
            ('/experiments/ENCSR922ESH/',
             OpenUCSCGenomeBrowserDM3fromExperiment),
            ('/experiments/ENCSR671XAK/',
             OpenUCSCGenomeBrowserDM6fromExperiment),
            ('/experiments/ENCSR422XRE/',
             OpenUCSCGenomeBrowserCE10fromExperiment),
            ('/experiments/ENCSR686FKU/',
             OpenUCSCGenomeBrowserCE11fromExperiment),
            ('/annotations/ENCSR212BHV/',
             OpenUCSCGenomeBrowserHG19fromExperiment),
            ('/experiments/ENCSR000CJR/',
             OpenUCSCGenomeBrowserHG19fromExperiment),
            ('/search/?type=Experiment&assembly=hg19&target.investigated_as=transcription+factor&assay_title=TF+ChIP-seq&replicates.library.biosample.biosample_ontology.classification=primary+cell',
             OpenUCSCGenomeBrowserHG19),
            ('/search/?type=Experiment&assembly=GRCh38&assay_title=shRNA+RNA-seq&target.investigated_as=transcription+factor&advancedQuery=%40type%3AExperiment+date_released%3A%5B2014-10-01+TO+2014-10-31%5D',
             OpenUCSCGenomeBrowserGRCh38),
            ('/search/?type=Experiment&assembly=mm9&assay_title=Repli-chip',
             OpenUCSCGenomeBrowserMM9),
            ('/search/?type=Experiment&assembly=mm10&assay_title=microRNA-seq&advancedQuery=%40type%3AExperiment+date_released%3A%5B2016-01-01+TO+2016-01-31%5D',
             OpenUCSCGenomeBrowserMM10),
            ('/search/?type=Experiment&assembly=dm3&status=released&replicates.library.biosample.biosample_ontology.classification=whole+organisms&assay_title=total+RNA-seq',
             OpenUCSCGenomeBrowserDM3),
            ('/search/?type=Experiment&assembly=dm6&replicates.library.biosample.life_stage=wandering+third+instar+larva',
             OpenUCSCGenomeBrowserDM6),
            ('/search/?type=Experiment&assembly=ce10&target.investigated_as=transcription+factor&replicates.library.biosample.life_stage=L4+larva',
             OpenUCSCGenomeBrowserCE10),
            ('/search/?type=Experiment&assembly=ce11&replicates.library.biosample.life_stage=L3+larva&target.investigated_as=transcription+factor',
             OpenUCSCGenomeBrowserCE11),
            ('/search/?searchTerm=hippocampus&type=Experiment',
             OpenUCSCGenomeBrowserHG19)
        ]
        self.check_permissions_default_actions = [
            ('/experiments/ENCSR524OCB/', None),
            ('/experiments/ENCSR000EFT/', None),
            ('/biosamples/ENCBS643IYW/', None),
            ('/experiments/ENCSR466YGC/', None),
            ('/experiments/ENCSR723PGJ/', None),
            ('/experiments/ENCSR313NYZ/', None),
            ('/files/ENCFF752JWY/', None),
            ('/targets/2L52.1-celegans/', None),
            ('/targets/CG15455-dmelanogaster/', None),
            ('/software/dnase-eval-bam-se/', None),
            ('/software/atac-seq-software-tools/', None),
            ('/software/trimAdapters.py/', None),
            ('/software/bigwigaverageoverbed/', None),
            ('/pipelines/ENCPL493SGC/', None),
            ('/pipelines/ENCPL035XIO/', None),
            ('/pipelines/ENCPL568PWV/', None),
            ('/pipelines/e02448b1-9706-4e7c-b31b-78c921d58f0b/', None),
            ('/pipelines/ENCPL734EDH/', None),
            ('/pipelines/ENCPL983UFZ/', None),
            ('/pipelines/ENCPL631XPY/', None),
            ('/publications/b2e859e6-3ee7-4274-90be-728e0faaa8b9/', None),
            ('/publications/a4db2c6d-d1a3-4e31-b37b-5cc7d6277548/', None),
            ('/publications/16c77add-1bfb-424b-8cab-498ac1e5f6ed/', None),
            ('/publications/da2f7542-3d99-48f6-a95d-9907dd5e2f81/', None),
            ('/internal-data-use-policy/', None),
            ('/tutorials/encode-users-meeting-2016/logistics/', None),
            ('/2017-06-09-release/', None)
        ]
        self.check_downloads_default_actions = [
            ('/experiments/ENCSR810WXH/', DownloadBEDFileFromTable),
            ('/experiments/ENCSR966YYJ/',
             DownloadBEDFileFromModal),
            ('/experiments/ENCSR810WXH/',
             DownloadGraphFromExperimentPage),
            ('/experiments/ENCSR810WXH/',
             DownloadDocuments),
            ('/ucsc-browser-composites/ENCSR707NXZ/',
             DownloadDocuments),
            ('/antibodies/ENCAB749XQY/',
             DownloadDocumentsFromAntibodyPage),
            ('/report/?searchTerm=nose&type=Biosample',
             DownloadTSVFromReportPage),
            ('/search/?type=Experiment&searchTerm=nose',
             DownloadMetaDataFromSearchPage),
            ('/files/ENCFF931OLL/',
             DownloadFileFromFilePage),
            ('/files/ENCFF291ELS/', DownloadFileFromFilePage)
        ]
        # List of tuples:
        # ('suburl', [{payloads}], [(user, expected_status_code)])
        self.check_requests_default_actions = {
            'patch': [
                ('/experiments/ENCSR000CUS/',
                 [{'description': 'test'}],
                 [('Public', 400),
                  (self.view_only_admin, 403)]),
                ('/experiments/ENCSR000CUS/',
                 [{'status': 'deleted'},
                  {'status': 'archived'},
                  {'status': 'proposed'},
                  {'status': 'ready for review'},
                  {'status': 'released'},
                  {'status': 'started'},
                  {'status': 'submitted'},
                  {'status': 'replaced'}],
                 [('admin', 200)]),
                ('/experiments/ENCSR035DLJ/',
                 [{'alternate_accessions': ['ENCSR000CUS']},
                  {'alternate_accessions': []}],
                 [('admin', 200)]),
                ('35f91f16-dcef-4ab2-90bd-3928b0db9a60',
                 [{'status': 'revoked'}],
                 [('admin', 200)]),
                ('/files/ENCFF002BYE/',
                 [{'status': 'deleted'},
                  {'status': 'in progress'},
                  {'status': 'released'},
                  {'status': 'replaced'}],
                 [('admin', 200)]),
                ('4dc1fbd3-6692-42fa-b710-03eaba9263c1',
                 [{'status': 'revoked'}],
                 [('admin', 200)]),
                ('/experiments/ENCSR524OCB/',
                 [{'status': 'submitted'}],
                 self.no_edit_by_lab),
                ('/experiments/ENCSR000EFT/',
                 [{'status': 'revoked'}],
                 self.no_edit_by_lab),
                ('/biosamples/ENCBS643IYW/',
                 [{'status': 'in progress'}],
                 self.edit_by_lab),
                ('/libraries/ENCLB045LXV/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/experiments/ENCSR466YGC/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/experiments/ENCSR255XZG/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/experiments/ENCSR313NYZ/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/files/ENCFF851EYG/',
                 [{'status': 'in progress'}],
                 self.edit_by_lab),
                ('/targets/2L52.1-celegans/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/software/dnase-eval-bam-se/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/software/atac-seq-software-tools/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/software/trimAdapters.py/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/software/bigwigaverageoverbed/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/pipelines/ENCPL493SGC/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/pipelines/ENCPL035XIO/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/pipelines/ENCPL568PWV/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/pipelines/e02448b1-9706-4e7c-b31b-78c921d58f0b/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/pipelines/ENCPL855QIE/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/pipelines/ENCPL983UFZ/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/pipelines/ENCPL631XPY/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/publications/b2e859e6-3ee7-4274-90be-728e0faaa8b9/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/publications/a4db2c6d-d1a3-4e31-b37b-5cc7d6277548/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/publications/16c77add-1bfb-424b-8cab-498ac1e5f6ed/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/publications/da2f7542-3d99-48f6-a95d-9907dd5e2f81/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab),
                ('/internal-data-use-policy/',
                 [{'status': 'in progress'}],
                 self.no_edit_non_object_page),
                ('/tutorials/encode-users-meeting-2016/logistics/',
                 [{'status': 'in progress'}],
                 self.no_edit_non_object_page),
                ('/2017-06-09-release/',
                 [{'status': 'in progress'}],
                 self.no_edit_by_lab)
            ],
            'post': [
                ('/experiments/',
                 [{'description': 'test post experiment',
                   'assay_term_name': 'TF ChIP-seq',
                   'biosample_ontology.term_id': 'CL:0010001',
                   'biosample_ontology.classification': 'primary cell',
                   'biosample_ontology.term_name': 'Stromal cell of bone marrow',
                   'target': '/targets/SMAD6-human/',
                   'award': '/awards/U41HG006992/',
                   'lab': '/labs/thomas-gingeras/',
                   'references': ['PMID:18229687', 'PMID:25677182']}],
                 [('Public', 400),
                  (self.lab_submitter, 201),
                  ('admin', 201)])
            ],
            'get': [
                ('/experiments/ENCSR082IHY/',
                 [None],
                 [('Public', 403),
                  (self.lab_submitter, 403),
                  ('admin', 200)]),
                ('/experiments/ENCSR000CUS',
                 [None],
                 [('Public', 200),
                  (self.lab_submitter, 200),
                  ('admin', 200)]),
                ('/experiments/ENCSR524OCB/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/experiments/ENCSR000EFT/',
                 [None],
                 self._released_expected_response),
                ('/biosamples/ENCBS643IYW/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/libraries/ENCLB045LXV/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/experiments/ENCSR466YGC/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/experiments/ENCSR723PGJ/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/experiments/ENCSR313NYZ/',
                 [None],
                 self._unreleased_other_expected_response),
                ('/experiments/ENCSR255XZG/',
                 [None],
                 self._released_expected_response),
                ('/experiments/ENCSR115BCB/',
                 [None],
                 self._released_expected_response),
                ('/files/ENCFF851EYG/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/targets/2L52.1-celegans/',
                 [None],
                 self._released_expected_response),
                ('/software/dnase-eval-bam-se/',
                 [None],
                 self._unreleased_other_expected_response),
                ('/software/atac-seq-software-tools/',
                 [None],
                 self._unreleased_other_expected_response),
                ('/software/trimAdapters.py/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/software/bigwigaverageoverbed/',
                 [None],
                 self._released_expected_response),
                ('/pipelines/ENCPL493SGC/',
                 [None],
                 self._released_expected_response),
                ('/pipelines/ENCPL035XIO/',
                 [None],
                 self._released_expected_response),
                ('/pipelines/ENCPL568PWV/',
                 [None],
                 self._unreleased_other_expected_response),
                ('/pipelines/e02448b1-9706-4e7c-b31b-78c921d58f0b/',
                 [None],
                 self._unreleased_other_expected_response),
                ('/pipelines/ENCPL855QIE/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/pipelines/ENCPL983UFZ/',
                 [None],
                 self._unreleased_other_expected_response),
                ('/pipelines/ENCPL631XPY/',
                 [None],
                 self._released_expected_response),
                ('/publications/b2e859e6-3ee7-4274-90be-728e0faaa8b9/',
                 [None],
                 self._released_expected_response),
                ('/publications/a4db2c6d-d1a3-4e31-b37b-5cc7d6277548/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/publications/16c77add-1bfb-424b-8cab-498ac1e5f6ed/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/publications/da2f7542-3d99-48f6-a95d-9907dd5e2f81/',
                 [None],
                 self._unreleased_other_expected_response),
                ('/internal-data-use-policy/',
                 [None],
                 self._unreleased_internal_expected_response),
                ('/tutorials/encode-users-meeting-2016/logistics/',
                 [None],
                 self._unreleased_other_expected_response),
                ('/2017-06-09-release/',
                 [None],
                 self._released_expected_response)
            ]
        }
        self.check_tools_default_actions = [
            {'name': 'ENCODE_get_fields.py',
             'command': '{} {} {}  --infile ENCSR000CUS --field status',
             'expected_output': 'accession\tstatus\r\nENCSR000CUS\trevoked\r\n'},
            {'name': 'ENCODE_patch_set.py',
             'command': '{} {} {} --accession ENCSR000CUS --field status --data revoked',
             'expected_output': 'OBJECT: ENCSR000CUS\nOLD DATA: status'
             ' revoked\nNEW DATA: status revoked'},
            {'name': 'ENCODE_release.py',
             'command': '{} {} {} --infile ENCSR000CUS',
             'expected_output': 'Data written to file Release_report.txt'},
            {'name': 'ENCODE_submit_files.py',
             'command': '{} {} permissions_qa_scripts/Test_submit_files.csv {}',
             'expected_output': "'file_size': 23972104"}
        ]
