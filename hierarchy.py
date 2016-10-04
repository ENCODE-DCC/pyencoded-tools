
level_6 = [
    'PublicationData',
    'UcscBrowserComposite',
    'ReferenceEpigenome',
    'MatchedSet',
    'TreatmentTimeSeries',
    'ReplicationTimingSeries',
    'OrganismDevelopmentSeries',
    'TreatmentConcentrationSeries',
    'Annotation',
    'Project']

level_5 = [
    'Experiment']

level_4 = [
    'Replicate']

level_3 = [
    'Library']

level_2 = [
    'AntibodyLot',
    'Biosample',
    'Software',
    'File',
    'Treatment',
    'TALEN',
    'RNAi',
    'Construct',
    'Target',
    'Document',
    'Publication',
    'Reference',
    'GeneticModification',
    'Crispr',
    'Tale',
    'SoftwareVersion',
    'DonorCharacterization',
    'ConstructCharacterization',
    'RNAiCharacterization',
    'BiosampleCharacterization',
    'StarQualityMetric',
    'MadQualityMetric',
    'HotspotQualityMetric',
    'GenericQualityMetric',
    'FastqcQualityMetric',
    'SamtoolsFlagstatsQualityMetric',
    'DnasePeakQualityMetric',
    'Encode2ChipSeqQualityMetric',
    'SamtoolsStatsQualityMetric',
    'EdwcomparepeaksQualityMetric',
    'PbcQualityMetric',
    'IDRQualityMetric',
    'EdwbamstatsQualityMetric',
    'ChipSeqFilterQualityMetric',
    'BigwigcorrelateQualityMetric',
    'CpgCorrelationQualityMetric',
    'PhantompeaktoolsSppQualityMetric',
    'IdrSummaryQualityMetric',
    'BismarkQualityMetric',
    'MouseDonor',
    'WormDonor',
    'FlyDonor',
    'HumanDonor',
    'Organism',
    'Lab',
    'Award',
    'Platform',
    'Source',
    'Image']

levels_mapping = {}
for entry in level_2:
    levels_mapping[entry] = 'level_2'
for entry in level_3:
    levels_mapping[entry] = 'level_3'
for entry in level_4:
    levels_mapping[entry] = 'level_4'
for entry in level_5:
    levels_mapping[entry] = 'level_5'
for entry in level_6:
    levels_mapping[entry] = 'level_6'

dictionary_of_lower_levels = {
    'level_6':
    set(level_2) |
    set(level_3) |
    set(level_4) |
    set(level_5),
    'level_5':
    set(level_2) |
    set(level_3) |
    set(level_4),
    'level_4':
    set(level_2) |
    set(level_3),
    'level_3':
    set(level_2),
    'level_2':
    set(level_2)}
