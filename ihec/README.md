# Scripts for submitting reference epigenomes to IHEC

In general, a submission to IHEC contains two parts: EpiRR and IHEC data hub. For us, ENCODE, one prerequisite (step 0) is to make sure all our reference epigenomes are up to date on the production portal.
Both EpiRR and IHEC data hub submissions can be validated with ihec-ecosystems (https://github.com/IHEC/ihec-ecosystems). As for now (11-11-2019), it's worth to keep in mind two things: 

1. two validations are not strictly related, i.e. metadata from validated EpiRR submission may not pass IHEC data hub submission. Therefore, it might be better to adjust all scripts, reconcile all conflicts/errors and validate both EpiRR submission and IHEC data hub submissions before submitting any of them.
2. Both validations should be run within ihec-ecosystems code base. Those scripts and code are not yet packaged thus cannot be called properly from outside.

### EpiRR submission

* Scripts: `epiRR.py`. This script needs to be run under version_metadata folder of ihec-ecosystems repository because it will use ihec-ecosystems to validate outputs. An "ENCODE" folder need to be created under version_metadata folder if it's not there before running this script.

* Basic commands:

  - `./epiRR.py --one ENCSR191PVZ ENCSR867OGI`

    Create EpiRR submission(s) for one or more ENCODE reference epigenome(s) specified by accession(s).

  - `./epiRR.py --all`

    Create EpiRR submissions for all released ENCODE reference epigenomes.

* Expected outputs:

  This script will validate generated EpiRR submission using ihec-ecosystems. For every ENCODE reference epigenome, there will be five files:

  1. ENCSR191PVZ.refepi.json
  2. ENCSR191PVZ_experiment.xml
  3. ENCSR191PVZ_samples.xml
  4. ENCSR191PVZ_experiment.validated.xml
  5. ENCSR191PVZ_samples.validated.xml

  The last two are created by ihec-ecosystems validation code and will show up once an EpiRR reference epigenome submission passes validation.

* How to submit?

  Package all outputs for all reference epigenomes to be submitted into on tarball and email it to epirr@ebi.ac.uk. A ticket will be created in their system this way.

### IHEC data hub submission

* Scripts:

  - `ihec_data_hub.py`

    It is not required but it's better to run this script under IHEC_Data_Hub folder of ihec-ecosystems repository because the next validation script needs outputs to be in the same folder and will use ihec-ecosystems to validate outputs.

    __*It's worth noting*__ that this script heavily depends on the `batch_hub` end point of the portal and doesn't do too much customization on its own. Therefore, any changes in encoded `batch_hub` end point could affect this script and any changes needed for IHEC data hub submission should be address in encoded `batch_hub` end point.

  - `ENCODE_IHEC_data_hub_valiation.sh`

    This script will assume there are three JSON submissions named as `ENCODE_IHEC_Data_Hub_hg19.json`, `ENCODE_IHEC_Data_Hub_hg38.json`, `ENCODE_IHEC_Data_Hub_mm10.json`. Such naming is not required. Feel free to run the command within the script separately as you like. Just remember to use the options as exampled in the script.

* Basic commands:

  - `./ihec_data_hub.py --one ENCSR191PVZ ENCSR867OGI`

    Create one set of submissions, one JSON per assembly (hg19, hg38, mm10), for one or more ENCODE reference epigenome(s) specified by accession(s).

  - `./ihec_data_hub.py --all`

    Create one set of submissions, one JSON per assembly (hg19, hg38, mm10), for all released ENCODE reference epigenomes.

  - `ENCODE_IHEC_data_hub_valiation.sh &> valiation.log`: validate three IHEC data submissions named as `ENCODE_IHEC_Data_Hub_hg19.json`, `ENCODE_IHEC_Data_Hub_hg38.json`, `ENCODE_IHEC_Data_Hub_mm10.json`.

* Expected outputs:
  IHEC data portal required us to combine tracks for all our reference epigenomes as one hub per assembly. The exact output depends on what reference epigenomes you'd like to submit. Generating submissions for all released ENCODE reference epigenomes will create three JSON files:

  - `ENCODE_IHEC_Data_Hub_hg19.json`
  - `ENCODE_IHEC_Data_Hub_hg38.json`
  - `ENCODE_IHEC_Data_Hub_mm10.json`.

* How to submit?

  Email JSON hub(s) to David Bujold, Mr at david.bujold@mcgill.ca and david.brownlee@computationalgenomics.ca. Those JSON files could be too big for email attachments. Use Google Drive is OK with David.
