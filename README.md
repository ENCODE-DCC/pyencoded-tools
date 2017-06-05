# pyencoded-tools

### ENCODE_duplicates_catch.py
This script allows detection of non-revoked files on the portal having identical content md5sum. The script will run without any parameters, except the usual --keyfile and --key arguments. Execution of the script will result in a 'tsv' report on detected duplicates. The report includes individual sections fro each lab: 

        LAB with DUPLICATES : <name of the lab>
        NUM of replicate cases : <num of duplications detected>
        <file accession>	<experiment accession>	<content md5sum>	<file status>

### ENCODE_get_fields.py
This script takes object identifiers and fieldnames and returns a TSV of the data gathered  
To get multiple objects use the *--infile* argument and provide a file with the list of object identifiers

        ./ENCODE_get_fields.py --infile filenames.txt
this can take accessions, uuids, @ids, or aliases

To get a single object use the *--infile* argument and use the object's identifier, also will take a comma separated list

        ./ENCODE_get_fields.py --infile ENCSR000AAA
        ./ENCODE_get_fields.py --infile 3e6-some-uuid-here-e45
        ./ENCODE_get_fields.py --infile this-is:an-alias
        ./ENCODE_get_fields.py --infile ENCSR000AAA,ENCSR000AAB

To get multiple fields use the *--field* argument and feed it a file with the list of fieldnames

        ./ENCODE_get_fields.py --field fieldnames.txt
this should be a single column file

To get a single field use the field argument:

        ./ENCODE_get_fields.py --field status
        ./ENCODE_get_fields.py --field status,target.title
where field is a string containing the field name or a comma separated list of fieldnames, this can be combined with the embedded values

To get embedded field values (such as target name from an experiment):  
**Note: ENCODE_get_fields will only expand the data untill it hits an array**
**currently it cannot get subarrays of arrays**

        ./ENCODE_get_fields.py --field target.title
    
    accession       target.title
    ENCSR087PLZ     H3K9ac (Mus musculus)
this can also get embedded values from lists

        ./ENCODE_get_fields.py --field files.status
*more about this feature is listed below*

To use a custom query for your object list:

        ./ENCODE_get_fields.py --query www.my/custom/url
this can be used with either useage of the *--field* option


Output prints in format of fieldname:object_type for non-strings

    Ex: accession    read_length:int    documents:list
        ENCSR000AAA  31                 [document1,document2]

    integers  ':int'
    lists     ':list'
    string are the default and do not have an identifier
*please note that list type fields will show only unique items*

        ./ENCODE_get_fields.py --field files.status --infile ENCSR000AAA

    accession       file.status:list
    ENCSR000AAA     ['released']
this is a possible output even if multiple files exist in experiment

To show all possible outputs from a list type field use the *--listfull* argument

        ./ENCODE_get_fields.py --field files.status --listfull

    accession       file.status:list
    ENCSR000AAA     ['released', 'released', 'released']


**ENCODE_collection useage and functionality:**

ENCODE_get_fields.py has ported over some functions of ENCODE_collection and now supports the *--collection* and *--allfields* options

Useage for *--allfields*:

        ./ENCODE_get_fields.py --infile ENCSR000AAA --allfields

    accession    status    files        award ...
    ENCSR000AAA  released  [/files/...] /awards/...

The *--allfields* option can be used with any of the commands,  
it returns all fields at the frame=object level,  
it also overrides any other *--field* option

Useage for *--collection*:

        ./ENCODE_get_fields.py --collection Experiment --status

    accession    status
    ENCSR000AAA  released

The  *--collection* option can be used with or without the *--es* option  
the *--es* option allows the script to search using elastic search, which is slightly faster than the normal table view used  
However, it may not posses the latest updates to the data and may not be preferable to your application  
*--collection* also overrides any other *--infile* option and so but it can be combined with any of the *--field* or *--allfields* options

**NOTE:** while *--collection* should work with the *--field* field.embeddedfield functionality I cannot guarantee speed when running due to embedded objects being extracted


### ENCODE_patch_set.py
Given a TSV file this script will PATCH data to the ENCODE database  
**_PLEASE NOTE:_** This script is a dryrun-default script, run it with the *--update* option to make any changes

**Input options:**
Input file should be a TSV (tab separated value) file with headers
if the field value is a non-string value, list its type separated by a colon

       accession   header1  header2:list  header3:int ...
       ENCSR000AAA value1   list1,list2   value3  ...

Whatever data is used to identify the object (accession, uuid, alias) goes in the accession column to be used for identification of object

**Input file format:**
To PATCH a single object, field with field type, and data:

        ./ENCODE_patch_set.py --accession ENCSR000AAA --field assay_term_name --data ChIP-seq
        ./ENCODE_patch_set.py --accession ENCSR000AAA --field read_length:int --data 31
        ./ENCODE_patch_set.py --accession ENCSR000AAA --field documents:list --data document1,document2

* For integers use ':int' or ':integer'
* For lists use    ':list' or ':array'
* Lists are appended to unless the *--overwrite* option is used
* *String are the default and do not require an identifier*


To PATCH flowcells:

        ./ENCODE_patch_set.py --flowcell

the *--flowcell* option is a flag used to have the script search for flowcell data in the infile  
Example infile:  

    accession   flowcell   lane    barcode   machine
    ENCSR000AAA value1     value2  value3    value4

not all the columns are needed for the flowcell to be built

**Removing data:**  
Data can be removed with the *--remove* option.  
This must be run with the *--update* command to make the changes.  
To remove items from a list you must also tag the column header as such, otherwise it will remove the entire item

       accession    subobject:list
       ENCSR000AAA  item1,item2
This removes "item1" and "item2" from the list, you need to include the FULL NAME of the object you want to remove (i.e. “/files/ENCFF000ABD/“)

       accession     subobject
       ENCSR000AAA   item1,item2
This will remove the "subobject" from the object completely

### ENCODE_release.py

**_PLEASE NOTE:_** This script is a dryrun-default script, run it with the *--update* option to make any changes

ENCODE_release.py is a script that will release objects fed to it  
Default settings only report the status of releaseable objects and will NOT release unless instructed  
In addition if an object fails to pass the Error or Not Compliant audits it will not be released

**Basic Useage:**

    ./ENCODE_release.py --infile file.txt
    ./ENCODE_release.py --infile ENCSR000AAA
    ./ENCODE_release.py --infile ENCSR000AAA,ENCSR000AAB,ENCSR000AAC

A single column file listing the  identifiers of the objects desired  
A single identifier or comma separated list of identifiers is also useable

    ./ENCODE_release.py --query "/search/?type=Experiment&status=release+ready"

A query may be fed to the script to use for the object list

    ./ENCODE_release.py --infile file.txt --update

*--update* should be used whenever you want to PATCH the changes to the database, otherwise the script will stop before PATCHing

    ./ENCODE_release.py --infile file.txt --force --update

if an object does not pass the 'Error' or 'Not Compliant' audit it can still be released with the *--force* option  
MUST BE RUN WITH *--update* TO WORK

    ./ENCODE_release.py --infile file.txt --logall

Default script will not log status of 'released' objects, using *--logall* will make it include the statuses of released items in the report file

**Misc. Useage:**

The output file default is 'Release_report.txt'
 * This can be changed with *--output*

Default keyfile location is '~/keyfile.json'
 * Change with *--keyfile*

Default key is 'default'
 * Change with *--key*

Default debug is off
 * Change with *--debug*


### ENCODE_error_summary.py

This script uses the matrix view available at "https://www.encodeproject.org/matrix/?type=Experiment" to find and total the Error and Not Compliant audits  
This script outputs a TSV file that has been formatted so that when it is opened in Google Sheets each cell with results will also be a link to the search page used to generate that cell data

**You must use Google Sheets to open the resulting file**  
Excel is unable to handle the formulas

This script will print out the following during it's run: "WARNING:root:No results found"  
This is due to how the long and short RNA-seq are searched and it does not affect the final results of the script

All commands need to be quote enclosed

*--rfa* command uses the award.rfa property to refine inital matrix

    ./ENCODE_error_summary.py --rfa "ENCODE,Roadmap"

*--species* command uses the organism.name property to refine the inital matrix

    ./ENCODE_error_summary.py --species "celegans,human,mouse"

*--lab* command uses the lab.title property to refine inital matrix

    ./ENCODE_error_summary.py --lab "bing-ren,j-micheal-cherry"

*--status* uses the status property to refine inital matrix

    ./ENCODE_error_summary.py --status "released,submitted"

The usual list of assay this script shows is:  
* Short RNA-seq
* Long RNA-seq
* microRNA profiling by array assay
* microRNA-seq DNase-seq
* whole-genome shotgun bisulfite sequencing
* RAMPAGE
* CAGE

use the *--all* command to select all the available assays for display  
the output file can be renamed using the *--outfile* option  
the *--allaudits* command will also list the "WARNING" and "DCC ACTION" audits


### ENCODE_publications.py  
**_PLEASE NOTE:_** This script is a dryrun-default script, run it with the *--update* option to make any changes

Takes in a VERY specific file format to use for updating the publications  
Also can update the existing publications using the pubmed database

An EMAIL is required to run this script  
This is for the Entrez database

This script requires the BioPython module

Options:

    ./ENCODE_publications.py --consortium Consortium_file.txt

This takes the consortium file

    ./ENCODE_publications.py --community Community_file.txt

This takes the community file

    ./ENCODE_publications.py --updateonly list.txt

Takes file with single column of publication UUIDs, checks against PubMed to ensure data is correct and will update if needed


### ENCODE_read_lengths.py

This script opens a fastq and calculates the read length, it can also print the header line of the fastq currently unable to parse header for information such as machine name

    ./ENCODE_read_lengths.py --infile file.txt
    ./ENCODE_read_lengths.py --infile ENCFF000AAA
    ./ENCODE_read_lengths.py --infile ENCFF000AAA,ENCFF000AAB,ENCFF000AAC

Takes either a list of the file accessions, a single accession, or comma separated list of accessions

    ./ENCODE_read_lengths.py --query "/search/?type=File"

Takes a query from which to get the list of files

    ./ENCODE_read_lengths.py --header

Prints the header line from the fastq


### ENCODE_submit_files.py

**_PLEASE NOTE:_** This script is a dryrun-default script, run it with the *--update* option to make any changes

Provide with a CSV file of metadata to post

    ./ENCODE_submit_files.py --encvaldata ./encValData

Use to define a different location for the encValData directory

    ./ENCODE_submit_files.py --validatefiles ./validateFiles

use to define a different location for the validateFiles script  
validateFiles must be made executable for this to work


### ENCODE_antibody_approver.py
**_PLEASE NOTE:_** This script is a dryrun-default script, run it with the *--update* option to make any changes

Given a TSV file this script will attempt to add in the information to the antibodies, the file is provided by the user

Example TSV file:

        @id     lanes   lane_status     notes   documents
        someID  2,3     compliant       get it? important_document.pdf
        someID  1,4     not compliant   got it  important_document.pdf
        someID  5       pending dcc review      good    important_document.pdf

Useage:

    ./ENCODE_antibody_approver.py --infile MyFile.txt --user 4eg4-some-uuid-ks87
    ./ENCODE_antibody_approver.py --infile MyFile.txt --user /users/some-user

Either a uuid or an @id can be used for user identification


### ENCODE_controlled_by_backfill.py
**_PLEASE NOTE:_** This script is a dryrun-default script, run it with the *--update* option to make any changes

Script to match files with their controlled by value.

Usage:

    ENCODE_controlled_by_backfill.py --infile MyFile.txt
    ENCODE_controlled_by_backfill.py --infile ENCSR000AAA
    ENCODE_controlled_by_backfill.py --infile ENCSR000AAA,ENCSR000AAB,ENCSR000AAC
    ENCODE_controlled_by_backfill.py --query "/search/?type=Experiment"

Script will take a file with single column list of experiment accessions  
Can also take a single experiment accession or comma separated list of experiment accessions  
A query from which to gather experiment accessions  

    ENCODE_controlled_by_backfill.py --method single
    ENCODE_controlled_by_backfill.py --method multi
    ENCODE_controlled_by_backfill.py --method biosample

There are three methods to pick from  
"single" assumes one replicate in the control  
"multi" assumes one control with its number of replicates equal to number of replicates in experiment  
"biosample" assumes multiple controls that should be matched on biosample

***By NOT selecting the '--method' option the script will try to guess the correct method***

    ENCODE_controlled_by_backfill.py --ignore_runtype

This makes the script ignore the value of the paired ends, default is off

    ENCODE_controlled_by_backfill.py --missing

Script will print out only the names of files missing controlled_by

### examples/3777-gravely-protocol.py

This script takes in a text file containing gravely documents for libraries and biosamples, parses it, downloads the PDFs from Gravely links, uploads to the portal and links the uploaded document to the relevant library or biosample

Example usage for libraries:

    python3 examples/3777-gravely-protocol.py --keyfile keypairs.json --key test --lib library_protocols.txt

Example usage for biosamples: 

    python3 examples/3777-gravely-protocol.py --keyfile keypairs.json --key test --bio biosample_protocols.txt
    

### ENCODE_import_data.py
**_PLEASE NOTE:_** This script is a dryrun-default script, run it with the *--update* option to make any changes  
This script takes in an Excel file with the data

By DEFAULT:  
If there is a uuid, alias, @id, or accession in the document it will ask if you want to PATCH that object  
Use '--patchall' if you want to patch ALL objects in your document and ignore that message

If no object identifiers are found in the document you need to use '--update' for POSTing to occur

Defining Object type:  
Name each "sheet" of the excel file the name of the object type you are using with the format used on https://www.encodeproject.org/profiles/  
Ex: Experiment, Biosample, Document, AntibodyCharacterization

Or use the '--type' argument, but this will only work for single sheet documents  

    ENCODE_import_data.py mydata.xlsx --type Experiment

The header should be the names of the fields just as in ENCODE_patch_set.py,  
Ex: award, lab, target, etc.

    For integers use ':int' or ':integer'
    For lists use ':list' or ':array'
    String are the default and do not require an identifier

To upload objects with attachments, have a column titled "attachment" containing the name of the file you wish to attach

FOR EMBEDDED SUBOBJECTS:  
Embedded objects are considered to be things like:
 - characterization_reviews in AntibodyCharacterizations  
 - tags in Constructs  
 They are assumed to be of the format of a list of dictionary objects  
 Ex:  
```
"characterization_reviews": [
        {
            "organism": "/organisms/human/",
            "biosample_term_name": "A375",
            "lane": 2,
            "biosample_type": "immortalized cell line",
            "biosample_term_id": "EFO:0002103",
            "lane_status": "compliant"
        },
        {
            "organism": "/organisms/mouse/",
            "biosample_term_name": "embryonic fibroblast",
            "lane": 3,
            "biosample_type": "primary cell",
            "biosample_term_id": "CL:2000042",
            "lane_status": "compliant"
        }
    ]
```
Formatting in the document should be as follows for the above example:  

    characterization_reviews.organism    characterization_reviews.lane:int    ....    characterization_reviews.organism-1    characterization_reviews.lane-1:int
    /organisms/human/                    2                                            /organisms/mouse/                      3

A second example:

    tags.name    tags.location    tags.name-1    tags.location-1
    FLAG         C-terminal       BOGUS          Fake-data

Again, this will become
```
"tags": [
        {
            "location": "C-terminal",
            "name": "FLAG"
        },
        {
            "location": "Fake-data",
            "name": "BOGUS"
        }
    ]
```
