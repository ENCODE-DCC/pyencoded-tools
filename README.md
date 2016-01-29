# pyencoded-tools


### ENCODE_get_fields.py

To get multiple objects use the '--object' argument
and provide a file with the list of object identifiers

        ENCODE_get_fields.py --object filenames.txt
this can take accessions, uuids, @ids, or aliases

To get a single object use the '--object' argument
and use the object's identifier

        ENCODE_get_fields.py --object ENCSR000AAA
        ENCODE_get_fields.py --object 3e6-some-uuid-here-e45
        ENCODE_get_fields.py --object this-is:an-alias

To get multiple fields use the '--field' argument
and feed it a file with the list of fieldnames

        ENCODE_get_fields.py --field fieldnames.txt
this should be a single column file

To get a single field use the field argument:

        ENCODE_get_fields.py --field status
where field is a string containing the field name

To get embedded field values (such as target name from an experiment):

**Note: ENCODE_get_fields will only expand the data untill it hits an array**
**currently it cannot get subarrays of arrays**

        ENCODE_get_fields.py --field target.title
    
    accession       target.title
    ENCSR087PLZ     H3K9ac (Mus musculus)
this can also get embedded values from lists

        ENCODE_get_fields.py --field files.status
*more about this feature is listed below*

To use a custom query for your object list:

        ENCODE_get_fields.py --query www.my/custom/url
this can be used with either useage of the '--field' option


Output prints in format of fieldname:object_type for non-strings

    Ex: accession    read_length:int    documents:list
        ENCSR000AAA  31                 [document1,document2]

    integers  ':int'
    lists     ':list'
    string are the default and do not have an identifier
*please note that list type fields will show only unique items*

        ENCODE_get_fields.py --field files.status --object ENCSR000AAA

    accession       file.status:list
    ENCSR000AAA     ['released']
this is a possible output even if multiple files exist in experiment

To show all possible outputs from a list type field
use the '--listfull' argument

        ENCODE_get_fields.py --field files.status --listfull

    accession       file.status:list
    ENCSR000AAA     ['released', 'released', 'released']


**ENCODE_collection useage and functionality:**

ENCODE_get_fields.py has ported over some functions of ENCODE_collection
and now supports the '--collection' and '--allfields' options

Useage for '--allfields':

        ENCODE_get_fields.py --object ENCSR000AAA --allfields

    accession    status    files        award ...
    ENCSR000AAA  released  [/files/...] /awards/...

The '--allfields' option can be used with any of the commands,
it returns all fields at the frame=object level,
it also overrides any other --field option

Useage for '--collection':

        ENCODE_get_fields.py --collection Experiment --status

    accession    status
    ENCSR000AAA  released

The  '--collection' option can be used with or without the '--es' option
the '--es' option allows the script to search using elastic search,
which is slightly faster than the normal table view used

However, it may not posses the latest updates to the data and may not be
preferable to your application
'--collection' also overrides any other '--object' option and so but it
can be combined with any of the '--field' or '--allfields' options

**NOTE:** while '--collection' should work with the '--field' field.embeddedfield
functionality I cannot guarantee speed when running due to embedded
objects being extracted


### ENCODE_patchSet.py

**_PLEASE NOTE:_** This script is a dryrun-default script, run it with the *--update* option to make any changes

**Input options:**
Input file should be a TSV (tab separated value) file with headers
if the field value is a non-string value, list its type separated by a colon

       accession   header1  header2:list  header3:int ...
       ENCSR000AAA value1   list1,list2   value3  ...

Whatever data is used to identify the object (accession, uuid, alias)
goes in the accession column to be used for identification of object

**Input file format:**
To PATCH a single object, field with field type, and data:

        $ python3 ENCODE_patchSet.py --accession ENCSR000AAA --field assay_term_name --data ChIP-seq
        $ python3 ENCODE_patchSet.py --accession ENCSR000AAA --field read_length:int --data 31
        $ python3 ENCODE_patchSet.py --accession ENCSR000AAA --field documents:list --data document1,document2

* For integers use ':int' or ':integer'
* For lists use    ':list' or ':array'
* Lists are appended to unless the *--overwrite* option is used
* *String are the default and do not require an identifier*

**Removing data:**
Data can be removed with the *--remove* option.  This must be run with the *--update* command to make the changes.

Note that the remove option FULLY removes the ENTIRE item.  If you have a list and you want to remove only certian list items you will need to feed in the list with the object removed and use the *--overwrite* option

