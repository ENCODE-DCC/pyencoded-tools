# pyencoded-tools


### ENCODE_get_fields.py

**Input options:**
* *--infile* infile with format defined below (ex: *--infile myfile.txt*)
* *--query* url query that points to the objects you want to view (ex: *--query /search/?type=experiment&assay_term_name=ChIP-seq&assembly=mm10*)
* *--accession* single object accession (ex: *--accession ENCSR000AAA*)

**Infile format:**
* Single column of object accessions, uuids, aliases, or other unique identifier

**Field options:**
* *--onefield* single fieldname (ex: *--onefield read_length*)
* *--multifield* file with single column fieldnames (ex: *--multifield myfields.txt*)


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

* For integers use ':int'
* For lists use    ':list'
* *String are the default and do not require an identifier*

**Removing data:**
Data can be removed with the *--remove* option.  This must be run with the *--update* command to make the changes.