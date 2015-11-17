# pyencoded-tools


## ENCODE_get_fields.py

BASIC REPORTER SCRIPT

ENCODE_get_fields.py can take in a file with a single column list of object identifiers (accessions, uuids, alises),
or an ENCODE query that will point to the list of objects you want
For the fieldnames either a file with a single column list of field names you want or use the "--onefield" and give a single fieldname


To get multiple fields use the multifield argument:

        $ python3 ENCODE_get_fields.py --infile filename --multifield fieldnames

    where the infile is a list of object identifiers
    and the multifield is a list of fields desired

To get a single field use the onefield argument:

        $ python3 ENCODE_get_fields.py --infile filename --onefield field

    where onefield is a string containing the field name

To use a custom query for your object list:

        $ python3 ENCODE_get_fields.py --query www.my/custom/url

    this can be used with multifield or onefield


## ENCODE_patchSet.py

Read in a file of object, correction fields and patch each object


Input file should be a TSV (tab separated value) file with headers
if the field value is a non-string value, list its type separated by a colon

accession   header1  header2:list  header3:int ...
ENCSR000AAA value1   list1,list2   value3  ...

Whatever data is used to identify the object (accession, uuid, alias)
goes in the accession column to be used for identification of object

To PATCH a single object, field with field type, and data:

        $ python3 ENCODE_patchSet.py --accession ENCSR000AAA --field assay_term_name --data ChIP-seq
        $ python3 ENCODE_patchSet.py --accession ENCSR000AAA --field read_length:int --data 31
        $ python3 ENCODE_patchSet.py --accession ENCSR000AAA --field documents:list --data document1,document2

    for integers use ':int'
    for lists use    ':list'
    string are the default and do not require an identifier
