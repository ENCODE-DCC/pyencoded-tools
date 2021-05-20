#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, argparse, os, time
import xml.etree.ElementTree as ET
from xml.dom import minidom as md
from datetime import datetime
import pandas as pd


def get_parser():
    parser = argparse.ArgumentParser(description="Generate an XML for DOI submission from experiment metadata")
    parser.add_argument("-i", "--infile", required=True, help="""Tab delimited file with appropriate metadata""", action="store")
    parser.add_argument("-o", "--outfile", required=True, help="""Output XML file in CrossRef Schema""", action="store")
    parser.add_argument("-p", "--patchfile", required=True, help="""Output tsv file to patch datsets with""", action="store")
    parser.add_argument("-d", "--dataset", default="experiments", choices={"annotations", "experiments", "functional-characterization-experiments", "transgenic-enhancer-experiments"}, help="""What type of dataset to create DOIs for""")
    return parser


def writeToFile(x, out_file):
    pretty_print = lambda f: '\n'.join([line for line in md.parseString(f).toprettyxml().split('\n') if line.strip()])
    roughString = ET.tostring(x)
    xmlFromString = ET.fromstring(pretty_print(roughString))
    tRoot = xmlFromString.find(".")
    def getChildren(elem):
        elems = elem.findall('*')
        if len(elems) > 0:  #if any children
            for x in elems:
                getChildren(x)
        else:  #if no children
            if len(elem.text) > 0:
                try:
                    while elem.text[0] == "\t" or elem.text[0] == " " or elem.text[0] == "\n":
                        elem.text = elem.text[1:]
                    while elem.text[-1] == "\t" or elem.text[-1] == " " or elem.text[-1] == "\n":
                        elem.text = elem.text[:-1]
                except:
                    pass
    getChildren(tRoot)
    ET.register_namespace("","http://www.crossref.org/schema/4.4.2")
    newTree = ET.ElementTree(xmlFromString)
    newTree.write(out_file,encoding="UTF-8")


def main():
    parser = get_parser()
    args = parser.parse_args()
    inFile = args.infile
    outFile = args.outfile
    exptType = args.dataset
    patchFile = args.patchfile

    doi_batch_Elem = ET.Element("doi_batch", {
                    "version":"4.4.2",
                    "xmlns":"http://www.crossref.org/schema/4.4.2",
                    "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance",
                    "xsi:schemaLocation":"http://www.crossref.org/schema/4.4.2 http://www.crossref.org/schema/deposit/crossref4.4.2.xsd"})

    #ALL DEFAULT HEAD INFO. NOT DERIVED, BUT SET
    head_Elem = ET.SubElement(doi_batch_Elem, "head")
    doiBatchId_Elem = ET.SubElement(head_Elem, "doi_batch_id")
    doiBatchId_Elem.text = "ENCODE Submission - " + str(datetime.now())
    timestamp_Elem = ET.SubElement(head_Elem, "timestamp")
    timestamp_Elem.text = time.strftime("%Y%m%d%H%M%S")
    depositor_Elem = ET.SubElement(head_Elem, "depositor")
    name_Elem = ET.SubElement(depositor_Elem,"depositor_name")
    name_Elem.text = "Bonita Lam"
    email_Elem = ET.SubElement(depositor_Elem,"email_address")
    email_Elem.text = "brlam@stanford.edu"
    registrant_Elem = ET.SubElement(head_Elem, "registrant")
    registrant_Elem.text = "Stanford University, ENCODE Data Coordination Center"

    body_Elem = ET.SubElement(doi_batch_Elem, "body")
    database_Elem = ET.SubElement(body_Elem,"database")
    databaseMetadata_Elem = ET.SubElement(database_Elem,"database_metadata",{"language":"en"})
    titles_Elem = ET.SubElement(databaseMetadata_Elem, "titles")
    title_Elem = ET.SubElement(titles_Elem, "title")
    title_Elem.text = "ENCODE Datasets"
    description_Elem = ET.SubElement(databaseMetadata_Elem,"description")
    description_Elem.text = "The repository collection of genomics data from the Encyclopedia of DNA Elements (ENCODE) project hosted and maintained by the ENCODE Data Coordination Center based in the Cherry Lab at Stanford University."
    institution_Elem = ET.SubElement(databaseMetadata_Elem, "institution")
    institutionName_Elem = ET.SubElement(institution_Elem,"institution_name")
    institutionName_Elem.text = "Stanford University"
    doiData_Elem = ET.SubElement(databaseMetadata_Elem, "doi_data")
    doi_Elem = ET.SubElement(doiData_Elem, "doi")
    doi_Elem.text = "10.17989/ENCODE"
    resource_Elem = ET.SubElement(doiData_Elem, "resource")
    resource_Elem.text = "https://www.encodeproject.org"

    #Parse input file to get necessary metadata
    infile_df = pd.read_csv(inFile, '\t')
    for ind in infile_df.index:
        title = infile_df['Accession'][ind]
        institute = infile_df['lab.institute_name'][ind]
        name = infile_df['lab.name'][ind][0].upper()
        surname = infile_df['lab.name'][ind].split('-')[-1].capitalize()

        if exptType in ['experiments', 'functional-characterization-experiments', 'transgenic-enhancer-experiments']:
            biosample = infile_df['Biosample summary'][ind]
            assay = infile_df['Assay title'][ind]
            if isinstance(biosample, str):
                description = assay + ' of ' + biosample
            else: 
                description = assay
            target = infile_df['Target of assay'][ind]
        elif exptType == 'annotations':
            annotation_type = infile_df['Annotation type'][ind]
            biosample = infile_df['Biosample term name'][ind]
            expt_description = infile_df['Description'][ind]
            if annotation_type == 'other' and isinstance(expt_description, str):
                description = expt_description
            elif isinstance(biosample, str) and isinstance(expt_description, str):
                description = annotation_type + ' of ' +  biosample + ', ' + expt_description
            elif not isinstance(biosample, str) and isinstance(expt_description, str):
                description = annotation_type + ' , ' + expt_description
            else:
                description = annotation_type
        year = infile_df['Date released'][ind][0:4]
        month = infile_df['Date released'][ind][5:7]
        day = infile_df['Date released'][ind][8:]
        doi = '10.17989/' + infile_df['Accession'][ind]
        resource = (f'https://www.encodeproject.org/{exptType}/') + infile_df['Accession'][ind] + '/'

        dataset_Elem = ET.Element("dataset", {"dataset_type":"record"})

        contributors_Elem = ET.SubElement(dataset_Elem,"contributors")
        person_name_Elem = ET.SubElement(contributors_Elem,"person_name", {"contributor_role":"author", "sequence": "first"})
        given_name_Elem = ET.SubElement(person_name_Elem,"given_name")
        given_name_Elem.text = name
        surname_Elem = ET.SubElement(person_name_Elem,"surname")
        surname_Elem.text = surname
        affiliation_Elem = ET.SubElement(person_name_Elem,"affiliation")
        affiliation_Elem.text = institute

        dtitles_Elem = ET.SubElement(dataset_Elem,"titles")
        dtitle_Elem = ET.SubElement(dtitles_Elem,"title")
        dtitle_Elem.text = title

        database_date_Elem = ET.SubElement(dataset_Elem,"database_date")
        publication_date_Elem = ET.SubElement(database_date_Elem,"publication_date")
        month_Elem = ET.SubElement(publication_date_Elem,"month")
        month_Elem.text = month
        day_Elem = ET.SubElement(publication_date_Elem,"day")
        day_Elem.text = day
        year_Elem = ET.SubElement(publication_date_Elem,"year")
        year_Elem.text = year

        datasetDescription_Elem = ET.SubElement(dataset_Elem,"description",{"language":"en"})
        if exptType in ['experiments', 'functional-characterization-experiments', 'transgenic-enhancer-experiments']:
            if isinstance(target, str):
                datasetDescription_Elem.text = target + ' ' + description
            else:
                datasetDescription_Elem.text = description
        else:
            datasetDescription_Elem.text = description

        doiData_Elem = ET.SubElement(dataset_Elem,"doi_data")
        doi_Elem = ET.SubElement(doiData_Elem,"doi")
        doi_Elem.text = doi
        resource_Elem = ET.SubElement(doiData_Elem,"resource")
        resource_Elem.text = resource

        database_Elem.append(dataset_Elem)

    writeToFile(doi_batch_Elem, outFile)

    #Produce patch file
    infile_df['doi'] = '10.17989/' + infile_df['Accession']
    infile_df = infile_df.rename(columns={'Accession': 'record_id'})
    patch = infile_df.loc[:, ['record_id', 'doi']]
    patch.to_csv(patchFile, '\t', index=False)

    print("Done")


if __name__ == '__main__':
    main()
