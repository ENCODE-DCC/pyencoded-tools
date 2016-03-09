import encodedcc


def files(objList, fileCheckedItems, connection):
    for i in range(0, len(objList)):
        exp = encodedcc.get_ENCODE(objList[i], connection, frame='embedded')
        if any(exp.get("files")):
            expfiles = exp["files"]
        else:
            tempfiles = exp.get("original_files")
            expfiles = []
            for t in tempfiles:
                temp = encodedcc.get_ENCODE(t, connection, frame='embedded')
                expfiles.append(temp)
        for i in range(0, len(expfiles)):
            fileob = {}
            file = expfiles[i]
            for field in fileCheckedItems:
                fileob[field] = file.get(field)
            fileob['submitted_by'] = file['submitted_by']['title']
            fileob['experiment'] = exp['accession']
            fileob['experiment-lab'] = exp['lab']['name']
            fileob['biosample'] = exp.get('biosample_term_name', '')
            fileob['flowcell'] = []
            fileob['lane'] = []
            fileob["Uniquely mapped reads number"] = ""
            if expfiles[i].get("file_format", "") == "bam":
                temp_file = expfiles[i]
                for q in temp_file.get("quality_metrics", []):
                    if "star-quality-metrics" in q.get("@id", ""):
                        fileob["Uniquely mapped reads number"] = q.get("Uniquely mapped reads number")
            for fcd in file['flowcell_details']:
                fileob['flowcell'].append(fcd.get('flowcell', ''))
                fileob['lane'].append(fcd['lane'])
            try:
                fileob['platform'] = fileob['platform']['title']
            except:
                fileob['platform'] = None
            try:
                fileob['species'] = exp['replicates'][0]['library']['biosample']['donor']['organism']['name']
            except:
                fileob['species'] = ''
            if 'replicate' in file:
                    rep = file['replicate']
                    if 'library' in rep and rep['library'] is not None:
                        library = file['replicate'].get('library')
                        try:
                            fileob['library_aliases'] = library['aliases']
                        except:
                            fileob['library_aliases'] = ""
                        if 'biosample' in library:
                            fileob['biosample_aliases'] = library['biosample']['aliases']
            if 'alias' in exp:
                fileob['alias'] = exp['aliases'][0]
            else:
                fileob['alias'] = ''
            if 'replicate' in file:
                fileob['biological_replicate'] = file['replicate']['biological_replicate_number']
                fileob['technical_replicate'] = file['replicate']['technical_replicate_number']
                fileob['replicate_id'] = file['replicate'].get('uuid')
            else:
                fileob['biological_replicate'] = fileob['technical_replicate'] = fileob['replicate_alias'] = ''
            row = []
            for j in fileCheckedItems:
                row.append(repr(fileob[j]))
            print('\t'.join(row))
