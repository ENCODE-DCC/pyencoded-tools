import encodedcc


def files(objList, fileCheckedItems, connection):
    for obj in objList:
        exp = encodedcc.get_ENCODE(obj, connection)
        if any(exp.get("files")):
            expfiles = exp["files"]
        else:
            expfiles = exp["original_files"]
        for f in expfiles:
            fileob = {}
            file = encodedcc.get_ENCODE(f, connection)
            for field in fileCheckedItems:
                fileob[field] = file.get(field)
            fileob["submitted_by"] = encodedcc.get_ENCODE(file["submitted_by"], connection)["title"]
            fileob["experiment"] = exp["accession"]
            fileob["experiment-lab"] = encodedcc.get_ENCODE(exp["lab"], connection)["name"]
            fileob["biosample"] = exp.get("biosample_term_name", "")
            fileob["flowcell"] = []
            fileob["lane"] = []
            fileob["Uniquely mapped reads number"] = ""
            fileob["biological_replicate"] = ""
            fileob["technical_replicate"] = ""
            fileob["replicate_id"] = ""
            if file.get("file_format", "") == "bam":
                for q in file.get("quality_metrics", []):
                    if "star-quality-metrics" in q.get("@id", ""):
                        fileob["Uniquely mapped reads number"] = q["Uniquely mapped reads number"]
            for fcd in file["flowcell_details"]:
                fileob["flowcell"].append(fcd.get("flowcell", ""))
                fileob["lane"].append(fcd["lane"])
            try:
                fileob["platform"] = encodedcc.get_ENCODE(fileob["platform"], connection)["title"]
            except:
                fileob["platform"] = None
            if "replicates" in exp:
                temp_rep = encodedcc.get_ENCODE(exp["replicates"][0], connection)
                if "library" in temp_rep:
                    temp_lib = encodedcc.get_ENCODE(temp_rep["library"], connection)
                    if "biosample" in temp_lib:
                        temp_bio = encodedcc.get_ENCODE(temp_lib["biosample"], connection)
                        if "donor" in temp_bio:
                            temp_don = encodedcc.get_ENCODE(temp_bio["donor"], connection)
                            if "organism" in temp_don:
                                temp_org = encodedcc.get_ENCODE(temp_don["organism"], connection)
                                fileob["species"] = temp_org["name"]
            else:
                fileob["species"] = ""
            if "replicate" in file:
                rep = encodedcc.get_ENCODE(file["replicate"], connection)
                fileob["biological_replicate"] = rep["biological_replicate_number"]
                fileob["technical_replicate"] = rep["technical_replicate_number"]
                fileob["replicate_id"] = rep["uuid"]
                if "library" in rep:
                    library = encodedcc.get_ENCODE(rep["library"], connection)
                    try:
                        fileob["library_aliases"] = library["aliases"]
                    except:
                        fileob["library_aliases"] = ""
                    if "biosample" in library:
                        bio = encodedcc.get_ENCODE(library["biosample"], connection)
                        fileob["biosample_aliases"] = bio["aliases"]
            if "aliases" in exp:
                fileob["alias"] = exp["aliases"][0]
            else:
                fileob["alias"] = ""
            row = []
            for j in fileCheckedItems:
                row.append(repr(fileob[j]))
            print('\t'.join(row))
