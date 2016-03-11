import argparse
import os.path
import encodedcc
import csv

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("~/keypairs.json")))
    parser.add_argument('--debug',
                        default=False,
                        action='store_true',
                        help="Print debug messages.  Default is False.")
    parser.add_argument('--update',
                        action='store_true',
                        default=False,
                        help="if there is one control, and it has experiments give exp that control")
    args = parser.parse_args()
    return args


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    connection = encodedcc.ENC_Connection(key)
    control = "/search/?type=experiment&award.project=Roadmap&status=released&assay_term_name=ChIP-seq&target.investigated_as=control"
    missing_control = "/search/?type=experiment&award.project=Roadmap&status=released&audit.NOT_COMPLIANT.category=missing+possible_controls"

    control_list = encodedcc.get_ENCODE(control, connection, frame="embedded").get("@graph", [])
    missing_list = encodedcc.get_ENCODE(missing_control, connection, frame="embedded").get("@graph", [])
    links = {}
    missing_accessions = []
    control_accessions = []
    for value in missing_list:
        missing_accessions.append(value["accession"])
    for value in control_list:
        control_accessions.append(value["accession"])
    print("building dictionary")
    for obj in control_list:
        if obj.get("replicates"):
            bio_acc = obj["replicates"][0]["library"]["biosample"]["accession"]
            links[bio_acc] = [[], []]
    print("sorting data")
    for obj in control_list:
        if obj.get("replicates"):
            control_acc = obj["accession"]
            bio_acc = obj["replicates"][0]["library"]["biosample"]["accession"]
            links[bio_acc][0].append(control_acc)
    for obj in missing_list:
        missing_acc = obj["accession"]
        # print(missing_acc)
        if obj.get("replicates"):
            bio_acc = obj["replicates"][0]["library"]["biosample"]["accession"]
        if links.get(bio_acc):
            links[bio_acc][1].append(missing_acc)
    found_controls = []
    found_missings = []
    print("writing data")
    with open("results.txt", "w") as f:
        f.write("biosample" + "\t" + "possible control" + "\t" + "possible experiments" + "\n")
        for key in links.keys():
            c = ";".join(links[key][0])
            for item in links[key][0]:
                found_controls.append(item)
            e = ";".join(links[key][1])
            for item in links[key][1]:
                found_missings.append(item)
            if len(links[key][1]) > 0:
                s = key + "\t" + c + "\t" + e +"\n"
                f.write(s)
    print("checking for left over items")
    diff_missing = set(missing_accessions) - set(found_missings)
    header = ["experiment", "biosample", "description", "controls", "control description", "control biosample", "lab", "control lab", "bio term name", "control term name", "bio type", "control bio type", "age", "control age", "organism", "control organism"]
    temp_list = []
    possible = []
    for acc in diff_missing:
        exp = encodedcc.get_ENCODE(acc, connection, frame="embedded")
        temp = {}
        if exp.get("replicates"):
            bio = exp["replicates"][0]["library"]["biosample"]["accession"]
            lab = exp["lab"]["@id"]
            bio_name = exp["replicates"][0]["library"]["biosample"]["biosample_term_name"]
            bio_type = exp["replicates"][0]["library"]["biosample"]["biosample_type"]
            bio_age = exp["replicates"][0]["library"]["biosample"]["age"]
            organism = exp["replicates"][0]["library"]["biosample"]["organism"]["name"]
            des = exp["replicates"][0]["library"]["biosample"].get("description", "NONE")
            for con in control_list:
                if con.get("replicates"):
                    con_id = con["accession"]
                    con_bio = con["replicates"][0]["library"]["biosample"]["accession"]
                    con_lab = con["lab"]["@id"]
                    con_bio_name = con["replicates"][0]["library"]["biosample"]["biosample_term_name"]
                    con_bio_type = con["replicates"][0]["library"]["biosample"]["biosample_type"]
                    con_age = con["replicates"][0]["library"]["biosample"]["age"]
                    con_organism = con["replicates"][0]["library"]["biosample"]["organism"]["name"]
                    con_des = con["replicates"][0]["library"]["biosample"].get("description", "NONE")
                    if bio_name == con_bio_name and lab == con_lab and bio_type == con_bio_type and organism == con_organism:
                        possible.append(acc)
                        temp["experiment"] = acc
                        temp["biosample"] = bio
                        temp["description"] = des
                        temp["controls"] = con_id
                        temp["control description"] = con_des
                        temp["control biosample"] = con_bio
                        temp["lab"] = lab
                        temp["control lab"] = con_lab
                        temp["bio term name"] = bio_name
                        temp["control term name"] = con_bio_name
                        temp["bio type"] = bio_type
                        temp["control bio type"] = con_bio_type
                        temp["age"] = bio_age
                        temp["control age"] = con_age
                        temp["organism"] = organism
                        temp["control organism"] = con_organism
        temp_list.append(temp)
    with open("missing_control.txt", "w") as tsvfile:
        writer = csv.DictWriter(tsvfile, delimiter='\t', fieldnames=header)
        writer.writeheader()
        for item in temp_list:
            writer.writerow(item)
    really_missing = set(diff_missing) - set(possible)
    with open("extras.txt", "w") as f:
        f.write(str(len(really_missing)) + " experiments unaccounted for\n")
        for line in really_missing:
            f.write(line + "\n")
    print("output written to results.txt, missing_controls.txt and extras.txt")
    '''if args.update:
                    for key in links.keys():
                        if len(links[key][0]) == 1 and len(links[key][1]) > 0:
                            # this is a single control with at least 1 experiment
                            patch_data = {}
                            for exp in links[key][1]:
                                patch_data["possible_controls"] = links[key][0]
                                print(exp, patch_data)
                                encodedcc.patch_ENCODE(exp, connection, patch_data)
            '''
if __name__ == '__main__':
        main()
