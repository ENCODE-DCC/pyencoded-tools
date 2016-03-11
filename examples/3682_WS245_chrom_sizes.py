import argparse
import re
import csv
import copy

EPILOG = '''
For more details:

        %(prog)s --help

This script is from Tim's suggestion,
my previous attampts did not work at all like expected

'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--infile',
                        help="single column list of object accessions")
    parser.add_argument('--debug',
                        default=False,
                        action='store_true',
                        help="Print debug messages.  Default is False.")
    args = parser.parse_args()
    return args


def main():
    args = getArgs()
    feature_type = ["mRNA", "ncRNA", "transposon", "pseudogene"]
    worm_parse = ["WormBase", "WormBase_transposon"]
    IDS = [line.rstrip("\n") for line in open("WS245Transcripts.txt")]
    # read in file
    read = [line.rstrip("\n") for line in open("WS245_transcripts.fasta")]

    split_list = []
    # find where we split the lines
    for x in range(0, len(read)):
        if ">" in read[x]:
            split_list.append(x)
    split_list.append(len(read))
    finder = re.compile(">(.+?)\s")
    chrom_sizes = {}
    with open("chrom.sizes", "w") as outfile:
        for x in range(0, len(split_list)-1):
            # split the lines, get read length, chrom name
            temp = read[split_list[x]:split_list[x+1]]
            result = finder.findall(temp[0])[0]
            #print(result)
            if result in IDS:
                sequence = ""
                for s in temp[1:]:
                    sequence += s.rstrip("\n")
                size = len(sequence)
                chrom_sizes[result] = sequence
                outfile.write("{chrom}\t{size}\n".format(chrom=result, size=size))


if __name__ == '__main__':
        main()
