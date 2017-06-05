table wgEncodeUncBsuProtH1WCLModPepMap
"Format for genomic mappings of mass spec proteogenomic hits - modified peptide version"
(
string  chrom;    "Reference sequence chromosome or scaffold"
uint    chromStart;     "Start position in chromosome"
uint    chromEnd;               "End position in chromosome"
string  name;           "Peptide sequence of the match with the integer portion of modification mass"
uint    score;          "Raw score scaled to a score of 0 (worst) to 1000 (best)"
char[1] strand;         "+ or - for strand"
uint    thickStart;     "Start position in chromosome (same as chromStart)"
uint    thickEnd;               "End position in chromosome (same as chromEnd)"
string  itemRgb;                "Color set to 0 for black"
uint    blockCount; "The number of blocks for a peptide mapped to the genome - peptides spanning splice junctions will have more than 1 block"
string  blockSizes; "Size of peptide block"
string  blockStarts; "Start position of peptide block"
float rawScore; "Raw score for a peptide/spectrum match"
string spectrumId; "An identifier of the spectrum associated with the peptide mapping"
uint peptideRank; "Rank of the peptide/spectrum match, for spectrum matching to different peptides"
float modMass; "Reflects the additional molecular weight for each modified peptide matched to a spectrum"
)

