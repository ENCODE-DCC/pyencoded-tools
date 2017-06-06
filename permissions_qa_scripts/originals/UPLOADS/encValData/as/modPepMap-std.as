table modPepMap
"Format for genomic mappings of mass spec proteogenomic hits - modified peptide version"
    (
    string chrom;      "Chromosome (or contig, scaffold, etc.)"
    uint   chromStart; "Start position in chromosome"
    uint   chromEnd;   "End position in chromosome"
    string name;       "Name of item"
    uint   score;      "Score from 0-1000"
    char[1] strand;    "+ or -"
    uint thickStart;   "Start of where display should be thick (start codon)"
    uint thickEnd;     "End of where display should be thick (stop codon)"
    uint reserved;     "Used as itemRgb as of 2004-11-22"
    int blockCount;    "Number of blocks"
    int[blockCount] blockSizes; "Comma separated list of block sizes"
    int[blockCount] chromStarts; "Start positions relative to chromStart"
    float rawScore; "Raw score for a peptide/spectrum match"
    string spectrumId; "An identifier of the spectrum associated with the peptide mapping"
    uint peptideRank; "Rank of the peptide/spectrum match, for spectrum matching to different peptides"
    float modMass; "Reflects the additional molecular weight for each modified peptide matched to a spectrum"
    )

