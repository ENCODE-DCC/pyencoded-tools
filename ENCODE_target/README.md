# Create a gene target on ENCODE portal

A new gene target on ENCODE portal should have the following properties:

* Status (status): can go directly to released.
* Targeted genes (genes): add one (or rarely more) gene(s).
* Target label (label): this field is free text. But preferably the label is the same as the gene symbol if it’s one gene target without modification or reflecting gene symbol(s) if it has multiple corresponding genes and/or is modified.
* Target modification (modifications): add one or more modifications if the target has been tagged or modified as comparing to wild type endogenous gene.
* Target category (investigated_as): use the following script to figure out one more more categories for the new target.

### A script to classify ENCODE target.

```
$./classify_targets.py -h
usage: classify_targets.py [-h] [--uniprots UNIPROTS [UNIPROTS ...]]
                           [--mgis MGIS [MGIS ...]] [--fbs FBS [FBS ...]]
                           [--wbs WBS [WBS ...]]
                           [--encode-go-map ENCODE_GO_MAP]
                           [--get-new-encode-go-map]

Calculate ENCODE category for a target.

optional arguments:
  -h, --help            show this help message and exit
  --uniprots UNIPROTS [UNIPROTS ...]
                        One or more UniProt ID(s). For example, Q9H9Z2
  --mgis MGIS [MGIS ...]
                        One or more MGI ID(s). For example, 1890546
  --fbs FBS [FBS ...]   One or more FlyBase ID(s). For example, FBgn0035626
  --wbs WBS [WBS ...]   One or more WormBase ID(s). For example,
                        WBGene00003014
  --encode-go-map ENCODE_GO_MAP
                        A JSON file mapping GO terms to ENCODE target
                        categories.
  --get-new-encode-go-map
                        A JSON file mapping GO terms to ENCODE target
                        categories.
```

1. Map GO term to ENCODE categories

  First, the script need to get a mapping to find the correct ENCODE category for each GO term. By default it will look for the mapping in a JSON file "ENCODE_GO_map.json" under the current working directory. Or a custom JSON map can be provided through the "--encode-go-map" option.

  Instead, this can be done through an OBO file over the internet (http://purl.obolibrary.org/obo/go.obo) by specifying the "--get-new-encode-go-map" option. This method will save the generated mapping under the current working directory as "ENCODE_GO_map.json".

2. Use the correct ID(s) for the ENCODE target

  It is important to understand what ID should be used to specify a target for the script. **In general, this script is designed for one target per run. (Modules/Functions in the script can be used if a large scale target categorization is desired.)** It will look for GO annotation through GOlr API (http://geneontology.org/docs/tools-guide/). IDs from GAF annotations (http://current.geneontology.org/products/pages/downloads.html) should be the correct IDs. In summary:

    * Human: UniProtKB
    * Mouse: MGI (mostly); UniProtKB (a few)
    * Fruit fly: FlyBase (mostly); UniProtKB (a few)
    * Worm: WormBase (mostly); UniProtKB (a feww)

  For the four database in the summary above, there are four corresponding options ("--uniprots", "--mgis", "--fbs", "--wbs") which can be used to provide IDs to the script. It is worth noting that some genes may have more than one corresponding UniProtKB IDs (such as more than one isoforms). Thus the script can take in more than one IDs for these options.

3. Examples:

  * First time run on human CUX1 gene, which has three UniProtKB proteins, with no GO term category map yet:

    `./classify_targets.py --uniprots Q13948 P39880 Q3LIA3 --get-new-encode-go-map`

  * Categorize mouse *Ctcf* use both MGI and UniProtKB IDs with default GO term category map. Using IDs from two database is not necessary here since only one would hit. But it won't hurt either:

    `./classify_targets.py --mgis 109447 --uniprots Q61164`

4. About how target category is determined

  In short, the ENCODE category is determined by votes from GO annotations weighed on their evidence level. The script will first keep only GO terms with highest evidence level among all GO annotations of that target. Then every remaining GO term will be converted to ENCODE categories based on GO term category map. Finally, categories with the most GO term support will be reported as the target categories.
