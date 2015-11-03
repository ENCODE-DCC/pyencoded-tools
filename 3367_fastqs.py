import requests
import gzip
from io import BytesIO
# taken from https://github.com/detrout/encode3-curation/blob/master/validate_encode3_aliases.py#L290
# originally written by Diane Trout


def fastq_read(url, reads=1):
    '''Read a few fastq records
    '''
    # Reasonable power of 2 greater than 50 + 100 + 5 + 100
    # which is roughly what a single fastq read is.
    BLOCK_SIZE = 512
    data = requests.get(url, stream=True)

    block = BytesIO(next(data.iter_content(BLOCK_SIZE * reads)))
    compressed = gzip.GzipFile(None, 'r', fileobj=block)
    for i in range(reads):
        header = compressed.readline().rstrip()
        sequence = compressed.readline().rstrip()
        qual_header = compressed.readline().rstrip()
        quality = compressed.readline().rstrip()
        yield (header, sequence, qual_header, quality)

url = "https://www.encodeproject.org/files/ENCFF060XCR/@@download/ENCFF060XCR.fastq.gz"
header, sequence, qual_header, quality = fastq_read(url)
