import argparse
import os.path
import encodedcc
import requests
import bs4

# http://stackoverflow.com/questions/27652543/how-to-use-python-requests-to-fake-a-browser-visit

EPILOG = '''
For more details:

        %(prog)s --help
'''


def getArgs():
    parser = argparse.ArgumentParser(
        description=__doc__, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--infile',
                        help="single column list of object accessions")
    parser.add_argument('--key',
                        default='default',
                        help="The keypair identifier from the keyfile.  \
                        Default is --key=default")
    parser.add_argument('--keyfile',
                        default=os.path.expanduser("~/keypairs.json"),
                        help="The keypair file.  Default is --keyfile=%s" % (os.path.expanduser("~/keypairs.json")))
    parser.add_argument('--showall',
                        help="prints out facet names",
                        action='store_true',
                        default=False)
    args = parser.parse_args()
    return args


def make_soup(response, showall):
    soup = bs4.BeautifulSoup(response.text, "lxml")
    facet = soup.find_all("div", class_="facet")
    print("total facets found {}".format(len(facet)))
    if showall:
        for f in facet:
            temp = f.find_all('h5')
            for t in temp:
                print(t.text)


def main():

    args = getArgs()
    key = encodedcc.ENC_Key(args.keyfile, args.key)
    auth = (key.authid, key.authpw)
    connection = encodedcc.ENC_Connection(key)
    types = ["Experiment", "Biosample", "AntibodyLot"]
    for t in types:
        print("Showing results for {} on {}".format(t, connection.server))
        url = "{}/search/?type={}&format=html".format(connection.server, t)
        #print(url)
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        response = requests.get(url, headers=headers)
        make_soup(response, args.showall)
        print("LOGGING IN NOW")
        login = requests.get(url, auth=auth, headers=headers)
        #print(login.text)
        make_soup(login, args.showall)


if __name__ == '__main__':
    main()
