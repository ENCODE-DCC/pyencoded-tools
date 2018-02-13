import requests
import pandas as pd

base_url = 'https://www.encodeproject.org/'
base_query = 'search/?type=Experiment&award.rfa=ENCODE3&status%21=deleted&status%21=replaced'
data_query = '&field=status&field=lab.title&field=assay_title&field=internal_status&limit=all&format=json'
auth = ("yourid", "yourpw")
output_file = 'encode_summary.xlsx'


def get_data():
    url = base_url + base_query + data_query
    print('Getting data from:', url)
    r = requests.get(url, auth=auth)
    return r.json()['@graph']


def make_url(row):
    query = '&lab.title={}&assay_title={}&status={}&internal_status={}'.format(*row)
    return base_url + base_query + query


def parse_data(data):
    for d in data:
        lab_title = d['lab']['title']
        d['lab'] = lab_title
        d.pop('@type', None)
    return data


def make_dataframe(data):
    dfs = pd.DataFrame(data).groupby(['lab', 'assay_title', 'status', 'internal_status']).count().reset_index()
    dfs['url'] = dfs[['lab', 'assay_title', 'status', 'internal_status']].apply(lambda x: make_url(x), axis=1)
    dfs['count'] = dfs[['url', '@id']].apply(lambda x: '=HYPERLINK("{}",{})'.format(x[0], x[1]), axis=1)
    return dfs


def output_excel(dfs):
    print('Outputting Excel to', output_file)
    dfs.drop(['@id', 'url'], axis=1).groupby(['lab', 'status', 'internal_status',
                                              'assay_title', 'count']).count().to_excel(output_file)
    

def main():
    data = parse_data(get_data())
    dfs = make_dataframe(data)
    output_excel(dfs) 
    

if __name__ == '__main__':
    main()
