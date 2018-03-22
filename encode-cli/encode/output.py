import click
import pandas as pd


def print_header(data, encode_object, field):
    click.secho('Found {} {}{}{}'.format(len(data),
                                         encode_object,
                                         ':' if field else ' ',
                                         field if field else ''),
                bold=True, fg='green')


def print_data(data, columns, out):
    if out == 'raw':
        data = '\n'.join(str(d) for d in data)
    else:
        df = pd.DataFrame(data, columns=columns)
        data = df.__getattr__('to_{}'.format(out))()
    click.secho(data, bold=True)


def print_error(message):
    click.secho(message, bold=True, fg='red')
