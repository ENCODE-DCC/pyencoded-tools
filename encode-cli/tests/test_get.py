from encode.main import cli


def test_basic_get_with_field(runner):
    result = runner.invoke(cli,
                           ['get',
                            '/users/860c4750-8d3c-40f5-8f2c-90c5e5d19e88/',
                            '--field',
                            'title'])
    assert result.output.splitlines() == [
        'Using server: https://www.encodeproject.org/',
        '"J. Michael Cherry"']
