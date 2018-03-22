from distutils.core import setup

setup(
    name='encode-cli',
    version='0.1',
    license='MIT',
    install_requires=[
        'Click',
    ],
    author='Keenan Graham',
    author_email='keenangraham@stanford.edu',
    maintainer='Ben Hitz',
    url='https://github.com/ENCODE-DCC/pyencoded-tools/tree/ENCD-3136-encode-cli/encode-cli',
    maintainer_email='hitz@stanford.edu',
    entry_points='''
        [console_scripts]
        encode=encode.main:cli
    ''',
)
