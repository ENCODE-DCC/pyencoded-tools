from setuptools import setup

setup(
    name='encode-cli',
    version='0.1',
    license='MIT',
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        encode=encode.main:cli
    ''',
)
