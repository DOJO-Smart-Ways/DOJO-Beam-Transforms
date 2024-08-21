from setuptools import setup, find_packages

setup(
    name='dojo-beam-transforms',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'apache-beam[dataframe,gcp,interactive]==2.58.0',
        'pandas==2.0.3',
        'pandas-datareader==0.10.0',
        'PyMuPDF==1.23.22',
        'pypinyin==0.51.0',
        'unidecode==1.3.8',
        'openpyxl==3.0.10',
        'fsspec==2024.6.1',
        'gcsfs==2024.6.1'
    ],
)
