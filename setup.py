from setuptools import setup, find_packages

setup(
    name='dojo-beam-transforms',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'fsspec==2023.5.0',
        'gcsfs==2023.5.0',
        'openpyxl==3.0.10',
        'pandas==1.5.3',
        'pandas-datareader==0.10.0',
        'pandas-gbq==0.17.9',
        'PyMuPDF==1.23.22',
    ],
)
