from setuptools import setup, find_packages

setup(
    name='dojo-beam-transforms',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'apache-beam[dataframe,gcp,interactive]==2.54.0',
        'pandas==2.0.3',
        'pandas-datareader==0.10.0',
        'PyMuPDF==1.23.22',
        'pypinyin==0.51.0',
        'unidecode==1.3.8'
    ],
)
