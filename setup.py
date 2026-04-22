from pathlib import Path
from setuptools import setup, find_packages

README = Path(__file__).with_name("README.md").read_text(encoding="utf-8")

setup(
    name='dojo-beam-transforms',
    version='3.1.1.post1',
    description='Apache Beam 2.72.0 transforms and utilities for Python 3.12',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms',
    project_urls={
        'Source': 'https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms',
        'Issues': 'https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms/issues',
    },
    packages=find_packages(),
    install_requires=[
        'apache-beam[dataframe,gcp,interactive]==2.72.0',
        'pandas==2.1.1',
        'apache-beam[dataframe,gcp,interactive]==2.72.0',
        'pandas==2.1.1',
        'numpy==1.26.3',
        'pytz==2025.2',
        'openpyxl==3.1.5'
    ],
)
