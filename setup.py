from setuptools import setup, find_packages

setup(
    name='dojo-beam-transforms',
    version='3.1.0',
    description='Apache Beam 2.72.0 transforms and utilities for Python 3.12',
    packages=find_packages(),
    install_requires=[
        'apache-beam[dataframe,gcp,interactive]==2.72.0',
        'pandas==2.1.1',
        'numpy==1.26.3',
        'pytz==2025.2',
        'openpyxl==3.1.5'
    ],
)
