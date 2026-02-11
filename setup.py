from setuptools import setup, find_packages

setup(
    name='dojo-beam-transforms',
    version='3.1.0',
    packages=find_packages(),
    install_requires=[
        'apache-beam[dataframe,gcp,interactive]==2.71.0',
        'pandas==2.0.3',
        'numpy==1.26.3',
        'pytz==2025.2',
        'openpyxl==3.1.5'
    ],
)
