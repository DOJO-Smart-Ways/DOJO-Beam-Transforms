from setuptools import setup, find_packages

setup(
    name='dojo-beam-transforms',
    version='3.0.0',
    packages=find_packages(),
    install_requires=[
        'apache-beam[dataframe,gcp,interactive]==2.64.0',
        'pandas==2.0.3',
        'pytz==2025.2'
    ],
)
