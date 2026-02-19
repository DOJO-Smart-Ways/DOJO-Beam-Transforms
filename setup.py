from setuptools import setup, find_packages

setup(
    name='dojo-beam-transforms',
    version='3.0.0',
    packages=find_packages(),
    install_requires=[
        'apache-beam[dataframe,gcp,interactive]>=2.61.0',  # 2.61+ added Python 3.12 support
        'pandas>=2.1.0',                                   # 2.1+ has Python 3.12 wheels
        'pytz>=2021.3',
        'openpyxl>=3.0.0',
    ],
)
