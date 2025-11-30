from setuptools import setup, find_packages

setup(name='dependencies',
version='0.1',
description='ETL Pipeline Dependencies',
author='Shadiqur',
author_email='shadiqurshaon@gmail.com',
packages=find_packages(include=['common','configs','connectors','logs','jobs','jobs.*','schemas','tests','tests.*']),
package_data={'configs': ['config.yaml', 'config_local.yaml']},
)


