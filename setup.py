from setuptools import setup, find_packages

setup(
    name="ecom-pipeline",
    version="0.1.0",
    description="Ecommerce ETL/ELT pipeline",
    packages=find_packages(include=["pipeline", "pipeline.*"]),
    include_package_data=True,
    install_requires=[
        "setuptools>=65.0.0",
        "wheel",
        "PyYAML>=6.0",
        "pyspark>=3.3.0",
        "psycopg2-binary>=2.9.9",
        "pyodbc>=5.0.1",
    ],
    python_requires=">=3.10",
)
