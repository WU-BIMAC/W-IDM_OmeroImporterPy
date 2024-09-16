from setuptools import setup, find_packages
from glob import glob

setup(
    name="omero-Importer",
    version="0.0.1",
    description="Python OMERO importer for overnight import tasks with metadata excel files and optional MMA files",
    packages=find_packages(exclude=["ez_setup"]),
    keywords=["omero", "microscope", "metadata", "micro-meta-app"],
    install_requires=[
        "boto3>=1.34.79",
        "botocore>=1.34.79",
        "pandas>=2.2.2",
        "xlrd>=2.0.1",
        "cryptography>=42.0.5",
        "ezomero>=3.0.0",
    ],
    include_package_data=True,
    zip_safe=False,
    author="Alex Rigano",
    author_email="alex.rigano@umassmed.edu",
)
