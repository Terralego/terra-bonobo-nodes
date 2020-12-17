import os
import re
import setuptools

HERE = os.path.abspath(os.path.dirname(__file__))

README = open(os.path.join(HERE, "README.md")).read()
CHANGES = open(os.path.join(HERE, "CHANGES.md")).read()

tests_require = [
    "factory-boy",
    "flake8",
    "coverage",
    "pylama",
    "tox",
    "black",
    "isort",
    "tblib",
    "eradicate",
    "asynctest",
]

setuptools.setup(
    name="terra-bonobo-nodes",
    version=open(os.path.join(HERE, "terra_bonobo_nodes", "VERSION.md")).read().strip(),
    include_package_data=True,
    author="Makina Corpus",
    author_email="terralego-pypi@makina-corpus.com",
    description="Set of bonobo ETL's nodes for terralego's projects",
    long_description=README + "\n\n" + CHANGES,
    long_description_content_type="text/markdown",
    url="https://github.com/Terralego/terra-bonobo-nodes",
    install_requires=[
        "Django>=2.2,<3.0",
        "django-geostore",
        "requests",
        "bonobo<0.6.9",
        "elasticsearch>=7.0.0,<8.0.0",
        "psycopg2",
        "pytest",
        "bygfiles",
    ],
    tests_require=tests_require,
    extras_require={"dev": tests_require},
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
