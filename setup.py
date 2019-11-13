import os
import setuptools

HERE = os.path.abspath(os.path.dirname(__file__))

README = open(os.path.join(HERE, 'README.md')).read()
CHANGES = open(os.path.join(HERE, 'CHANGES.md')).read()

setuptools.setup(
    name="terra-bonobo-nodes",
    version=open(os.path.join(HERE, 'django_geosource', 'VERSION.md')).read().strip(),
    include_package_data=True,
    author="Makina Corpus",
    author_email="terralego-pypi@makina-corpus.com",
    description="Set of bonobo ETL's nodes for terralego's projects",
    long_description=README + '\n\n' + CHANGES,
    long_description_content_type="text/markdown",
    url="https://github.com/Terralego/terra-bonobo-nodes",
    install_requires=[
        "requests>=2.19,<2.20",
        "django>=2.2.7",
        "bonobo>=0.6.0,<0.6.9",
        "elasticsearch>=0.6.0,<7.0.0",
        "psycopg2>=2.7",
        "pytest>=4.5.0,<4.5.9",
    ],
    dependency_links=[
        "git+https://github.com/jrmi/pyfiles.git@master#egg=pyfiles-0.1",
    ],
    tests_require=['asynctest'],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
