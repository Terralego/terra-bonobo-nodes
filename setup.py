import os
import re
import setuptools

HERE = os.path.abspath(os.path.dirname(__file__))

README = open(os.path.join(HERE, 'README.md')).read()
CHANGES = open(os.path.join(HERE, 'CHANGES.md')).read()

with open('requirements.txt') as fic:
    reqs = [a.strip() for a in fic.read().splitlines()
            if a.strip() and not re.search('^\\s*(#|-e)', a)]

setuptools.setup(
    name="terra-bonobo-nodes",
    version=open(os.path.join(HERE, 'terra_bonobo_nodes', 'VERSION.md')).read().strip(),
    include_package_data=True,
    author="Makina Corpus",
    author_email="terralego-pypi@makina-corpus.com",
    description="Set of bonobo ETL's nodes for terralego's projects",
    long_description=README + '\n\n' + CHANGES,
    long_description_content_type="text/markdown",
    url="https://github.com/Terralego/terra-bonobo-nodes",
    install_requires=reqs,
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
