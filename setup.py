import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="terra-bonobo-nodes",
    version="0.3.0",
    author="Terralego",
    author_email="terralego-pypi@makina-corpus.com",
    description="Set of bonobo ETL's nodes for terralego's projects",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Terralego/terra-bonobo-nodes",
    install_requires=[
        "requests>=2.19,<2.20",
        "django==2.2.5",
        "bonobo>=0.6.0,<0.6.9",
        "elasticsearch>=0.6.0,<7.0.0",
        "psycopg2>=2.7",
        "pytest>=4.5.0,<4.5.9",
        "terra-common>0.3",
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
