import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="terra_bonobo_nodes",
    version="0.0.1",
    author="rve",
    author_email="rve@makina-corpus.com",
    description="A small example package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    install_requires=[
        "requests>=2.19,<2.20",
        "django>=2.1,<2.19",
        "bonobo>=0.6.0,<0.6.9",
        "elasticsearch>=0.6.0,<7.0.0",
        "psycopg2>=2.7.0,<2.7.9",
        "pytest>=4.5.0,<4.5.9",
    ],
    dependency_links=[
        "git+https://github.com/jrmi/pyfiles.git@master#egg=pyfiles-0.1",
        "git+https://github.com/Terralego/terra-back.git@master#egg=terra-back",
    ],
    tests_require=['asynctest'],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)