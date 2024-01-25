#################################################################################

#################################################################################
"""
Project setup with setuptools
"""

from setuptools import setup, find_packages
from pathlib import Path

cwd = Path(__file__).parent
long_description = (cwd / "README.md").read_text()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name="orphaned-wells-ui-server",
    url="https://github.com/CATALOG-Historic-Records/orphaned-wells-ui-server",
    version="0.1.dev",
    description="Orphaned Wells UI Backend Server",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="UOW team",
    license="",
    # Classifiers help users find your project by categorizing it.
    #
    # For a list of valid classifiers, see https://pypi.org/classifiers/
    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Natural Language :: English",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords="orphaned wells",
    packages=find_packages(
        include=("backend*",),
    ),
    python_requires=">=3.8",
    install_requires=[
        "pymongo>3",  # database interface
        "annotated-types",
        "anyio",
        "click",
        "dnspython",
        "exceptiongroup",
        "fastapi",
        "h11",
        "idna",
        "pydantic>2",
        "pydantic_core",
        "python-dotenv",
        "sniffio",
        "starlette",
        "typing_extensions",
        "uvicorn",
    ],
    extras_require={
        "dev": [
            #     "Sphinx==7.1.*",  # docs
            #     "sphinx_rtd_theme",  # docs
            #     "json-schema-for-humans",  # pretty JSON schema in HTML
            #     "black",  # code formatting
            #     # other requirements
                "pytest",  # test framework
            #     "pytest-cov",  # code coverage
            #     "mongomock",  # mongodb mocking for testing
        ],
    },
)
