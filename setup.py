from setuptools import setup

setup(
    name="cm_tools",
    author="Eric Charles, Fritz Mueller",
    author_email="echarles@slac.stanford.edu, fritzm@SLAC.Stanford.EDU",
    url="https://github.com/lsst-dm/cm_tools",
    packages=["lsst.cm.tools"],
    package_dir={"": "python"},
    description="Campaign Managment Tools",
    setup_requires=["setuptools_scm"],
    long_description=open("README.rst").read(),
    package_data={"": ["README.rst", "LICENSE"]},
    use_scm_version={"write_to": "python/lsst/cm/tools/_version.py"},
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    install_requires=["lsst-daf-butler", "sqlalchemy-utils"],
    scripts=["bin/cm"],
    tests_require=["pytest", "pytest-cov"],
)
