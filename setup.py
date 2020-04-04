from setuptools import setup, find_packages
setup(
    name="grpyutil",
    version="0.10",
    description="utils module",
    author="ZJ",
    # url="",
    license=" LGPL ",
    packages=find_packages(),
    # scripts=[" scripts/test.py "],
)


import sys


def get():
    return sys.path
