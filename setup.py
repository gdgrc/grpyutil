
# from distutils.core import setup
from setuptools import setup, find_packages

find_packages_list = find_packages()

print(find_packages_list)
setup(
    name="grpyutil",
    version="0.10",
    description="utils module",
    author="ZJ",
    # url="",
    license=" LGPL ",
    packages=find_packages_list,
    # scripts=[" scripts/test.py "],
)


import sys


def get():
    return sys.path
