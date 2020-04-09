
# from distutils.core import setup
from setuptools import setup, find_packages

find_packages_list = find_packages()


setup(
    name="grpyutil",
    version="0.11",
    description="utils module",
    author="ZJ",
    # url="",
    license=" LGPL ",
    packages=find_packages_list,
    # scripts=[" scripts/test.py "],
)
