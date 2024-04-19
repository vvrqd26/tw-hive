from setuptools import setup, find_packages

setup(
    name='tw_hive',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'tweety-ns',
        'flask',
    ]
)
