#!/usr/bin/env python
from setuptools import setup

with open('requirements.txt') as f:
    requirements = [l.strip() for l in f]

with open('README.rst') as f:
    long_desc = f.read().strip()

setup(
    name='PyTestlink',
    version='0.1',
    url='http://wazo.community',
    license='GPL3',
    author='Wazo Authors',
    author_email='dev.wazo@gmail.com',
    description='Minimal library for accessing a testlink database',
    long_description=long_desc,
    packages=['testlink'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=requirements,
    scripts=['bin/testlink_report'],
    classifiers=[
        # see http://pypi.python.org/pypi?:action=list_classifiers
        # -*- Classifiers -*-
        'License :: OSI Approved',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        "Programming Language :: Python",
    ]
)
