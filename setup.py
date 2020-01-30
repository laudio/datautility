''' Package setup. '''

import setuptools

import datautility

# Requirements
requirements = [
    'pyodbc>=4.0.27,<5',
    'flatten_json==0.1.6'
]

# Development Requirements
requirements_dev = [
    'pytest==4.*',
    'mock==3.0.5'
]

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name=datautility.name,
    version=datautility.version,
    description='Common data utilities for python scripts',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/laudio/datautility',
    packages=setuptools.find_packages(),
    install_requires=requirements,
    extras_require={'dev': requirements_dev},
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
