from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='invoker',
      version=version,
      description="A wide-area task management system.",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Osamu Ogasawara',
      author_email='oogasawa@nig.ac.jp',
      url='https://github.com/code-lab-0/invoker',
      license='LGPL-3',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'rethinkdb'
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
