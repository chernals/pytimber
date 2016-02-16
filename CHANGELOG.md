# pytimber changelog

## 01/2016 (T. Levens)

  * Updated repository structure
  * Added setup.py for setuptools/pip installation
  * Added support for the (in progress) [.jar management system](https://gitlab.cern.ch/bi/cmmnbuild-dep-manager)
    which allows better cohabitation with PyJapc.
  * By default, if this module is not installed then the bundled .jar file is
    used as before.

## 12/2015 (C. Hernalsteens)

New branch **multiprocessing** with support for parallelized Java CALS
acquisition and data processing. The performance gain is excellent so far.

  * Multithreading in Python to parallelize the Java acquisition API calls
  * Use of python `multiprocessing` to parallelize the data processing

Merged from my own package. The API should be identical, except for the new
functionalities that should be transparent for existing use.

  * Developed for use with 'cycled' machines (PSB, PS, SPS)
  * Filtering by fundamental data
  * Aligned datasets (*getAligned()*)
  * (minor) Support for MATRIXNUMERIC datatype
  * (minor) Split some functions into smaller pieces

## 10/2015 (M. Betz)

Documentation (=examples) in pyTimberExamples.ipynb

Applied some changes to integrate pyTimber in the BI Anaconda installation.

  * Made compatible with Python3
  * Simplified the return value (less nesting of lists in dicts, etc.)
  * Specify a point in time or over a time range
