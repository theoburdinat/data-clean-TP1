# TP 1 : Formatting and Sanitizing - 

<div class="alert alert-warning">

**Note**: you do not have writing rights over this repo. One of the teammates should create a fork (a remote copy under your username, in the github world) for your group to work on. 
[Check this tutorial to learn about forks](https://www.earthdatascience.org/workshops/intro-version-control-git/about-forks/).

</div>

# Instructions
## Context
For this first practical assignment, we'll be working with data from _Montpellier métropole_. 
You will be downloading the [raw "Defibrillators of Montpellier" dataset](https://data.montpellier3m.fr/sites/default/files/ressources/MMM_MMM_DAE.csv) (the one you've played with in CodinGame).

## Requirements 
In groups you will write a loader script that creates the clean (formatted+sanitized+framed) dataframe.
From the original raw data, you want to extract a data frame containing
the information provided in the "Defibrillators" CodinGame challenge, + some extra info:
- Name
- Address (including postal code and city name)
- Contact phone number (one number)
- Latest maintenance date
- Maintenance frequency
- Longitude (in degrees)
- Latitude (in degrees)

## Deliverables
A fork of this git repo with 
- specification of each test sample in `loader_test.py`.
- a working version of `loader.py` made by your group, with comments on your cleaning choices.

## Technical requisites
Throughout this assignent you'll use multiple pandas functionalities such as:
- drop useless columns
- convert string types to numeric or datetime types
- manipulating strings with `.str` (aka the string accessor)
- checking strings for patterns using regular expressions

To learn how to do this in Pandas, you may refer to the following documents:
- ["Pythonic Data Cleaning With pandas and NumPy",  by Malay Agarwal at RealPython.com](https://realpython.com/python-data-cleaning-numpy-pandas/)
- ["Working with text data", at pandas docs](https://pandas.pydata.org/docs/user_guide/text.html#)
- ["Time series / date functionality", at pandas docs](https://pandas.pydata.org/docs/user_guide/timeseries.html)

Some useful examples applied to the data at hand are given in the [examples](./examples) directory.

# Methodology
This assignment will also be an opportunity to practice some test-oriented development.
To build the loading script, you will proceed with the following steps: 
1. Test-oriented problem specification
2. Implementation of the cleaning functions
3. Automatic validation with tests


## Step 1. Test-oriented problem specification
Prepare some tests cases that ensure that your cleaning functions work as expected:

1. Select a handful of examples (between 10 and 15) from your data that cover the problems you have identified. Based on these examples, compose a sample dirty data file (`sample_dirty.csv`) that will serve as a testing reference. **This is done by the teacher and you can see the results in the `data` directory.**
3. Manually compose a `pd.DataFrame` that corresponds to the `sample_dirty` data loaded with the correct types/formats (`sample_formatted`).
   Only the pertinent columns should be loaded by this function (any useless columns must not be read). **This is already specified in `loader_test.py`.**
4. Manually compose a `pd.DataFrame` where all the sanity problems got fixed (`sample_sanitized`). **This is already specified in `loader_test.py`.**
5. Manually compose a `pd.DataFrame` framed as requested (`sample_framed`). Column renaming or merging should be performed at this step. **You must complete this case in `loader_test.py`**.

These test cases should be specified in the file `loader_test.py` where indicated.


## Step 2. Implementation of the cleaning functions
Your loading function should be in a script `loader.py`. It should be as modular as possible. The current `loader.py` gives an example modularization template:
```python
...

def load_formatted_data(data_fname:str) -> pd.DataFrame:
    """ One function to read csv into a dataframe with appropriate types/formats.
        Note: read only pertinent columns, ignore the others.
    """
    ...
    return df

def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function to do all sanitizing"""
    ...
    return df

def frame_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function all framing (column renaming, column merge)"""
    ...
    return df


def load_clean_data(data_path:str=DATA_PATH)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (load_formatted_data(data_path)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    return df

...
```
## Step 3. Automatic validation with tests
Since the problem is defined in terms of test cases, verifying the task is completed consists in passing each of the test cases.
The `loader_test.py` file contains testing functions that check each of them (assuming you implemented all the test cases). You can run them using `pytest`.

