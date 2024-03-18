import os
import re
import requests
import numpy as np
import pandas as pd

DATA_PATH = 'data/MMM_MMM_DAE.csv'

def download_data(url = DATA_PATH, force_download=False):
    # Utility function to donwload data if it is not in disk
    data_path = os.path.join('data', os.path.basename(url.split('?')[0]))
    if not os.path.exists(data_path) or force_download:
        # ensure data dir is created
        os.makedirs('data', exist_ok=True)
        # request data from url
        response = requests.get(url, allow_redirects=True)
        # save file
        with open(data_path, "w") as f:
            # Note the content of the response is in binary form: 
            # it needs to be decoded.
            # The response object also contains info about the encoding format
            # which we use as argument for the decoding
            f.write(response.content.decode(response.apparent_encoding))

    return data_path


def load_formatted_data(data_fname:str) -> pd.DataFrame:
    """ One function to read csv into a dataframe with appropriate types/formats.
        Note: read only pertinent columns, ignore the others.
    """
    # Load the dataset
    raw_dataset = pd.read_csv(data_fname, usecols=['nom', 'adr_num', 'adr_voie', 'com_cp', 'com_nom', 'tel1', 'freq_mnt', 'dermnt', 'lat_coor1', 'long_coor1'], encoding ='utf-8')
    
    # Copy the loaded dataset to keep the original one 
    cleaned_dataset = raw_dataset.copy()
    
    # Replace all empty strings with null values
    cleaned_dataset = cleaned_dataset.replace([' '], pd.NA)

    # Convert all string columns to string
    cleaned_dataset[['nom', 'adr_num', 'adr_voie', 'com_cp', 'com_nom', 'tel1', 'freq_mnt']] = cleaned_dataset[['nom', 'adr_num', 'adr_voie', 'com_cp', 'com_nom', 'tel1', 'freq_mnt']].astype('string')

    # Convert the last maintenance to the datetime format, and replace values ​​that are not convertible to null values
    cleaned_dataset['dermnt']=pd.to_datetime(cleaned_dataset['dermnt'], errors='coerce', format='%Y-%m-%d')

    # Convert values that have a date format in maintenance frequency to null values
    cleaned_dataset['freq_mnt'] = cleaned_dataset['freq_mnt'].where(pd.to_datetime(cleaned_dataset['freq_mnt'], errors='coerce').isna(), pd.NA)

    # Convert latitude and longitude data to string, and if it's not possible set null values
    cleaned_dataset['lat_coor1'] = pd.to_numeric(cleaned_dataset['lat_coor1'], errors='coerce')
    cleaned_dataset['long_coor1'] = pd.to_numeric(cleaned_dataset['long_coor1'], errors='coerce')

    return cleaned_dataset


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function to do all sanitizing
    """
    
    # Address number
    df.loc[df['adr_num'] == '-', 'adr_num'] = pd.NA
    for i in range(len(df)):
        if pd.notna(df.loc[i, 'adr_num']):
            # Delete space around -
            if re.match(r'^\d+\s*-\s*\d+$', df.loc[i, 'adr_num']):
                df.loc[i, 'adr_num'] = re.sub(r'\s*-\s*', '-', df.loc[i, 'adr_num'])
            # Delete unwanted characters
            elif re.match(r'^(\d+)(?!\s*bis)\D+', df.loc[i, 'adr_num']):
                df.loc[i, 'adr_num'] = re.sub(r'^(\d+)(?!\s*bis)\D+', r'\1', df.loc[i, 'adr_num'])

    # Name of the street
    df.loc[df['adr_voie'] == '-', 'adr_voie'] = pd.NA
    for i in range(len(df)):
        if pd.notna(df.loc[i, 'adr_voie']):
            # If there is the total adress in this field, we remove unnecassary stuff (from the zip code)
            if re.match(r'.*\b\d{5}\b.*', df.loc[i, 'adr_voie']):
                df.loc[i, 'adr_voie'] = re.sub(r'\s*\b\d{5}\b.*', '', df.loc[i, 'adr_voie'])
            # If there is more than two spaces, we only put one
            if re.match(r'.*\s{2,}.*', df.loc[i, 'adr_voie']):
                df.loc[i, 'adr_voie'] = re.sub(r'\s{2,}', ' ', df.loc[i, 'adr_voie'])
            # If there is the number of the street, we delete it
            if re.match(r'^\d+\s+.*', df.loc[i, 'adr_voie']):
                df.loc[i, 'adr_voie'] = re.sub(r'^\d+\s+', '', df.loc[i, 'adr_voie'])
            # We delete all stuff after commas
            if re.match(r'.*,.*', df.loc[i, 'adr_voie']):
                df.loc[i, 'adr_voie'] = re.sub(r',.*', '', df.loc[i, 'adr_voie'])
            # We have to put caps on the last word
            words = df.loc[i, 'adr_voie'].split()
            words[-1] = words[-1].title()
            df.loc[i, 'adr_voie'] = ' '.join(words)

    # ZIP code
    df.loc[df['com_cp'] == '0', 'com_cp'] = pd.NA

    # City name
    # We have to put the last word with a cap on the first letter, and the rest in small letters
    df['com_nom'] = df['com_nom'].str.capitalize()

    # Phone number
    df.loc[df['tel1'] == '-', 'tel1'] = pd.NA
    for i in range(len(df)):
        # Phone number format
        if pd.notna(df.loc[i, 'tel1']):
            digits = re.findall(r'([^3])\s*(\d{2})\s*(\d{2})\s*(\d{2})\s*(\d{2})', df.loc[i, 'tel1'])
            if digits:
                df.loc[i, 'tel1'] = "+33 {} {} {} {} {}".format(*digits[0])

    # Maintenance frequency
    df['freq_mnt'] = df['freq_mnt'].str.capitalize()  
    for i in range(len(df)):
        # Spelling mistakes correction
        if pd.notna(df.loc[i, 'freq_mnt']):
            if re.match(r'Tout.*', df.loc[i, 'freq_mnt']):
                df.loc[i, 'freq_mnt'] = re.sub(r'Tout', 'Tous', df.loc[i, 'freq_mnt'])

    return df


# Define a framing function
def frame_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function all framing (column renaming, column merge)"""
    df['adr_num'] = df['adr_num '] + df['adr_voie '] + df['com_cp '] + df['com_nom ']
    df.rename(columns={'adr_num':'adress'})
    df.rename(columns={'long_coor1':'long'})
    df.rename(columns={'lat_coor1':'lat'})
    del df['adr_voie']
    del df['com_cp']
    del df['com_nom']
    return df


# once they are all done, call them in the general clean loading function
def load_clean_data(data_path:str=DATA_PATH)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (load_formatted_data(data_path)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    return df


# if the module is called, run the main loading function
if __name__ == '__main__':
    load_clean_data(download_data())