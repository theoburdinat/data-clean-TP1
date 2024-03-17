import os
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
        Comment on sanitize : 
        - pas d'adresse -> on enleve num voie (fait)
        - Formatter montpellier (fait)
        - Formatter num tél (fait)
        - Formatter fréquence de maintenance 
        - Code postal si pas correct (5 chiffres) -> mettre pd.NA (fait)
        - Vérifier number of units for lat_coor et long_coor """
    
    #Code Postal
    for i in range(len(df)):
        if len(df[i].com_cp) != 5:
            df[i].com_cp=pd.NA
        if df[i].adr_voie==pd.NA: #pas d'adresse on enleve num voie
            df[i].adr_num=pd.NA
        if df[i].freq_mnt!="": #formatter maintenance
            df[i].freq_mnt='Tous les ans'
    #Formater Montpellier
    df['com_nom'] = df['com_nom'].str.capitalize()

    #Numéro de tel
    df['tel1'] = df['tel1'].str.replace(r'^(\+?33|0)\s*', r'+33 ', regex=True)
    df['tel1'] = df['tel1'].str.replace(r'(\d{1})(\d{2})(\d{2})(\d{2})(\d{2})', r'\1 \2 \3 \4 \5', regex=True)

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