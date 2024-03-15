import os
import requests
import numpy as np
import pandas as pd

DATA_PATH = 'data/MMM_MMM_DAE.csv'

def download_data(url, force_download=False, ):
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
    df = pd.read_csv(
        data_fname,
        usecols=['nom', 'adr_num', 'adr_voie', 'com_cp', 'com_nom', 'tel1', 'freq_mnt', 'dermnt', 'lat_coor1', 'long_coor1'],
        dtype={'nom':str, 'adr_num':str, 'adr_voie':str, 'com_cp':str, 'com_nom':str, 'tel1':str, 'freq_mnt':str, 'dermnt':pd.Timestamp, 'lat_coor1':np.float64, 'long_coor1':np.float64})
    return df


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