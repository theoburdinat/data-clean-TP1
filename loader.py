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
    raw_dataset = pd.read_csv(data_fname, usecols=['nom', 'adr_num', 'adr_voie', 'com_cp', 'com_nom', 'tel1', 'freq_mnt', 'dermnt', 'lat_coor1', 'long_coor1'], encoding ='utf-8', na_values=['', ' '])
    
    cleaned_dataset = raw_dataset.copy()
    
    cleaned_dataset[['nom', 'adr_num','adr_voie','com_nom','com_cp','tel1','freq_mnt']] = cleaned_dataset[['nom', 'adr_num','adr_voie','com_nom','com_cp','tel1','freq_mnt']].replace(np.NaN, pd.NA)
    cleaned_dataset['dermnt']=cleaned_dataset['dermnt'].replace(np.NaN, pd.NaT)
    cleaned_dataset['dermnt']=pd.to_datetime(cleaned_dataset['dermnt'], errors='coerce', format='%Y-%m-%d')

    for index, row in cleaned_dataset.iterrows():
        if not isinstance(row['nom'], str):
            cleaned_dataset.loc[index, 'nom'] = str(cleaned_dataset.loc[index, 'nom'])
        if not isinstance(row['adr_num'], str):
            cleaned_dataset.loc[index, 'adr_num'] = str(cleaned_dataset.loc[index, 'adr_num'])
        if not isinstance(row['adr_voie'], str):
            cleaned_dataset.loc[index, 'adr_voie'] = str(cleaned_dataset.loc[index, 'adr_voie'])
        if not isinstance(row['com_nom'], str):
            cleaned_dataset.loc[index, 'com_nom'] = str(cleaned_dataset.loc[index, 'com_nom'])
        if not isinstance(row['com_cp'], str):
            cleaned_dataset.loc[index, 'com_cp'] = str(cleaned_dataset.loc[index, 'com_cp'])
        if not isinstance(row['tel1'], str):
            cleaned_dataset.loc[index, 'tel1'] = str(cleaned_dataset.loc[index, 'tel1'])
        if not isinstance(row['freq_mnt'], str):
            cleaned_dataset.loc[index, 'freq_mnt'] = str(cleaned_dataset.loc[index, 'freq_mnt'])
    
    cleaned_dataset['lat_coor1'] = pd.to_numeric(cleaned_dataset['lat_coor1'], errors='coerce')
    cleaned_dataset['long_coor1'] = pd.to_numeric(cleaned_dataset['long_coor1'], errors='coerce')
    cleaned_dataset['dermnt'] = cleaned_dataset['dermnt'].astype(str)
    cleaned_dataset.loc[5, 'dermnt']=str(pd.NaT)
    cleaned_dataset.loc[5, 'freq_mnt']=str(pd.NA)
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