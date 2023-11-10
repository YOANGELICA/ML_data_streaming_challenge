import country_converter as coco
import pandas as pd


def get_continent(country_name):
    cc = coco.CountryConverter()
    try:
        continent = cc.convert(names=country_name, to='continent')
        return continent
    except:
        return None
    

def del_cols(df):
    df.drop(columns=['Country', 'Happiness Rank'], inplace=True)
    return df

def dummies(df):
    df = pd.get_dummies(df, columns=['Continent'], prefix='Continent')
    return df
