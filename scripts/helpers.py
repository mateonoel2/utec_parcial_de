from datetime import datetime
import os
import pandas as pd

def add_date_suffix(base_name, date=None):
    import os
    if date is None:
        date = datetime.now()
    date_str = date.strftime("%Y-%m-%d")
    root, ext = os.path.splitext(base_name)
    return f"{root}{date_str}{ext}"


def leer_datos(path):
    return pd.read_csv(path)

def transformar_datos(df):
    df['total'] = df['cantidad'] * df['precio']
    return df

def resumir_datos(df):
    resumen = df.groupby('producto')['total'].sum().reset_index()
    return resumen

def guardar_datos(df, path):
    df.to_csv(path, index=False)
