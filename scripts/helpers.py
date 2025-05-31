from datetime import datetime
import os
import pandas as pd

def add_date_suffix(base_path: str) -> str:
    """
    Add date suffix to a base path for file versioning
    """
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_path.rstrip('/')}/{current_date}"

def ensure_directory_exists(file_path: str) -> None:
    """
    Ensure that the directory for a file path exists
    """
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes
    """
    if os.path.exists(file_path):
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 * 1024)
    return 0.0


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
