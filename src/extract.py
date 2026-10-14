import pandas as pd
from sqlalchemy import create_engine
import os

def extract_grammys_from_db():
    print("Iniciando extracción desde la base de datos relacional...")
    # Ajusta el usuario, contraseña, puerto y nombre de la BD origen si son distintos
    engine = create_engine('mysql+mysqlconnector://root:root@mysql-source:3306/grammy_source')
    
    # Extraemos la tabla operativa (asumiendo que se llama grammy_awards)
    df_grammys = pd.read_sql("SELECT * FROM grammy_awards", engine)
    
    # Lo guardamos como CSV temporal (este será el archivo que lea GX y Transform)
    df_grammys.to_csv('data/grammy_extracted.csv', index=False)
    print(f"Extracción completada. {len(df_grammys)} filas extraídas.")

def extract_spotify_csv():
    # Como Spotify ya viene en CSV por requerimiento del taller, solo verificamos su existencia
    if not os.path.exists('data/spotify.csv'):
        raise FileNotFoundError("El archivo fuente data/spotify.csv no existe.")
    print("Archivo fuente de Spotify validado.")