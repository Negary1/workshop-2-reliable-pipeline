import pandas as pd
import numpy as np

def transform_and_integrate_data(spotify_path: str, grammys_path: str) -> str:
    print("Iniciando fase de Transformación e Integración...")

    # 1. Cargar los datos crudos extraídos
    df_spotify = pd.read_csv(spotify_path)
    df_grammys = pd.read_csv(grammys_path) 

    # 2. Transformación de Spotify: Limpieza de la columna 'artists'
    df_spotify = df_spotify.dropna(subset=['artists'])
    
    # Renombrar popularity para alinear con el Data Warehouse
    if 'popularity' in df_spotify.columns:
        df_spotify = df_spotify.rename(columns={'popularity': 'popularity_score'})

    # Quitar corchetes/comillas y separar estrictamente por PUNTO Y COMA (;)
    df_spotify['artist_clean'] = df_spotify['artists'].str.replace(r"[\[\]']", "", regex=True)
    df_spotify['artist_clean'] = df_spotify['artist_clean'].str.split(';')
    
    # Explotar la lista para granularidad de artista
    df_spotify_exploded = df_spotify.explode('artist_clean')
    df_spotify_exploded['artist_clean'] = df_spotify_exploded['artist_clean'].str.strip().str.lower()

    # 3. Transformación de Grammys: Limpieza para el JOIN
    # Filtrar nulos masivos de premios técnicos ANTES de manipular el string
    df_grammys = df_grammys.dropna(subset=['artist'])
    
    df_grammys_clean = df_grammys[['artist', 'winner']].copy()
    df_grammys_clean['artist_clean'] = df_grammys_clean['artist'].str.strip().str.lower()
    
    # Deduplicar artistas ganadores
    df_grammys_winners = df_grammys_clean[df_grammys_clean['winner'] == True]
    df_grammys_winners = df_grammys_winners[['artist_clean']].drop_duplicates()
    df_grammys_winners['grammy_winner_flag'] = True

    # 4. INTEGRATION CONTRACT (El Cruce Left Join)
    df_integrated = pd.merge(
        df_spotify_exploded, 
        df_grammys_winners, 
        on='artist_clean', 
        how='left'
    )
    
    # Imputar control group (No ganadores = False)
    df_integrated['grammy_winner_flag'] = df_integrated['grammy_winner_flag'].fillna(False)

    # 5. Transformación Analítica (AR3): Cuartiles Acústicos
    condiciones = [
        (df_integrated['danceability'] >= 0.5) & (df_integrated['valence'] >= 0.5),
        (df_integrated['danceability'] < 0.5) & (df_integrated['valence'] < 0.5)
    ]
    valores = ['High Engagement', 'Low Engagement']
    df_integrated['acoustic_quartile'] = np.select(condiciones, valores, default='Medium Engagement')

    # 6. Preparar Llaves Dimensionales (Surrogate Keys)
    # DIMENSIÓN ARTISTAS
    dim_artists = df_integrated[['artist_clean', 'grammy_winner_flag']].drop_duplicates()
    dim_artists = dim_artists.rename(columns={'artist_clean': 'artist_name'})
    dim_artists['artist_id'] = [f"A{i}" for i in range(1, len(dim_artists) + 1)] # Generar surrogate key alfanumérica

    # Cruzamos para integrar el artist_id generado a nuestra sábana principal
    df_integrated = pd.merge(
        df_integrated, 
        dim_artists[['artist_name', 'artist_id']], 
        left_on='artist_clean', 
        right_on='artist_name', 
        how='left'
    )

    # 7. Guardar la sábana desnormalizada temporalmente para GX y load.py
    output_path = "data/integrated_data_prepared.csv"
    
    # Seleccionamos estrictamente las columnas que necesita el DW
    columnas_dw = [
        'track_id', 'artist_id', 'artist_name', 'grammy_winner_flag', 
        'track_name', 'acoustic_quartile', 'danceability', 'valence', 
        'duration_ms', 'popularity_score'
    ]
    
    df_final = df_integrated[columnas_dw]
    df_final.to_csv(output_path, index=False)
    print(f"[*] Transformación completa. Datos preparados guardados en {output_path}")
    
    return output_path