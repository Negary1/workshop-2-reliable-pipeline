import pandas as pd
from sqlalchemy import create_engine, text

def load_data_to_dw():
    print("Iniciando carga al Data Warehouse Dimensional...")
    df = pd.read_csv('data/integrated_data_prepared.csv')
    
    engine = create_engine('mysql+mysqlconnector://root:root@mysql-dw:3306/music_dw')
    
    # Limpieza idempotente: Vaciar tablas antes de cargar
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text("TRUNCATE TABLE fact_track_performance;"))
        conn.execute(text("TRUNCATE TABLE dim_tracks;"))
        conn.execute(text("TRUNCATE TABLE dim_artists;"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
    
    # 1. Carga de Artistas
    dim_artists = df[['artist_id', 'artist_name', 'grammy_winner_flag']].drop_duplicates()
    dim_artists.to_sql('dim_artists', con=engine, if_exists='append', index=False)
    
    # 2. Carga de Pistas (Defensiva)
    dim_tracks = df[['track_id', 'track_name', 'acoustic_quartile', 'danceability', 'valence', 'duration_ms']].drop_duplicates()
    
    # REGLA DEFENSIVA: Truncar a 255 caracteres para encajar en el esquema actual de MySQL
    dim_tracks['track_name'] = dim_tracks['track_name'].astype(str).str[:255]
    
    dim_tracks.to_sql('dim_tracks', con=engine, if_exists='append', index=False)
    
    # 3. Carga de Hechos
    fact_performance = df[['track_id', 'artist_id', 'popularity_score']]
    fact_performance.to_sql('fact_track_performance', con=engine, if_exists='append', index=False)
    
    print("Carga dimensional completada exitosamente.")