from src.transform import transform_and_integrate_data

# Rutas simuladas que Airflow le pasaría a la función
spotify_validado = "data/spotify.csv"
grammys_validado = "data/grammy.csv"

# Ejecutar la transformación
resultado = transform_and_integrate_data(spotify_validado, grammys_validado)