from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Asegurar que Airflow encuentre la carpeta src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.transform import transform_and_integrate_data
from src.extract import extract_grammys_from_db, extract_spotify_csv
from src.load import load_data_to_dw

default_args = {
    'owner': 'engineering_student',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'reliable_music_pipeline',
    default_args=default_args,
    description='Batch pipeline for Spotify and Grammys with GX quality gates',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # --- 1. FASE DE EXTRACCIÓN ---
    extract_spotify = PythonOperator(
        task_id='extract_spotify',
        python_callable=extract_spotify_csv,
    )

    extract_grammys = PythonOperator(
        task_id='extract_grammys',
        python_callable=extract_grammys_from_db,
    )

    # --- 2. FASE DE VALIDACIÓN CRUDA ---
    validate_spotify_raw = BashOperator(
        task_id='validate_spotify_raw',
        bash_command='cd /opt/airflow && python scripts/test_checkpoint.py',
    )

    validate_grammys_raw = BashOperator(
        task_id='validate_grammys_raw',
        bash_command='cd /opt/airflow && python scripts/test_checkpoint_grammys.py',
    )

    # --- 3. FASE DE TRANSFORMACIÓN ---
    def run_transformation():
        # ¡ATENCIÓN! Ahora usamos el CSV extraído desde la BD, no el archivo original
        transform_and_integrate_data('data/spotify.csv', 'data/grammy_extracted.csv')

    transform_and_integrate = PythonOperator(
        task_id='transform_and_integrate',
        python_callable=run_transformation,
    )

    # --- 4. FASE DE VALIDACIÓN PREPARADA ---
    validate_prepared = BashOperator(
        task_id='validate_prepared',
        bash_command='cd /opt/airflow && python scripts/test_checkpoint_prepared.py',
    )

    # --- 5. FASE DE CARGA AL DW ---
    load_to_dw = PythonOperator(
        task_id='load_dw',
        python_callable=load_data_to_dw,
    )

    # --- DEPENDENCIAS (ARQUITECTURA DEL TALLER) ---
    extract_spotify >> validate_spotify_raw
    extract_grammys >> validate_grammys_raw
    
    [validate_spotify_raw, validate_grammys_raw] >> transform_and_integrate >> validate_prepared >> load_to_dw