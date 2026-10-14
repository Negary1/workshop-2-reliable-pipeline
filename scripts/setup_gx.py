import great_expectations as gx

print("Inicializando configuración de Great Expectations...")

# 1. Obtener el contexto de GX
context = gx.get_context()

# 2. Configurar el origen de datos (Datasource) apuntando a la carpeta data/
datasource_name = "spotify_datasource"
try:
    datasource = context.sources.add_pandas_filesystem(
        name=datasource_name,
        base_directory="data"
    )
    print(f"[*] Datasource '{datasource_name}' creado.")
except Exception:
    print(f"[*] El datasource ya existía. Obteniendo contexto...")
    datasource = context.get_datasource(datasource_name)

# 3. Conectar tu archivo CSV específico usando una expresión regular
try:
    asset = datasource.add_csv_asset(
        name="spotify_csv", 
        batching_regex=r"spotify\.csv"
    )
    print(f"[*] Archivo spotify.csv enlazado.")
except Exception:
    asset = datasource.get_asset("spotify_csv")

# 4. Crear la Suite de reglas
suite_name = "spotify_raw_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
print(f"[*] Suite '{suite_name}' creada.")

# 5. Inicializar el Validador
batch_request = asset.build_batch_request()
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

# 6. Aplicar reglas
print("[*] Evaluando reglas de calidad...")
validator.expect_column_values_to_not_be_null(column="popularity")
validator.expect_column_values_to_be_between(column="popularity", min_value=0, max_value=100)
validator.expect_column_values_to_not_be_null(column="danceability")
validator.expect_column_values_to_not_be_null(column="valence")

# 7. Guardar la configuración
validator.save_expectation_suite(discard_failed_expectations=False)
print(f"[*] ¡Éxito! Las reglas han sido guardadas en la carpeta gx/.")
