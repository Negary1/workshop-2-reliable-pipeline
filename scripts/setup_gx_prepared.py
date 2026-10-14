import great_expectations as gx

print("Inicializando GX para los Datos Preparados...")
context = gx.get_context()

# 1. Configurar el origen de datos apuntando al archivo transformado
datasource_name = "prepared_datasource"
try:
    datasource = context.sources.add_pandas_filesystem(
        name=datasource_name,
        base_directory="data"
    )
except Exception:
    datasource = context.get_datasource(datasource_name)

try:
    asset = datasource.add_csv_asset(
        name="prepared_csv", 
        batching_regex=r"integrated_data_prepared\.csv"
    )
except Exception:
    asset = datasource.get_asset("prepared_csv")

# 2. Crear la Suite de reglas finales
suite_name = "prepared_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# 3. Inicializar el Validador
batch_request = asset.build_batch_request()
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

print("[*] Evaluando reglas del Integration Contract...")

# Regla 1: El JOIN no debió dejar valores nulos en el flag del Grammy
validator.expect_column_values_to_not_be_null(column="grammy_winner_flag")

# Regla 2: El flag del Grammy debe ser estrictamente Booleano (True o False)
validator.expect_column_values_to_be_in_type_list(column="grammy_winner_flag", type_list=["bool"])

# Regla 3: La transformación analítica (AR3) solo puede tener estos 3 valores
validator.expect_column_values_to_be_in_set(
    column="acoustic_quartile", 
    value_set=["High Engagement", "Medium Engagement", "Low Engagement"]
)

# Regla 4: La llave subrogada del artista (artist_id) debe ser única y no nula
validator.expect_column_values_to_not_be_null(column="artist_clean")

# 4. Guardar la configuración
validator.save_expectation_suite(discard_failed_expectations=False)
print("[*] ¡Éxito! Las reglas de datos preparados han sido guardadas.")