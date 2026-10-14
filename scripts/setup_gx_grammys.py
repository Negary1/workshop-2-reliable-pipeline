import great_expectations as gx

print("Conectando GX a MySQL (Grammys)...")
context = gx.get_context()

# 1. Configurar la conexión a tu contenedor Docker (puerto 3306)
connection_string = "mysql+mysqlconnector://root:root@localhost:3306/grammy_source"
datasource_name = "grammy_mysql"

try:
    datasource = context.sources.add_sql(
        name=datasource_name, 
        connection_string=connection_string
    )
    print(f"[*] Conexión '{datasource_name}' establecida.")
except Exception:
    datasource = context.get_datasource(datasource_name)
    print("[*] Conexión recuperada.")

# 2. Conectar la tabla específica que creamos con source_setup.sql
try:
    asset = datasource.add_table_asset(name="grammy_table", table_name="grammy_awards")
except Exception:
    asset = datasource.get_asset("grammy_table")

# 3. Crear la Suite de reglas para los Grammys
suite_name = "grammys_raw_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# 4. Inicializar el Validador
batch_request = asset.build_batch_request()
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

print("[*] Evaluando reglas de calidad para Grammys...")

# Regla 1: Validar nuestra hipótesis del perfilado (la columna winner no tiene nulos)
validator.expect_column_values_to_not_be_null(column="winner")

# Regla 2: Validar el sesgo de selección (winner siempre es 'True')
validator.expect_column_values_to_be_in_set(column="winner", value_set=["True", "true", "1", 1, True])

# Regla 3: El nombre del artista no puede ser nulo para asegurar el cruce (Integration Contract)
validator.expect_column_values_to_not_be_null(column="artist")

# 5. Guardar la configuración
validator.save_expectation_suite(discard_failed_expectations=False)
print("[*] ¡Éxito! Las reglas de los Grammys han sido guardadas.")