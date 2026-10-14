import sys
import great_expectations as gx

context = gx.get_context()

# 1. Obtener la suite para verificar qué expectativas tiene guardadas
suite = context.get_expectation_suite("spotify_raw_suite")
print("=== DIAGNÓSTICO DE LA SUITE ===")
print(f"Nombre de la suite: {suite.expectation_suite_name}")
print(f"Número de expectativas configuradas: {len(suite.expectations)}")
for exp in suite.expectations:
    print(f" - Regla activa: {exp.expectation_type} en columna '{exp.kwargs.get('column')}'")

# 2. Configurar el Checkpoint
checkpoint_name = "spotify_raw_checkpoint"
checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1,
    "class_name": "Checkpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "spotify_datasource",
                "data_asset_name": "spotify_csv",
            },
            "expectation_suite_name": "spotify_raw_suite",
        }
    ],
}
context.add_or_update_checkpoint(**checkpoint_config)

# 3. Ejecutar la validación
print("\nEjecutando la validación cruda de Spotify...")
result = context.run_checkpoint(checkpoint_name=checkpoint_name)

# 4. Imprimir el resultado detallado de las reglas
print("\n=== RESULTADO DE VALIDACIÓN DETALLADO ===")
print(f"¿Validación global exitosa? -> {result.success}")

for validation in result.list_validation_results():
    for res in validation["results"]:
        exp_type = res["expectation_config"]["expectation_type"]
        col = res["expectation_config"]["kwargs"].get("column")
        success = res["success"]
        print(f"Regla: {exp_type} | Columna: {col} | ¿Pasó?: {success}")
        if not success:
            print(f"   -> DETALLE DEL ERROR: {res['result']}")

# 5. Circuit-breaker
if result.success:
    print("\n✅ La validación retornó ÉXITO.")
    sys.exit(0)
else:
    print("\n❌ La validación RETORNÓ FALLO. Deteniendo pipeline...")
    sys.exit(1)