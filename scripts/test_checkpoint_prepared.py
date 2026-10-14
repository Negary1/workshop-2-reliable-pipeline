import great_expectations as gx

context = gx.get_context()

# 1. Configurar el Checkpoint apuntando al archivo cruzado final
checkpoint_name = "prepared_checkpoint"
checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1,
    "class_name": "Checkpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "prepared_datasource",
                "data_asset_name": "prepared_csv",
            },
            "expectation_suite_name": "prepared_suite",
        }
    ],
}

context.add_or_update_checkpoint(**checkpoint_config)

# 2. Ejecutar la validación
print("Ejecutando la validación final de Datos Preparados...")
result = context.run_checkpoint(checkpoint_name=checkpoint_name)

# 3. Abrir el reporte visual (Data Docs)
if result.success:
    print("¡ÉXITO TOTAL! Los datos cruzados pasaron todas las reglas.")
else:
    print("Advertencia: Algunas reglas fallaron. Revisa el reporte.")

context.build_data_docs()
context.open_data_docs()