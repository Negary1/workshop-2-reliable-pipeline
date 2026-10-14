import great_expectations as gx

context = gx.get_context()

# 1. Configurar el Checkpoint apuntando a MySQL y a la suite de Grammys
checkpoint_name = "grammys_raw_checkpoint"
checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1,
    "class_name": "Checkpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "grammy_mysql",
                "data_asset_name": "grammy_table",
            },
            "expectation_suite_name": "grammys_raw_suite",
        }
    ],
}

context.add_or_update_checkpoint(**checkpoint_config)

# 2. Ejecutar la validación directamente contra la base de datos
print("Ejecutando la validación cruda de Grammys en MySQL...")
result = context.run_checkpoint(checkpoint_name=checkpoint_name)

# 3. Generar y abrir el reporte visual (Data Docs)
if result.success:
    print("¡Todos los datos pasaron las compuertas de calidad!")
else:
    print("Advertencia: Algunas reglas fallaron. Revisa el reporte.")

context.build_data_docs()
context.open_data_docs()