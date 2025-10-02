# 1. Importamos las herramientas necesarias
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# 2. Definimos el DAG (la "receta")
with DAG(
    dag_id="mi_primer_dag", # El nombre único de nuestro DAG
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), # Cuándo empezó a ser válido
    schedule=None, # Lo ejecutaremos manualmente
    catchup=False, # No intentes ejecutar tareas pasadas que se hayan perdido
    tags=["tutorial"], # Etiquetas para organizar los DAGs
    max_active_runs=1,
) as dag:
    # 3. Definimos las Tareas (los "pasos de la receta")
    
    # Tarea 1: Un operador que ejecuta un comando de consola
    tarea_de_inicio = BashOperator(
        task_id="tarea_de_inicio", # Nombre único de la tarea
        bash_command="echo '¡Mi primer DAG ha comenzado!'",
    )

    # Tarea 2: Otra tarea que ejecuta otro comando
    tarea_final = BashOperator(
        task_id="tarea_final",
        bash_command="echo '¡El DAG ha terminado con éxito!'",
    )

    # 4. Definimos el orden (la "secuencia de pasos")
    tarea_de_inicio >> tarea_final