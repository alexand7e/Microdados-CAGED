from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

from caged_data_processor import CagedProcessor

def process_caged_data(ds, **kwargs):
    local_path = os.getcwd()
    paths = {
        'bronze': os.path.join(local_path, 'data', 'bronze'),
        'silver': os.path.join(local_path, 'data', 'silver'),
        'gold': os.path.join(local_path, 'data', 'gold')
    }

    caged = CagedProcessor(microdata_directory=paths.get('bronze'),
                           processed_files_directory=paths.get('silver'),
                           current_month=kwargs['execution_date'].month,
                           current_year=kwargs['execution_date'].year,
                           download_microdata=True,
                           insert_processed_table=True)

    caged_microdados_full, caged_agrupamentos = caged.processar_caged_por_mes_ano(get_categoria=True)
    caged_formatado_completo = caged.consolidar_caged(caged_agrupamentos)

    try:
        caged_formatado_completo["salarios"] = caged.analisar_salarios_aprimorado(caged_microdados_full, ["Setores"])
        caged_formatado_completo["Setor - Município"] = caged.get_maior_setor_por_municipio(caged_microdados_full)
    except Exception as e:
        print(f'Erro {e}')

    caged.salvar_arquivos(saving_dir=paths.get('gold'), dicionario_df=caged_formatado_completo, suffix_file="CAGED")

    df_detalhamento = caged.analises_combinadas(caged_microdados_full, grupos=["Setores"])
    df_detalhamento.to_excel(os.path.join(paths.get('gold'), 'Setores.xlsx'))

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'caged_data_processing',
    default_args=default_args,
    description='Process CAGED data monthly',
    schedule_interval='0 0 1 * *',  # Runs on the first day of every month
    catchup=False
)

# Set up the task
process_caged_data_task = PythonOperator(
    task_id='process_caged_data',
    python_callable=process_caged_data,
    provide_context=True,
    dag=dag
)

# Task Pipeline
process_caged_data_task
