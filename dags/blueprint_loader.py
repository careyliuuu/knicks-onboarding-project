import os
import yaml
import sys
from glob import glob

# Add templates to path
sys.path.append("/usr/local/airflow/dags/blueprints/templates")
from blueprints.templates.nba_prediction import NBAPredictionBlueprint

CONFIGS_DIR = "/usr/local/airflow/dags/blueprints/instances"

for config_file in glob(os.path.join(CONFIGS_DIR, "*.yaml")):
    with open(config_file, "r") as f:
        config_data = yaml.safe_load(f)
        
    dag_id = os.path.splitext(os.path.basename(config_file))[0] + "_pipeline"
    
    # 1. Create the instance (Empty)
    blueprint_instance = NBAPredictionBlueprint()
    
    # 2. Manually assign the values from the YAML
    blueprint_instance.team_name = config_data['team_name']
    blueprint_instance.team_id = config_data['team_id']
    blueprint_instance.schedule_cron = config_data.get('schedule_cron', '@daily')
    
    # Register DAG
    globals()[dag_id] = blueprint_instance.render(dag_id=dag_id)