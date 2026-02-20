import os
import yaml
import sys
from glob import glob
from pathlib import Path

# 1. Dynamically find the path to the dags/ folder based on THIS file's location
dags_folder = Path(__file__).parent.resolve()

# 2. Explicitly add the templates folder to the system path
templates_folder = dags_folder / "blueprints" / "templates"
sys.path.append(str(templates_folder))

# 3. Import the template (since its folder is in the path, we just call the file name)
from nba_prediction import NBAPredictionBlueprint

# 4. Dynamically point to the instances folder
CONFIGS_DIR = str(dags_folder / "blueprints" / "instances")

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