from blueprint import Blueprint
from airflow import DAG  # <--- 1. Import the standard DAG class
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Asset
from airflow.datasets import Asset  # or from airflow import Dataset
from datetime import datetime
import pandas as pd
import json
import os
from openai import OpenAI

class NBAPredictionBlueprint(Blueprint):
    """
    Defines the layout for an NBA Prediction DAG.
    Variables like 'team_name' are injected from YAML.
    """
    team_name: str
    team_id: int
    schedule_cron: str = "@daily" 

    def render(self, dag_id: str):
        
        # Define the Asset dynamically
        team_asset = Asset(f"redshift://{self.team_name.lower()}_predictions")

        with DAG(
            dag_id=dag_id,
            start_date=datetime(2024, 1, 1),
            schedule=self.schedule_cron,
            catchup=False,
            tags=['blueprint', self.team_name, 'nba']
        ) as dag:
            
            # A. Table Setup
            create_table = SQLExecuteQueryOperator(
                task_id="ensure_table_exists",
                conn_id='redshift_default',
                sql="""
                CREATE TABLE IF NOT EXISTS nba_predictions (
                    team VARCHAR(50),
                    game_date VARCHAR(50),
                    opponent VARCHAR(100),
                    prediction VARCHAR(65535),
                    confidence_score INT,
                    reasoning VARCHAR(65535),
                    created_at TIMESTAMP DEFAULT GETDATE()
                );
                """
            )

            # B. Ingest Data
            @task(task_id="fetch_recent_stats")
            def fetch_recent_stats(t_id=self.team_id):
                file_path = "include/Games.csv"
                if not os.path.exists(file_path):
                    raise FileNotFoundError("Games.csv not found!")
                
                df = pd.read_csv(file_path, low_memory=False)
                
                # Filter for this specific team
                team_games = df[
                    (df['hometeamId'] == t_id) | 
                    (df['awayteamId'] == t_id)
                ].sort_values('gameDateTimeEst', ascending=True)
                
                
                team_games = team_games.astype(object).where(pd.notnull(team_games), None) # Replace 'nan' (which crashes JSON) with None (which becomes null)

                return team_games.tail(5).to_dict(orient="records")

            # C. Groq Prediction
            @task(task_id="generate_prediction")
            def generate_prediction(recent_stats_list, team_name=self.team_name):
                df_context = pd.DataFrame(recent_stats_list)
                stats_table = df_context.to_markdown(index=False)
                
                system_msg = f"You are a sharp {team_name} analyst."
                prompt = f"""
                Analyze the {team_name}'s last 5 games:
                {stats_table}
                
                Predict if they will WIN or LOSE the next game.
                Return JSON: {{ "prediction": "Win/Loss", "confidence_score": 0-100, "key_factor": "reasoning" }}
                """

                # Standard OpenAI Client pointed at Groq
                client = OpenAI(
                    base_url="https://api.groq.com/openai/v1",
                    api_key=os.environ.get("GROQ_API_KEY")
                )

                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": prompt}
                    ]
                )
                return response.choices[0].message.content

            # D. Parse JSON 
            @task(task_id="parse_response")
            def parse_response(response_text):
                try:
                    # 1. Clean the string
                    clean_text = response_text.strip()
                    
                    # 2. Remove markdown code blocks if they exist
                    if "```" in clean_text:
                        # Extract everything between the first ```json and the last ```
                        import re
                        match = re.search(r"```(?:json)?\s*(.*)\s*```", clean_text, re.DOTALL)
                        if match:
                            clean_text = match.group(1)
                    
                    # 3. Parse
                    return json.loads(clean_text)
                except Exception as e:
                    print(f"FAILED TO PARSE: {response_text}") # Print the raw text to logs for debugging
                    return {"prediction": "Error", "confidence_score": 0, "key_factor": f"Parse Failed: {str(e)}"}

            # E. Human Approval
            human_check = ApprovalOperator(
                task_id="approve_prediction",
                subject=f"Approve {self.team_name} Prediction",
                body="AI Output: {{ task_instance.xcom_pull(task_ids='parse_response') }}",
                defaults=["Approve"],
            )

            # F. Load to Redshift
            save_to_db = SQLExecuteQueryOperator(
                task_id="publish_to_redshift",
                conn_id='redshift_default',
               outlets=[Asset(name="knicks_prediction", uri="redshift://knicks_predictions/")],
                sql="""
                    INSERT INTO nba_predictions (team, game_date, opponent, prediction, confidence_score, reasoning)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """,
                parameters=[
                    self.team_name,
                    "{{ task_instance.xcom_pull(task_ids='fetch_recent_stats')[-1]['gameDateTimeEst'] }}",
                    "Next Opponent", 
                    "{{ task_instance.xcom_pull(task_ids='parse_response')['prediction'] }}",
                    "{{ task_instance.xcom_pull(task_ids='parse_response')['confidence_score'] }}",
                    "{{ task_instance.xcom_pull(task_ids='parse_response')['key_factor'] }}"
                ]
            )

            # Flow
            stats = fetch_recent_stats()
            raw_pred = generate_prediction(stats)
            parsed_pred = parse_response(raw_pred)
            create_table >> stats >> raw_pred >> parsed_pred >> human_check >> save_to_db
        
        # 3. CRITICAL: Return the DAG object so the loader can register it!
        return dag