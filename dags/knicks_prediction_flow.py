from airflow.decorators import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator 
from airflow.sdk import Asset
from datetime import datetime
from dags.assets import KNICKS_ASSET
import pandas as pd
import os

# Config
DB_CONN_ID = 'redshift_default'

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=['knicks', 'enterprise', 'groq'],
    doc_md="""
    # Enterprise Knicks Pipeline (Free Tier)
    - **Source**: Kaggle (Games.csv + TeamHistories.csv)
    - **AI**: Groq (Llama 3.3) via Airflow AI SDK
    - **Destination**: Redshift
    """
)
def knicks_enterprise_pipeline():
    
    # 1. Setup Redshift Table (Nuclear Option: Drop & Recreate)
    create_table = SQLExecuteQueryOperator(
        task_id="ensure_table_exists",
        conn_id=DB_CONN_ID,
        split_statements=True,
        sql="""
        -- CASCADE used to fix OID error
        DROP TABLE IF EXISTS knicks_predictions CASCADE;
        
        CREATE TABLE knicks_predictions (
            game_date VARCHAR(50),
            opponent VARCHAR(100),
            recent_stats VARCHAR(65535),
            ai_prediction VARCHAR(65535),
            created_at TIMESTAMP DEFAULT GETDATE()
        );
        """
    )

    # 2. EXTRACT: Read Kaggle CSVs (Fixed: Gets correct name directly from game file)
    @task
    def get_latest_game_context():
        # Define paths
        base_path = "/usr/local/airflow/include"
        games_path = os.path.join(base_path, "Games.csv")           
        
        # Load Data
        df_games = pd.read_csv(games_path, low_memory=False)
        
        # KNICKS ID
        KNICKS_ID = 1610612752
        
        # Filter for Knicks games
        knicks_games = df_games[
            (df_games['hometeamId'] == KNICKS_ID) | 
            (df_games['awayteamId'] == KNICKS_ID)
        ].sort_values('gameDateTimeEst', ascending=False)
        
        if knicks_games.empty:
            raise ValueError("No Knicks games found! Check the CSV file.")

        # Get the most recent game
        last_game = knicks_games.iloc[0]
        game_date = last_game['gameDateTimeEst']
        
        # Determine Opponent & Score directly from the game row
        if last_game['hometeamId'] == KNICKS_ID:
            is_home = True
            points = last_game['homeScore']
            opp_points = last_game['awayScore']
            # Read the name directly from this row.
            opp_name = last_game['awayteamName'] 
        else:
            is_home = False
            points = last_game['awayScore']
            opp_points = last_game['homeScore']
            opp_name = last_game['hometeamName']
            
        return {
            "date": str(game_date),
            "opponent": str(opp_name),  # <--- Now guarantees "Thunder" for recent games
            "result": f"Knicks scored {points} vs {opp_name} {opp_points}",
            "location": "Home" if is_home else "Away"
        }
    
    # 3. TRANSFORM: AI SDK Analysis
    @task.llm(
        model="groq:llama-3.3-70b-versatile", 
        system_prompt="You are a data-driven NBA analyst. Analyze the provided game stats."
    )
    def analyze_game_performance(game_context: dict):
        return f"""
        Analyze this game context: {game_context}.
        1. Was this a good defensive performance?
        2. Based on this, would you bet on them next game?
        Keep it under 3 sentences.
        """

    # 4. HITL
    human_check = ApprovalOperator(
        task_id="approve_analysis",
        subject="Approve AI Prediction",
        body="AI Analysis: {{ task_instance.xcom_pull(task_ids='analyze_game_performance') }}",
        defaults=["Approve"], 
    )

    # 5. LOAD: Save to Redshift
    save_to_db = SQLExecuteQueryOperator(
        task_id="publish_to_redshift",
        conn_id=DB_CONN_ID,
        outlets=[KNICKS_ASSET],
        # 1. Use %s for placeholders
        sql="""
            INSERT INTO knicks_predictions (game_date, opponent, recent_stats, ai_prediction)
            VALUES (%s, %s, %s, %s);
        """,
        # 2. Pass values as a LIST
        parameters=[
            '{{ task_instance.xcom_pull(task_ids="get_latest_game_context")["date"] }}',
            '{{ task_instance.xcom_pull(task_ids="get_latest_game_context")["opponent"] }}',
            '{{ task_instance.xcom_pull(task_ids="get_latest_game_context")["result"] }}',
            '{{ task_instance.xcom_pull(task_ids="analyze_game_performance") }}'
        ]
    )

    context = get_latest_game_context()
    analysis = analyze_game_performance(context)
    
    create_table >> context >> analysis >> human_check >> save_to_db

knicks_enterprise_pipeline()