from models.database import connect_to_db, disconnect_from_db, sql_to_dataframe, pattern_historique
from models.processing_pipelines import cleaning, hourly_data_per_route, time_section
from models.prompts import ollama_connect, pattern_recognition, anomaly_detection
from models.structures import PatternOutput, AnomalyAnalysisOutput

def pattern_recognition():
    connect_to_db()

    try:
        df = sql_to_dataframe("SELECT * FROM your_table")
        cleaned_df = cleaning(df)
        hourly_data = hourly_data_per_route(cleaned_df)
        time_sections = time_section(hourly_data)
        ollama = ollama_connect()
        patterns = []
        for section in time_sections:
            pattern = pattern_recognition(ollama, section)
            patterns.append(PatternOutput(pattern))
        anomalies = []
        for pattern in patterns:
            anomaly = anomaly_detection(pattern)
            anomalies.append(AnomalyAnalysisOutput(anomaly))

        return patterns, anomalies

    finally:
        disconnect_from_db()
