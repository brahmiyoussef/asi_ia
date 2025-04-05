from models.database import connect_to_db, disconnect_from_db, sql_to_dataframe, pattern_historique
from models.processing_pipelines import cleaning, hourly_data_per_route, time_section
from models.prompts import ollama_connect, pattern_recognition, anomaly_detection
import pandas as pd
from models.structures import PatternOutput, AnomalyAnalysisOutput
import matplotlib.pyplot as plt 
import json

conn =connect_to_db("postgres")
print('connected to db successfully')
QUERY='''SELECT *
FROM messages
WHERE received_at >= CURRENT_DATE - INTERVAL '30 days';'''


[llm1,llm2]=ollama_connect()
print(llm1)

print('connected to ollama')

df=sql_to_dataframe(conn, QUERY)
print('returned data from db successfully')
print(df.shape)
cleaned_df = cleaning(df)
#print('cleaned data successfully')
#print(cleaned_df.head(1))
#print('cleaned data dictionary')
#print(*cleaned_df.keys())
[hourly_data,data] = hourly_data_per_route(cleaned_df)
#print(len(hourly_data),'cleaned data dictionary')
filtered_df=time_section(hourly_data,5,17)
#print('cleaned data dictionary')
#print(*filtered_df.keys())
prompt_testing=filtered_df['664b0f32-23ca-4c6a-bf15-1d3d04ec3674']
hourly_counts = prompt_testing.groupby('hour')['count'].sum().reset_index()
'''plt.figure(figsize=(10, 6))
plt.plot(hourly_counts['hour'], hourly_counts['count'], marker='o')
plt.title('Plot of Count Values')
plt.xlabel('hour')
plt.ylabel('Count')
plt.grid(True)
plt.show()'''
pattern=pattern_recognition(llm1, prompt_testing)

print(pattern.content)


