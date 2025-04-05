import pandas as pd
import itertools


def cleaning(df):
    column_names=['id', 'received_at', 'sent_at', 'status', 'payload', 'creation_date', 'route_id', 'operation_type', 'type', 'fk_matching_id', 'reject_rsn', 'storage_key_file_in']

    data=df.drop(columns=['sent_at', 'status','payload','creation_date','type', 'fk_matching_id','operation_type','reject_rsn','storage_key_file_in', ])
    data['received_at']=pd.to_datetime(data['received_at'],yearfirst=True)
    data['date']= data['received_at'].dt.date
    data['dayOfWeek'] = data['received_at'].dt.day_name()
    data['hour']=data['received_at'].dt.hour
    data['hour'] = data['hour'].astype(int)
    data.sort_values(by='received_at', inplace=True)
    return data
        


def hourly_data_per_route(df):
    date_day_mapping = df[['date', 'dayOfWeek']].drop_duplicates()
 
    unique_hours = range(24)
    unique_route_ids=pd.unique(df['route_id'])    
    all_combinations = date_day_mapping.merge(
        pd.DataFrame(itertools.product(unique_hours, unique_route_ids), columns=['hour', 'route_id']),
        how='cross')
    test_features3 = df.groupby(['date', 'dayOfWeek', 'hour', 'route_id']).size().reset_index(name='count')  
    data = all_combinations.merge(test_features3, on=['date', 'dayOfWeek', 'hour', 'route_id'], how='left').fillna(0)
    data = data[~data['dayOfWeek'].isin(['Saturday', 'Sunday'])]
    cleaned_data={}
    for x in unique_route_ids:
        dt=data[data['route_id']==x]
        dt=dt.drop(columns=['route_id','dayOfWeek','date'])
        cleaned_data[x]=dt
    return cleaned_data, data




def time_section(data_dict, start, end):
    filtered_dict = {}
    for key, df in data_dict.items():
        filtered_df = df[(df['hour'] >= start) & (df['hour'] <= end)]
        filtered_dict[key] = filtered_df

    return filtered_dict