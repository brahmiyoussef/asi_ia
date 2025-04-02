import pandas as pd


def cleaning(df):
    data=df.drop(columns=['sent_at','payload', 'operation_type','reject_rsn','storage_key_file_in'])
    data['received_at']=pd.to_datetime(data['received_at'],yearfirst=True)
    data['dayOfWeek'] = data['received_at'].dt.day_name
    data['hour']=data['received_at'].dt.hour
    data['hour'] = data['hour'].astype(int)
    data.sort_values(by='received_at', inplace=True)
    return data
        


def hourly_data_per_route(df):
    data=df.groupby(['date','dayOfWeek','hour','route_id']).count()
    unique_route_ids=pd.unique(data['route_id'])
    n=len(unique_route_ids)
    cleaned_data={}
    for x in unique_route_ids:
        dt=data[data['route_id']==x]
        dt=dt.drop(columns='route_id')
        cleaned_data[x]=dt
    return cleaned_data



def time_section(df,start,end):
    data=df[df['hour']<=end]
    data=data[data['hour']>=start]
    return data
