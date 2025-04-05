from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama
from models.structures import PatternOutput, AnomalyAnalysisOutput


def ollama_connect():
    llm = ChatOllama(
        model = "model2",
        temperature = 0.8,
        num_predict = 5000,
        history=False,
        top_k=100,
        repeat_last_n=200,
        )
    
    str_pattern_llm = llm.with_structured_output(PatternOutput)
    str_anomaly_llm =llm.with_structured_output(AnomalyAnalysisOutput)

    return llm, str_anomaly_llm

def pattern_recognition(llm,data):
    message= '''"You are an expert Data Engineer with a strong understanding of data analysis and general knowledge . Your task is to analyze a dataset of message counts and describe the usual values for the count of messages in different hours of the day.
Here is the format you will use to analyze the data and provide some values or intervales that represents the usual counts 
---

## Data Description
$brief_description_of_the_data_including_source_and_type_of_messages

## Hourly Message Count Analysis

### Hour 5-6 (Early Morning)
$description_of_message_count_during_these_hours_and deviation


### Hour 7-9 (Morning Commute)
$description_of_message_count_during_these_hours

### Hour 10-12 (Late Morning)
$description_of_message_count_during_these_hours

### Hour 13-17 (Afternoon)
$description_ofl_message_count_during_these_hours

## Overall Trends and Observations
$summary_of_the_general_trends_observed_in_the_hourly_message_counts
---

Here is the data you are tasked with analyzing:{data}

'''
    prompt = ChatPromptTemplate.from_messages(
        [
'''            (
                "system",
                "you are tasked to detect the usual count of messages in different hours and intervalls based on the value of the hour and count, please don't mind the index column.",
            ),'''
            ("human", message),
        ]
    )

    chain = prompt | llm
    output=chain.invoke(
        {
            
            "data": data,
        }
    )
    return output

def anomaly_detection(llm, time, count, pattern):
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "You are tasked with monitoring a data pipeline to determine if it is working properly. The data pipeline should follow these norms:{pattern}",
            ),
            (
                "human",
                "I will provide you with the current time and the count of messages received. Based on these inputs, you need to determine if the pipeline is operating normally. Please let me know if the pipeline is working properly based on the given information. Time: {time}. Messages Received per hour: {count}. Is the pipeline working properly?"
            ),
        ]
    )

    chain = prompt | llm

    output1 = chain.invoke(
        {
            "pattern":pattern,
            "time": time,
            "count": count,
        }
    )

    return output1


# def branshing methond for the mail process or neglect 
# email content prompting 
# ShortCircuitOperator for end tasks