from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama


def ollama_connect(patternoutput,anomalyAnalysisOutput):
    llm = ChatOllama(
        model = "model2",
        temperature = 0.8,
        num_predict = 300,
    )
    str_pattern_llm = llm.with_structured_output(patternoutput)
    str_anomaly_llm =llm.with_structured_output(anomalyAnalysisOutput)

    return str_pattern_llm, str_anomaly_llm

def pattern_recognition(llm,data):
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "you are an expert in detecting patterns in data .",
            ),
            ("human", "You are tasked with analyzing a series of numbers that represent the count of messages per hour in a data pipeline. Your goal is to determine the pattern or trend within the series of numbers. I will provide you with the series of numbers, and you need to identify the pattern and explain it. Here is the series of numbers: {data}  Please analyze the series and describe the pattern in detail."),
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