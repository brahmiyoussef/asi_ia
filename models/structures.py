from langchain_core.pydantic_v1 import BaseModel, Field

class PatternOutput(BaseModel):
    analysis: str = Field(..., description="Detailed analysis of the detected pattern")
    pattern: str = Field(..., description="The detected pattern in the data")


class AnomalyAnalysisOutput(BaseModel):
    analysis: str = Field(..., description="Detailed analysis of whether the data represents an anomaly")
    is_anomaly: bool = Field(..., description="Whether the data represents an anomaly")
