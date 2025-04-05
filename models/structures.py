from pydantic import BaseModel,Field
from typing import List

class IntervalAnalysis(BaseModel):
    hour: int = Field(..., description="The hour of the day (0-23)")
    mean_count: float = Field(..., description="a value that represents the usual messages count in this hour")
    trend: str = Field(..., description="pattern observed during this hour (e.g., 'increasing', 'decreasing', 'stable')")
    overall_trend: str = Field(..., description='the overall pattern in the day')

class PatternOutput(BaseModel):
    analysis: str = Field(..., description="Detailed analysis of the detected pattern in each hour")
    intervals: List[IntervalAnalysis] = Field(..., description="List of all interval analyses with detailed counts and patterns")
    overall_trend: str = Field(..., description='walkthough a day s pattern with details ')


class AnomalyAnalysisOutput(BaseModel):
    analysis: str = Field(..., description="Detailed analysis of whether the data represents an anomaly")
    is_anomaly: bool = Field(..., description="Whether the data represents an anomaly")
