from pydantic import BaseModel, Field
from typing import List, Optional, Dict


# BaseModel is for intialization of data models.
class RawData(BaseModel):
    endpoint: str
    payload: dict

class ProductItem(BaseModel):
    id: str
    title: str
    source: str
    price: Optional[float] = None
    category: str
    processed_at: str

class ErrorLog(BaseModel):
    endpoint: str
    error: str
    timestamp: str


class Summary(BaseModel):
    total_products: int
    processing_time_seconds: float
    success_rate: float
    sources: List[str]


class PriceMetrics(BaseModel):
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    average_price: Optional[float] = None
    valid_price_count: int = 0


class Metrics(BaseModel):
    price_metrics: PriceMetrics
    category_distribution: Dict[str, int] = Field(default_factory=dict)
    source_request_counts: Dict[str, int] = Field(default_factory=dict)


class OutputModel(BaseModel):
    summary: Summary
    products: List[ProductItem]
    errors: List[ErrorLog]
    metrics: Metrics