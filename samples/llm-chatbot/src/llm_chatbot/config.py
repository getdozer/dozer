from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    dozer_api_base: str
    customer_id: str
    chroma_directory: str
    collection_name: str


def load_settings() -> Settings:
    return Settings(
        dozer_api_base=os.environ.get("DOZER_API_BASE", "http://localhost:8080").rstrip("/"),
        customer_id=os.environ.get("DOZER_CUSTOMER_ID", "customer-001"),
        chroma_directory=os.environ.get("CHROMA_DIRECTORY", ".chroma"),
        collection_name=os.environ.get("CHROMA_COLLECTION", "credit_card_products"),
    )
