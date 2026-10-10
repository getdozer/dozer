import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CreditCardProduct:
    product_id: str
    name: str
    annual_fee_cents: str
    rewards: str
    eligibility: str
    additional_details: str


def load_credit_card_products(csv_path: Path) -> list[CreditCardProduct]:
    with csv_path.open(encoding="utf-8", newline="") as product_file:
        return [
            CreditCardProduct(**row)
            for row in csv.DictReader(product_file)
            if row.get("product_id")
        ]


def product_document(product: CreditCardProduct) -> tuple[str, dict[str, str]]:
    page_content = " ".join(
        [
            product.name,
            f"annual fee {product.annual_fee_cents} cents",
            product.rewards,
            product.eligibility,
            product.additional_details,
        ]
    )
    return page_content, {
        "product_id": product.product_id,
        "name": product.name,
        "annual_fee_cents": product.annual_fee_cents,
    }
