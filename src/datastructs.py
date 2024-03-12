from dataclasses import dataclass

import polars as pl


@dataclass
class product_version:
    id: pl.String
    flagship_product: pl.String
    version: pl.String
    release_date: pl.Date
    sunset_date: pl.Date
    is_active: pl.Boolean


@dataclass
class reason_code_mt:
    id: pl.String
    reason_code: pl.String
    reason_class: pl.String
    description: pl.String
    needs_approval: pl.String
