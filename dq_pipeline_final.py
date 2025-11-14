"""
dq_pipeline_final.py

Dual validation data quality pipeline for the Amazon orders dataset:

- Read CSV with pandas
- Row-level schema validation with Pydantic
- Column-level & statistical validation with Great Expectations
- Save failed rows to failed_rows.csv
- Send detailed alerts to Slack via webhook
- Save expectation suite as JSON and YAML for versioning

Expected structure:

project_root/
│
├─ data/
│   └─ amazon_orders.csv
├─ config/
│   └─ slack_webhook.json
├─ expectations/
│   └─ amazon_suite.json / amazon_suite.yaml (auto-created)
└─ dq_pipeline_final.py
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import requests
import yaml
import great_expectations as gx

# Use Pydantic v1 API via compatibility mode (works when pydantic>=2 is installed)
from pydantic.v1 import BaseModel, Field, conint, confloat, validator


# =========================================================
# 1. Pydantic model for row-level validation
# =========================================================

class AmazonOrder(BaseModel):
    order_id: str = Field(alias="Order ID")
    date: str = Field(alias="Date")
    status: str = Field(alias="Status")
    qty: conint(ge=0) = Field(alias="Qty")
    amount: confloat(ge=0.0) = Field(alias="Amount")
    currency: str = Field(alias="currency")
    ship_country: str = Field(alias="ship-country")

    @validator("date")
    def validate_date_format(cls, v: str) -> str:
        """
        Validate date format like '04-30-22' (MM-DD-YY).
        """
        try:
            datetime.strptime(v, "%m-%d-%y")
        except Exception as e:
            raise ValueError(f"Invalid date format '{v}'. Expected MM-DD-YY.") from e
        return v

    @validator("currency")
    def validate_currency(cls, v: str) -> str:
        if v != "INR":
            raise ValueError(f"Unexpected currency '{v}'. Expected 'INR'.")
        return v

    @validator("ship_country")
    def validate_ship_country(cls, v: str) -> str:
        if v != "IN":
            raise ValueError(f"Unexpected ship-country '{v}'. Expected 'IN'.")
        return v


# =========================================================
# 2. Helper functions
# =========================================================

def load_dataframe(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"Loaded dataset from {csv_path} with {len(df)} rows.")
    return df


def load_slack_webhook(config_path: Path) -> str | None:
    if not config_path.exists():
        print(f"[WARN] Slack config not found at {config_path}. Slack alerts will be skipped.")
        return None
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    url = cfg.get("webhook_url")
    if not url:
        print("[WARN] 'webhook_url' key not found in slack_webhook.json. Slack alerts will be skipped.")
        return None
    return url


def slack_send(webhook_url: str | None, payload: Dict[str, Any]) -> None:
    if not webhook_url:
        # Slack disabled / not configured
        return
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code >= 400:
            print(f"[WARN] Slack returned HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[WARN] Failed to send Slack message: {e}")


# =========================================================
# 3. Row-level validation with Pydantic
# =========================================================

def run_pydantic_validation(df: pd.DataFrame) -> Dict[str, Any]:
    pydantic_errors: List[Dict[str, Any]] = []
    valid_indices: List[int] = []

    for idx, row in df.iterrows():
        try:
            AmazonOrder(**row.to_dict())
            valid_indices.append(idx)
        except Exception as e:
            pydantic_errors.append(
                {
                    "row_index": idx,
                    "error": str(e),
                }
            )

    num_errors = len(pydantic_errors)
    error_rate = round((num_errors / len(df)) * 100, 4) if len(df) > 0 else 0.0

    print(f"Pydantic validation errors: {num_errors} ({error_rate}%)")

    return {
        "errors": pydantic_errors,
        "num_errors": num_errors,
        "error_rate": error_rate,
        "valid_indices": valid_indices,
    }


# =========================================================
# 4. Column-level validation with Great Expectations
# =========================================================

def build_ge_suite(context: gx.DataContext, suite_name: str) -> gx.ExpectationSuite:
    """
    Create or fetch the expectation suite for Amazon orders.
    """
    try:
        suite = context.suites.get(name=suite_name)
        print(f"Loaded existing GE suite: {suite_name}")
    except Exception:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        print(f"Created new GE suite: {suite_name}")

    # Clear any existing expectations to avoid duplicates during development
    suite.expectations.clear()

    # Core expectations for the Amazon dataset
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Order ID")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="Order ID")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="Qty", min_value=0)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="Amount", min_value=0.0)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="ship-country",
            value_set=["IN"],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="Status",
            value_set=[
                "Shipped",
                "Cancelled",
                "Shipped - Delivered to Buyer",
            ],
        )
    )

    return suite




def run_ge_validation(df: pd.DataFrame, context: gx.DataContext, suite: gx.ExpectationSuite) -> Dict[str, Any]:
    """
    Run Great Expectations validation over the DataFrame.
    """
    datasource = context.data_sources.add_pandas(name="amazon_source")
    data_asset = datasource.add_dataframe_asset(name="amazon_orders_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="all_orders")

    validation_def = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name="amazon_validation",
    )

    print("Running Great Expectations validation...")
    results = validation_def.run(batch_parameters={"dataframe": df})
    is_success = results["success"]
    print("GE validation success:", is_success)

    # Collect failed expectation metadata
    failed_expectations: List[Dict[str, Any]] = []
    total_expectations = len(results["results"])

    for r in results["results"]:
        if not r["success"]:
            failed_expectations.append(
                {
                    "expectation": r["expectation_config"]["type"],
                    "column": r["expectation_config"]["kwargs"].get("column", "N/A"),
                    "unexpected_percent": r["result"].get("unexpected_percent", 0.0),
                    "unexpected_count": r["result"].get("unexpected_count", 0),
                    "unexpected_index_list": r["result"].get("unexpected_index_list", []),
                }
            )

    num_failed = len(failed_expectations)
    failure_rate = round((num_failed / total_expectations) * 100, 4) if total_expectations > 0 else 0.0

    print(f"GE failed expectations: {num_failed} ({failure_rate}%)")

    return {
        "success": is_success,
        "results": results,
        "failed_expectations": failed_expectations,
        "num_failed": num_failed,
        "total_expectations": total_expectations,
        "failure_rate": failure_rate,
    }


def extract_failed_rows(df: pd.DataFrame, ge_result: Dict[str, Any], output_path: Path) -> int:
    """
    Use GE's unexpected_index_list to extract failing rows and save to CSV.
    Returns number of failed rows saved.
    """
    failed_rows_list: List[pd.DataFrame] = []

    for f in ge_result["failed_expectations"]:
        col = f.get("column")
        idx_list = f.get("unexpected_index_list") or []

        if col and idx_list and col in df.columns:
            failed_rows_list.append(df.iloc[idx_list])

    if failed_rows_list:
        final_failed_df = pd.concat(failed_rows_list).drop_duplicates()
        output_path.parent.mkdir(exist_ok=True, parents=True)
        final_failed_df.to_csv(output_path, index=False)
        print(f"Saved failed rows to: {output_path} ({len(final_failed_df)} rows)")
        return len(final_failed_df)
    else:
        print("No row-level failures to save from GE results.")
        return 0


# =========================================================
# 5. Slack alert composition
# =========================================================

def build_slack_payload(
    total_rows: int,
    ge_result: Dict[str, Any],
    pydantic_result: Dict[str, Any],
) -> Dict[str, Any]:
    failed_expectations = ge_result["failed_expectations"]
    num_failed = ge_result["num_failed"]
    total_expectations = ge_result["total_expectations"]
    failure_rate = ge_result["failure_rate"]

    pydantic_errors = pydantic_result["num_errors"]
    pydantic_error_rate = pydantic_result["error_rate"]

    if failed_expectations:
        fail_summary = "\n".join(
            [
                f"- *{f['expectation']}* on column `{f['column']}` → "
                f"`{f['unexpected_percent']}%` unexpected (count={f['unexpected_count']})"
                for f in failed_expectations
            ]
        )
    else:
        fail_summary = "_No failed expectations at GE level._"

    text_title = ":x: *DATA QUALITY ALERT – Amazon Orders*"
    if ge_result["success"] and pydantic_errors == 0:
        text_title = ":white_check_mark: *DATA QUALITY OK – Amazon Orders*"

    payload: Dict[str, Any] = {
        "text": text_title,
        "attachments": [
            {
                "color": "#ff0000" if not ge_result["success"] or pydantic_errors > 0 else "#36a64f",
                "title": "Validation Summary",
                "fields": [
                    {"title": "Total Rows", "value": str(total_rows), "short": True},
                    {"title": "GE Expectations", "value": str(total_expectations), "short": True},
                    {"title": "GE Failed Expectations", "value": str(num_failed), "short": True},
                    {"title": "GE Failure Rate", "value": f"{failure_rate}%", "short": True},
                    {"title": "Pydantic Error Rows", "value": str(pydantic_errors), "short": True},
                    {"title": "Pydantic Error Rate", "value": f"{pydantic_error_rate}%", "short": True},
                ],
            },
            {
                "color": "#ff8c00",
                "title": "Failed Expectation Details",
                "text": fail_summary,
            },
        ],
    }

    return payload


# =========================================================
# 6. Main pipeline
# =========================================================

def main() -> None:
    # ---------- Paths ----------
    csv_path = Path("data/amazon_orders.csv")
    slack_config_path = Path("config/slack_webhook.json")
    expectations_dir = Path("expectations")
    failed_rows_path = Path("failed_rows.csv")

    # ---------- Load data ----------
    df = load_dataframe(csv_path)
    total_rows = len(df)

    # ---------- Pydantic validation ----------
    pydantic_result = run_pydantic_validation(df)

    # ---------- GE context & suite ----------
    context = gx.get_context()
    suite_name = "amazon_orders_suite"
    suite = build_ge_suite(context, suite_name)
    

    # ---------- GE validation ----------
    ge_result = run_ge_validation(df, context, suite)

    # ---------- Extract failed rows ----------
    failed_row_count = extract_failed_rows(df, ge_result, failed_rows_path)

    # ---------- Slack alert ----------
    webhook_url = load_slack_webhook(slack_config_path)
    payload = build_slack_payload(total_rows, ge_result, pydantic_result)
    slack_send(webhook_url, payload)
    print("Slack summary alert sent (if webhook configured).")

    # Additional critical alert if GE failure rate too high
    if ge_result["failure_rate"] > 5 or pydantic_result["error_rate"] > 1:
        critical_text = (
            f":rotating_light: *CRITICAL DQ ALERT* – "
            f"GE failure rate {ge_result['failure_rate']}%, "
            f"Pydantic error rate {pydantic_result['error_rate']}%"
        )
        slack_send(webhook_url, {"text": critical_text})
        print("Critical Slack alert sent (threshold exceeded).")

    print("\n--- Pipeline finished ---")
    print(f"Total rows: {total_rows}")
    print(f"Pydantic errors: {pydantic_result['num_errors']}")
    print(f"GE failed expectations: {ge_result['num_failed']}")
    print(f"Failed rows saved by GE: {failed_row_count}")


if __name__ == "__main__":
    main()
