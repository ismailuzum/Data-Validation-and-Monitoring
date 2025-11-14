import pandas as pd
import great_expectations as gx
import json
import requests
from pathlib import Path


# ---------------------------------------------------------
# 1. Load Dataset with Pandas
# ---------------------------------------------------------
csv_path = Path("data/amazon_orders.csv")
df = pd.read_csv(csv_path, low_memory=False)

total_rows = len(df)
print(f"Loaded dataset with {total_rows} rows.")




# ---------------------------------------------------------
# 2. Create Great Expectations Context
# ---------------------------------------------------------
context = gx.get_context()

datasource = context.data_sources.add_pandas(name="amazon_source")
data_asset = datasource.add_dataframe_asset(name="amazon_orders_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe(name="all_orders")


# ---------------------------------------------------------
# 3. Expectation Suite (GX v1.0+ code-first)
# ---------------------------------------------------------
suite_name = "amazon_orders_suite"

try:
    suite = context.suites.get(name=suite_name)
except:
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="Order ID"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="Order ID"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="Qty", min_value=0))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="Amount", min_value=0))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="ship-country", value_set=["IN"]))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="Status",
        value_set=[
            "Shipped",
            "Cancelled",
            "Shipped - Delivered to Buyer"
        ]
    )
)


# ---------------------------------------------------------
# 4. Run Validation
# ---------------------------------------------------------
validation_def = gx.ValidationDefinition(
    data=batch_definition,
    suite=suite,
    name="amazon_validation"
)

results = validation_def.run(batch_parameters={"dataframe": df})
is_success = results["success"]

print("Validation success:", is_success)


# ---------------------------------------------------------
# 5. Extract detailed validation stats
# ---------------------------------------------------------
failed_expectations = []
total_expectations = len(results["results"])

for r in results["results"]:
    if not r["success"]:
        failed_expectations.append({
            "expectation": r["expectation_config"]["type"],
            "column": r["expectation_config"]["kwargs"].get("column", "N/A"),
            "unexpected_percent": r["result"].get("unexpected_percent", 0)
        })

num_failed = len(failed_expectations)
fail_rate = round((num_failed / total_expectations) * 100, 2)


# ---------------------------------------------------------
# 6. Collect failed rows safely
# ---------------------------------------------------------
failed_csv_path = Path("failed_rows.csv")
failed_rows_list = []

for r in results["results"]:
    if not r["success"]:
        col = r["expectation_config"]["kwargs"].get("column")
        
        # Only column-based expectations create rows
        if col and col in df.columns:

            # GE always provides unexpected indices inside `result["unexpected_index_list"]`
            unexpected_idx = r["result"].get("unexpected_index_list", [])

            if unexpected_idx:
                failed_rows_list.append(df.iloc[unexpected_idx])

# If we have failed rows → save them
if failed_rows_list:
    final_failed_df = pd.concat(failed_rows_list).drop_duplicates()
    final_failed_df.to_csv(failed_csv_path, index=False)
    print(f"Saved failed rows to: {failed_csv_path}")
else:
    print("No failed rows to save (failures were rule-level, not row-level).")



# ---------------------------------------------------------
# 7. Slack Notification (Enhanced Formatting)
# ---------------------------------------------------------
webhook_cfg = json.loads(Path("config/slack_webhook.json").read_text())
webhook_url = webhook_cfg["webhook_url"]

def slack_send(payload):
    requests.post(webhook_url, json=payload)


if not is_success:

    fail_summary = "\n".join(
        [
            f"- *{f['expectation']}* on column `{f['column']}` → `{f['unexpected_percent']}%` unexpected"
            for f in failed_expectations
        ]
    )

    payload = {
        "text": ":x: *DATA QUALITY ALERT – Amazon Orders*",
        "attachments": [
            {
                "color": "#ff0000",
                "title": "Validation Summary",
                "fields": [
                    {"title": "Total Rows", "value": str(total_rows), "short": True},
                    {"title": "Total Expectations", "value": str(total_expectations), "short": True},
                    {"title": "Failed Expectations", "value": str(num_failed), "short": True},
                    {"title": "Failure Rate", "value": f"{fail_rate}%", "short": True},
                ]
            },
            {
                "color": "#ff8c00",
                "title": "Failed Expectation Details",
                "text": fail_summary
            }
        ]
    }

    slack_send(payload)
    print("Slack alert sent.")

    # Critical alert if fail rate above 5%
    if fail_rate > 5:
        slack_send({"text": f":rotating_light: *CRITICAL ALERT* – Failure rate {fail_rate}% > 5%"})
        print("Critical Slack alert sent.")

else:
    print("All good, no Slack alert.")


