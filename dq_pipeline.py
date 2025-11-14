import pandas as pd
import great_expectations as gx
import json
import requests
from pathlib import Path


# ---------------------------------------------------------
# 1. Load Dataset with Pandas
# ---------------------------------------------------------
csv_path = Path("data/amazon_orders.csv")
df = pd.read_csv(csv_path)

print(f"Loaded dataset with {len(df)} rows.")


# ---------------------------------------------------------
# 2. Create GX Context (Ephemeral - code-based)
# ---------------------------------------------------------
context = gx.get_context()

# Add Pandas datasource
datasource = context.data_sources.add_pandas(name="amazon_source")

# Create dataframe asset
data_asset = datasource.add_dataframe_asset(name="amazon_orders_asset")

# Define a batch (entire dataframe)
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    name="all_orders"
)


# ---------------------------------------------------------
# 3. Create Expectation Suite
# ---------------------------------------------------------
suite_name = "amazon_orders_suite"

try:
    suite = context.suites.get(name=suite_name)
except:
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

# ---- Column-level Expectations ----
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="Order ID")
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="Order ID")
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="Qty", min_value=0
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="Amount", min_value=0
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="ship-country",
        value_set=["IN"]
    )
)

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

# Save suite (to expectations/ folder)
#context.suites.save(suite)


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
# 5. Slack Notification (only if fail)
# ---------------------------------------------------------
config_path = Path("config/slack_webhook.json")
webhook_cfg = json.loads(config_path.read_text())
webhook_url = webhook_cfg["webhook_url"]


def notify_slack(message: str):
    payload = {
        "text": message
    }
    requests.post(webhook_url, json=payload)


if not is_success:
    msg = (
        ":x: *DATA QUALITY FAILED* \n"
        f"Dataset: Amazon Orders\n"
        f"Validations Passed: {is_success}\n"
        f"Total Rows: {len(df)}"
    )
    notify_slack(msg)
    print("Slack notification sent.")
else:
    print("All good. No Slack notification needed.")
