import pandas as pd
from datetime import datetime
import os
import logging

# ----------------------------------------------------
# Logging configuration
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Simulated data source (in a real case, this would be a database or API)
def get_source_data():
    """Simulate a data source with updated_at timestamps."""
    data = [
        {"id": 1, "name": "Alice", "updated_at": "2025-10-25T10:00:00"},
        {"id": 2, "name": "Bob",   "updated_at": "2025-10-26T09:00:00"},
        {"id": 3, "name": "Carol", "updated_at": "2025-10-26T11:00:00"},
    ]
    return pd.DataFrame(data)

# ----------------------------------------------------
# Checkpoint functions
# ----------------------------------------------------
CHECKPOINT_FILE = "checkpoint.txt"

def load_last_checkpoint():
    """Load the last successful extraction timestamp."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            ts = f.read().strip()
            if ts:
                logging.info(f"Loaded last checkpoint: {ts}")
                return datetime.fromisoformat(ts)
    logging.info("No previous checkpoint found. Full extraction will be performed.")
    return None


def save_checkpoint(timestamp: datetime):
    """Save the timestamp of the last successful extraction."""
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(timestamp.isoformat())
    logging.info(f"Saved checkpoint: {timestamp.isoformat()}")


# ----------------------------------------------------
# Extraction logic (idempotent)
# ----------------------------------------------------
def extract_new_data():
    """Extract only new or updated records since the last checkpoint."""
    df = get_source_data()
    last_checkpoint = load_last_checkpoint()

    if last_checkpoint:
        df["updated_at"] = pd.to_datetime(df["updated_at"])
        new_data = df[df["updated_at"] > last_checkpoint]
    else:
        new_data = df

    logging.info(f"Extracted {len(new_data)} new/updated records.")
    return new_data


def main():
    try:
        new_data = extract_new_data()
        if not new_data.empty:
            # Simulate saving to storage or next pipeline step
            logging.info("Processing new records:")
            logging.info(f"\n{new_data}")

            # ✅ Convert updated_at column to datetime if needed
            new_data["updated_at"] = pd.to_datetime(new_data["updated_at"])
            latest_ts = new_data["updated_at"].max()

            # ✅ Ensure latest_ts is a datetime before saving
            if isinstance(latest_ts, str):
                latest_ts = datetime.fromisoformat(latest_ts)

            save_checkpoint(latest_ts)
        else:
            logging.info("No new data to extract.")
    except Exception as e:
        logging.exception(f"Extraction failed: {e}")


if __name__ == "__main__":
    main()
