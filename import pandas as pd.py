import pandas as pd

# Sample dataset
df = pd.DataFrame({
    "emp_id": [101, 102, 103, 104, 105],
    "salary": [70000, 65000, 60000, 72000, 68000],
    "bonus": [5000, 7000, None, 6000, 8000]
})

print(df)
"""
# Fill missing bonus using apply (10% of salary if bonus is missing)
df["bonus"] = df.apply(
    lambda row: row["bonus"] if pd.notna(row["bonus"]) else row["salary"] * 0.10,
    axis=1
)

# Calculate total compensation
df["total_compensation"] = df.apply(
    lambda row: row["salary"] + row["bonus"], axis=1
)

print(df)
"""
