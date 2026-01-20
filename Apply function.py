import pandas as pd

# Sample dataset
df = pd.DataFrame({
    "emp_id": [101, 102, 103, 104, 105],
    "salary": [70000, None, 60000, 72000, 68000],
    "bonus": [5000, 7000, None, 6000, 8000]
})

#print(df)

df1=df.fillna({"salary":0,"bonus":0})
print(df1)
#if we specify inplace=True means changes will be applied to original dataframe 
df1=df["salary"].fillna(0)
print(df1)

#Q1. Replace all missing values in a DataFrame with 0.
#df.fillna(0,inplace=True)

#Q2. Fill missing salaries with the average salary.
df["salary"]=df["salary"].fillna(df["salary"].mean())
print(df)

df["bonus"]=df["bonus"].fillna(method="ffill")
print(df)


df = pd.DataFrame({
    "emp_id": [101, 102, 103, 104,105,106],
    "department": ["IT", "Finance", None, "HR",'HR',"HR"],
    "salary": [70000, None, 60000, 75000,4233,34555,],
    "bonus": [5000, 7000, None, None,2000,1000]
})

print(df)

#Given a DataFrame with salary and bonus columns, if bonus is NaN, fill it with 10% of salary. Use apply().
df["bonus"]=df["bonus"].fillna(df["salary"]*0.1)
"""
#Fill missing bonus with department-wise mean bonus. 
df["bonus"]=df["bonus"].fillna(
    df.groupby("department")["bonus"].transform("mean"))

#Fill missing bonus with department-wise mean bonus.
#  if department is null take bonus as all employee mean
df["bonus"]=df["bonus"].fillna(
    df.groupby("department")["bonus"].transform("mean"))\
        .fillna(df["bonus"].sum())
"""

print(df)
#Given a DataFrame with salary and bonus columns, if bonus is NaN, fill it with 10% of salary. Use apply().
df["bonus"]=df["bonus"].fillna(df["salary"]*0.1)
print(df)
print(df.dtypes)
print(df.columns)
#print(df.groupby("department")["bonus"].transform("sum"))

#df["bonus"]=df["bonus"].fillna(df.groupby("department")["bonus"].transform("sum"))
#print(df)

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
