import pandas as pd
import dagster as dg # type: ignore

# sample_data_file = "src/dagster_pg/defs/data/sample_data.csv"
sample_data_file = "gs://test-tmp-delete-when-done/agents/sample_data.csv"
processed_data_file = "src/dagster_pg/defs/data/processed_data2.csv"

sample_data_file_excel = "gs://test-tmp-delete-when-done/agents/sample_data.csv"
processed_data_file_excel = "src/dagster_pg/defs/data/excels/processed_excel.csv"


@dg.asset
def processed_data_CSV():
    ## Read data from the CSV
    # df = pd.read_csv(sample_data_file)
    df = pd.read_csv(sample_data_file, storage_options={"token": "google_default"})


    ## Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    ## Save processed data
    df.to_csv(processed_data_file, index=False)
    return "Data loaded successfully"

@dg.asset
def processed_data_execl():
    df = pd.read_excel(sample_data_file_excel)
    df["age"] = df["age"] + 10
    df.to_excel(processed_data_file_excel, index=False)
    return "Process Data successfully!!!"
