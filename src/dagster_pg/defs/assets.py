import pandas as pd
import dagster as dg # type: ignore
import gcsfs

# sample_data_file = "src/dagster_pg/defs/data/sample_data.csv"
sample_data_file = "gs://test-tmp-delete-when-done/agents/sample_data.csv"
processed_data_file = "src/dagster_pg/defs/data/processed_data2.csv"

sample_data_file_excel = "gs://test-tmp-delete-when-done/excel/raw/agents-dagster.xlsx"
processed_data_file_excel = "gs://test-tmp-delete-when-done/excel/processed/agents-dagster_processedv1.xlsx"

sample_data_file_txt = "gs://test-tmp-delete-when-done/txt/raw/agents-dagster.txt"
processed_data_file_txt = "gs://test-tmp-delete-when-done/txt/processed/agents-dagster_processedv1.txt"


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

@dg.asset
def processed_data_txt():
    fs = gcsfs.GCSFileSystem(
        project= "myorder-beta",
        token=""
    )
    with fs.open(sample_data_file_txt, "r") as f:
        lines = f.readlines()
    
    process_lines = []
    for line in lines:
        parts = line.strip().split(",")
        if len(parts) > 3:
            parts[2] = str(int(parts[2]) + 10)
        process_lines.append(",".join(parts))


    with fs.open(processed_data_file_txt, "w") as f:
        f.write("\n".join(process_lines))
    
    return "Data loaded successfully"