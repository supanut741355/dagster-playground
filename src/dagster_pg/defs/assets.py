import pandas as pd
import dagster as dg # type: ignore
import gcsfs # type: ignore
from PyPDF2 import PdfReader, PdfWriter, PageObject # type: ignore
from io import BytesIO

# sample_data_file = "src/dagster_pg/defs/data/sample_data.csv"
sample_data_file = "gs://test-tmp-delete-when-done/agents/sample_data.csv"
processed_data_file = "src/dagster_pg/defs/data/processed_data2.csv"

sample_data_file_excel = "gs://test-tmp-delete-when-done/excel/raw/agents-dagster.xlsx"
processed_data_file_excel = "gs://test-tmp-delete-when-done/excel/processed/agents-dagster_processedv1.xlsx"

sample_data_file_txt = "gs://test-tmp-delete-when-done/txt/raw/agents-dagster.txt"
processed_data_file_txt = "gs://test-tmp-delete-when-done/txt/processed/agents-dagster_processedv1.txt"

sample_data_file_pdf = "gs://test-tmp-delete-when-done/pdf/raw/agents-dagster.pdf"
processed_data_file_pdf = "gs://test-tmp-delete-when-done/pdf/processed/agents-dagster_processedv1.pdf"


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

@dg.asset
def process_data_pdf():
    fs = gcsfs.GCSFileSystem(
        project= "myorder-beta",
        token="/Users/nutx/Desktop/Workspaces/PoC/dagster-PG/dagster-pg/src/dagster_pg/defs/key/myorder-275414-2164afd724f0.json"
    )
    with fs.open(sample_data_file_pdf, "rb") as f:
        reader = PdfReader(f)
        text=""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    lines = text.strip().split("\n")
    processed_lines = []
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 3:
            parts[2] = str(int(parts[2]) + 10)  
        processed_lines.append(",".join(parts))


    for line in processed_lines:
        print(line)

    # write back to pdf
    # writer = PdfWriter()
    # pdf_bytes = BytesIO()
    # for line in processed_lines:
    #     page = PageObject.create_blank_page(width=300, height=50)
    #     page.insert_text(line)
    #     writer.add_page(page)
    # with fs.open(processed_data_file_pdf, "wb") as f_out:
    #     writer.write(f_out)
    
    return "Data loaded successfully"