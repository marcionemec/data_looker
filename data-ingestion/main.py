import os
import subprocess
import pandas as pd
from pyspark.sql import SparkSession

# Container paths
BASE_PATH = "/app/data"
RAW_PATH = f"{BASE_PATH}/raw"
PARQUET_PATH = f"{BASE_PATH}/parquet"

os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(PARQUET_PATH, exist_ok=True)

def download_file(url, target_path):
    if os.path.exists(target_path):
        print(f"File already exists: {target_path}")
        return
    
    print(f"Downloading: {url}")
    subprocess.run([
        "curl", "-L", "--http1.1", "--insecure",
        "--retry", "10", "--retry-all-errors",
        "--compressed", "--no-buffer",
        "-o", target_path, url
    ], check=True)

def get_spark():
    return SparkSession.builder \
        .appName("DataPipeline") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def process_files():
    spark = get_spark()
    
    urls = [
        "https://dadosabertos.capes.gov.br/dataset/1e577e61-729f-473a-8dd6-4918401c18a9/resource/34d3bd04-a1e4-40d9-8471-3c2109de7808/download/br-capes-colsucup-projeto-2022-2025-03-31.csv",
        "https://dadosabertos.capes.gov.br/dataset/1e577e61-729f-473a-8dd6-4918401c18a9/resource/597a41e9-2948-4053-bb7f-ae4adae1fae4/download/br-capes-colsucup-projeto-2021-2025-03-31.xlsx"
    ]

    for url in urls:
        filename_full = url.split("/")[-1]
        filename_no_ext = filename_full.split(".")[0]
        local_file = f"{RAW_PATH}/{filename_full}"

        # 1. Extraction
        download_file(url, local_file)

        # 2. Transformation
        print(f"Transforming {filename_full} to Parquet...")
        try:
            if local_file.endswith(".csv"):
                # Use absolute path for Spark in container
                df = spark.read \
                    .option("header", True) \
                    .option("encoding", "ISO-8859-1") \
                    .option("sep", ";") \
                    .csv(local_file)
            
            elif local_file.endswith(".xlsx"):
                # Spark doesn't read Excel natively well; using Pandas as intermediary
                pd_df = pd.read_excel(local_file)
                df = spark.createDataFrame(pd_df.astype(str)) # Convert to string to avoid schema issues

            output_dir = f"{PARQUET_PATH}/{filename_no_ext}"
            df.write.mode("overwrite").parquet(output_dir)
            print(f"Successfully saved to: {output_dir}")
            
        except Exception as e:
            print(f"Error processing {filename_full}: {e}")

    spark.stop()

if __name__ == "__main__":
    process_files()