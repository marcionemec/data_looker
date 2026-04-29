import subprocess
import os
import pandas as pd
from pyspark.sql import SparkSession
from urllib3.util.retry import Retry

local_path = 'local_data'
os.makedirs(f"{local_path}/raw", exist_ok=True)
os.makedirs(f"{local_path}/parquet", exist_ok=True)
  

def should_download(path):
    return not os.path.exists(path)

def download_file(url, path):
    import subprocess
    if should_download(path):
        subprocess.run([
            "curl",
            "-L",
            "--http1.1",
            "--insecure",
            "--retry", "10",
            "--retry-all-errors",

            "--compressed",        # aceita gzip (às vezes acelera CSV)
            "--no-buffer",         # reduz latência
            "--connect-timeout", "10",
            "--max-time", "0",     # sem limite total

            "-o", path,
            url
        ], check=True)

def download_file_lento(url, path):
    subprocess.run([
        "curl",
        "-L",
        "--http1.1",
        "--insecure",         # ignora SSL (usar só se necessário)
        "--retry", "10",
        "--retry-all-errors",
        "-o", path,
        url
    ], check=True)

# 1. Start Spark
spark = SparkSession.builder \
    .appName("LocalCSVtoParquet") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# 2. List of URLs
urls = [
    "https://dadosabertos.capes.gov.br/dataset/1e577e61-729f-473a-8dd6-4918401c18a9/resource/34d3bd04-a1e4-40d9-8471-3c2109de7808/download/br-capes-colsucup-projeto-2022-2025-03-31.csv",
    "https://dadosabertos.capes.gov.br/dataset/1e577e61-729f-473a-8dd6-4918401c18a9/resource/597a41e9-2948-4053-bb7f-ae4adae1fae4/download/br-capes-colsucup-projeto-2021-2025-03-31.xlsx"
]

# 3. Loop through URLs
for url in urls:
    # Extract filename (without extension)
    filename = url.split("/")[-1].split(".")[0]
    print(f"Processing {filename} {url}...")
    

    local_file = f"{local_path}/raw/{filename}.csv"
    download_file(url,local_file) 
    #with urllib.request.urlopen(url) as response:
    #    data = response.read()
        # Detect file type
    if local_file.endswith(".csv"): 
        spark_df = spark.read \
            .option("header", True) \
            .option("encoding", "latin1") \
            .csv(f"file:///{local_path}")
        #df = pd.read_csv(BytesIO(data), encoding="latin1", errors="replace")
    elif url.endswith(".xlsx"):
        #df = pd.read_excel(BytesIO(data), encoding="latin1", errors="replace")
        df = pd.read_excel(local_file)
        spark_df = spark.createDataFrame(df)
    else:
        print(f"Skipping unsupported file type: {url}")
        continue 
    
    
     
    
    # Save locally as Parquet
    output_path = f"{local_path}/parquet/{filename.split('.')[0]}"
    spark_df.write.mode("overwrite").parquet(output_path)

    print(f"Saved {output_path}")

spark.stop()
