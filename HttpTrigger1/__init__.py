import logging
import azure.functions as func
from pyspark.sql import SparkSession
import tempfile


def read_csv(spark, file_path):
    """Reads CSV file into a PySpark DataFrame."""
    try:
        df = spark.read.option("header", "true").csv(file_path)
        return df
    except Exception as e:
        raise RuntimeError(f"Error reading CSV: {str(e)}")


def edit_csv(df):
    """Fills missing stock status values with 'Out Of Stock'."""
    try:
        df = df.fillna("Out Of Stock", subset=["Stock_status"])
        return df
    except Exception as e:
        raise RuntimeError(f"Error processing CSV: {str(e)}")


def join_df(df1, df2):
    """Joins two DataFrames on 'pg_vendor_product_id'."""
    try:
        return df1.join(df2, df1["pg_vendor_product_id"] == df2["pg_vendor_product_id"], "inner")
    except Exception as e:
        raise RuntimeError(f"Error joining DataFrames: {str(e)}")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing CSV files...")

    try:
        # Ensure both files exist in the request
        if "file_1" not in req.files or "file_2" not in req.files:
            return func.HttpResponse("Both files (file_1 and file_2) must be uploaded.", status_code=400)

        file1 = req.files["file_1"]
        file2 = req.files["file_2"]
        temp_file_1 = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        temp_file_2 = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        file1.save(temp_file_1.name)
        file2.save(temp_file_2.name)

        spark = SparkSession.builder.appName("CSVProcessor").getOrCreate()
        oos = read_csv(spark, temp_file_1.name)
        art_m = read_csv(spark, temp_file_2.name)
        oos = edit_csv(oos)
        df = join_df(oos, art_m)

        table_str = df.limit(20).toPandas().to_string(index=False)

        spark.stop()

        return func.HttpResponse(table_str, mimetype="text/plain")

    except Exception as e:
        logging.error(f"Error processing CSV: {str(e)}")
        return func.HttpResponse(f"Error processing CSV: {str(e)}", status_code=500)


if __name__ == "__main__":
    main()
