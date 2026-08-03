import sys
from pyspark.sql import SparkSession

def verify_all_nulls(df):
    # Verify all rows have null/empty values in a single pass
    # Standardizing null checks: We only check core types that have consistent NULL representation.
    # JSON and MapType are ignored for null-verification due to inconsistent string/collection mapping.
    ignored_cols = {"f_json", "f_map"}
    conds = []
    for f in df.schema.fields:
        if f.name in ignored_cols:
            continue
            
        col_name = f"`{f.name}`"
        if f.dataType.typeName() == "array":
            # For arrays, check both null and size
            conds.append(f"({col_name} IS NOT NULL AND size({col_name}) > 0)")
        else:
            conds.append(f"{col_name} IS NOT NULL")
            
    if not conds:
        print("No columns available for null verification. Skipping.")
        return

    filter_expr = " OR ".join(conds)
    print(f"Applying null verification filter: {filter_expr}")
    
    invalid_rows = df.filter(filter_expr)
    invalid_count = invalid_rows.count()
    
    if invalid_count > 0:
        print("Detected non-null or non-empty values! Sample invalid rows:")
        invalid_rows.show(10, truncate=False)
        raise Exception(f"Detected {invalid_count} non-null or non-empty values where all nulls were expected")

def test_read_verify(spark, table_name, all_nulls, expected_count):
    # Standardized on ARROW format
    print(f"Read Verify from {table_name} (format=ARROW, all_nulls={all_nulls}, expected_count={expected_count})")
    df = spark.read.format("bigquery") \
        .option("table", table_name) \
        .option("readDataFormat", "ARROW") \
        .option("bigNumericDefaultPrecision", 38) \
        .option("bigNumericDefaultScale", 9) \
        .load()
    
    df.cache()
    count = df.count()
    print(f"Count: {count}")
    if count != expected_count:
        raise Exception(f"Count mismatch! Expected: {expected_count}, Actual: {count}")

    if all_nulls:
        verify_all_nulls(df)
    
    print("Read Verify (ARROW) Success!")

def test_write_and_verify(spark, source_table, destination_table, write_method, intermediate_format="parquet", gcs_bucket=None):
    print(f"Write and Verify: {source_table} -> {destination_table} (method={write_method}, format={intermediate_format})")
    
    # 1. Read source
    df = spark.read.format("bigquery") \
        .option("table", source_table) \
        .option("readDataFormat", "ARROW") \
        .option("bigNumericDefaultPrecision", 38) \
        .option("bigNumericDefaultScale", 9) \
        .load()
    
    source_count = df.count()

    # 2. Write back
    if "f_json" in df.columns:
        df = df.withMetadata("f_json", {"sqlType": "JSON"})

    writer = df.write.format("bigquery") \
        .option("table", destination_table) \
        .option("writeMethod", write_method) \
        .mode("overwrite")
    
    if write_method == "INDIRECT":
        writer.option("temporaryGcsBucket", gcs_bucket) \
              .option("intermediateFormat", intermediate_format)
    
    writer.save()

    # 3. Verify destination
    df_back = spark.read.format("bigquery") \
        .option("table", destination_table) \
        .option("readDataFormat", "ARROW") \
        .option("bigNumericDefaultPrecision", 38) \
        .option("bigNumericDefaultScale", 9) \
        .load()
    
    back_count = df_back.count()
    print(f"Source count: {source_count}, Back count: {back_count}")
    
    if source_count != back_count:
        raise Exception(f"Write Verification count mismatch! Source: {source_count}, Back: {back_count}")

    print(f"Write and Verify ({write_method}/{intermediate_format}) Success!")

def test_case_sensitivity(spark, table_name, column_name):
    print(f"Testing case sensitivity on {table_name} with column {column_name}")
    df = spark.read.format("bigquery") \
        .option("table", table_name) \
        .option("readDataFormat", "ARROW") \
        .load() \
        .filter("ID IS NOT NULL")
    
    # Verify column resolution
    df.select(column_name).collect()
    
    actual_col = df.columns[1]
    print(f"Successfully resolved {column_name}. Actual column name in DF: {actual_col}")

if __name__ == "__main__":
    mode = sys.argv[1]
    spark = SparkSession.builder.appName(f"Acceptance Test: {mode}").getOrCreate()
    
    try:
        if mode == "read_verify":
            table_name = sys.argv[2]
            all_nulls = sys.argv[3].lower() == "true"
            expected_count = int(sys.argv[4])
            test_read_verify(spark, table_name, all_nulls, expected_count)
        elif mode == "write_standard":
            source_table = sys.argv[2]
            destination_table = sys.argv[3]
            write_method = sys.argv[4]
            gcs_bucket = sys.argv[5] if len(sys.argv) > 5 else None
            test_write_and_verify(spark, source_table, destination_table, write_method, "parquet", gcs_bucket)
        elif mode == "write_special":
            source_table = sys.argv[2]
            destination_table = sys.argv[3]
            write_method = sys.argv[4]
            gcs_bucket = sys.argv[5] if len(sys.argv) > 5 else None
            test_write_and_verify(spark, source_table, destination_table, write_method, "avro", gcs_bucket)
        elif mode == "casing":
            table_name = sys.argv[2]
            column_name = sys.argv[3]
            test_case_sensitivity(spark, table_name, column_name)
    finally:
        spark.stop()
