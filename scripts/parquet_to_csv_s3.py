"""
Parquet to CSV Converter - S3 to S3
Reads Parquet files from S3, converts to CSV, writes back to S3
No local file system needed - all in the cloud!
"""

import boto3
import pandas as pd
import io
import sys
from datetime import datetime

# Configuration
SOURCE_BUCKET = 'ecommerce-intelligence-sa'
DEST_BUCKET = 'ecommerce-intelligence-sa'

# Table configurations - maps table name to S3 paths
TABLES = {
    'sentiment_trends': {
        'source_prefix': 'processed/sentiment_trends/',
        'dest_key': 'processed/sentiment_trends/sentiment_trends.csv'
    },
    'competitive_analysis': {
        'source_prefix': 'processed/competitive_analysis/',
        'dest_key': 'processed/competitive_analysis/competitive_analysis.csv'
    },
    'prediction_features': {
        'source_prefix': 'processed/prediction_features/',
        'dest_key': 'processed/prediction_features/prediction_features.csv'
    },
    'velocity_analysis': {
        'source_prefix': 'processed/velocity_analysis/',
        'dest_key': 'processed/velocity_analysis/velocity_analysis.csv'
    }
}

def convert_parquet_to_csv(table_name='sentiment_trends'):
    """
    Convert Parquet files from S3 to CSV in S3
    All processing happens in memory - no local files
    
    Args:
        table_name: Which table to convert (default: sentiment_trends)
    """
    if table_name not in TABLES:
        print(f"❌ Unknown table: {table_name}")
        print(f"Available tables: {list(TABLES.keys())}")
        return
    
    config = TABLES[table_name]
    source_prefix = config['source_prefix']
    dest_key = config['dest_key']
    
    print("=" * 80)
    print(f"PARQUET TO CSV CONVERTER - {table_name.upper()}")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    print(f"\nSource: s3://{SOURCE_BUCKET}/{source_prefix}")
    print(f"Destination: s3://{DEST_BUCKET}/{dest_key}\n")
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    try:
        # List all Parquet files in the source location
        print("📂 Listing Parquet files...")
        response = s3_client.list_objects_v2(
            Bucket=SOURCE_BUCKET,
            Prefix=source_prefix
        )
        
        if 'Contents' not in response:
            print(f"❌ No files found at s3://{SOURCE_BUCKET}/{source_prefix}")
            return
        
        # Filter for .parquet files
        parquet_files = [
            obj['Key'] for obj in response['Contents'] 
            if obj['Key'].endswith('.parquet')
        ]
        
        if not parquet_files:
            print(f"❌ No Parquet files found")
            return
        
        print(f"✅ Found {len(parquet_files)} Parquet file(s):")
        for file in parquet_files:
            print(f"   - {file}")
        
        # Read all Parquet files into DataFrames
        print(f"\n📖 Reading Parquet files from S3...")
        dataframes = []
        
        for file_key in parquet_files:
            print(f"   Reading: {file_key}")
            
            # Download Parquet file to memory
            obj = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
            parquet_bytes = obj['Body'].read()
            
            # Read Parquet from bytes
            df = pd.read_parquet(io.BytesIO(parquet_bytes))
            dataframes.append(df)
            print(f"   ✅ Loaded {len(df):,} rows")
        
        # Combine all DataFrames
        if len(dataframes) > 1:
            print(f"\n🔗 Combining {len(dataframes)} DataFrames...")
            combined_df = pd.concat(dataframes, ignore_index=True)
        else:
            combined_df = dataframes[0]
        
        print(f"✅ Total rows: {len(combined_df):,}")
        print(f"✅ Columns: {combined_df.columns.tolist()}")
        
        # Show data types
        print(f"\n📊 Data types:")
        for col, dtype in combined_df.dtypes.items():
            print(f"   {col:25s} → {dtype}")
        
        # Show sample data
        print(f"\n📝 Sample data (first 3 rows):")
        print(combined_df.head(3).to_string())
        
        # Convert to CSV in memory
        print(f"\n💾 Converting to CSV...")
        csv_buffer = io.StringIO()
        combined_df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')
        
        csv_size_mb = len(csv_bytes) / 1024 / 1024
        print(f"✅ CSV size: {csv_size_mb:.2f} MB")
        
        # Upload CSV to S3
        print(f"\n☁️  Uploading to S3...")
        s3_client.put_object(
            Bucket=DEST_BUCKET,
            Key=dest_key,
            Body=csv_bytes,
            ContentType='text/csv'
        )
        
        print(f"✅ Upload complete!")
        print(f"\n📍 CSV location: s3://{DEST_BUCKET}/{dest_key}")
        
        # Verify upload
        print(f"\n✅ Verifying upload...")
        head_response = s3_client.head_object(Bucket=DEST_BUCKET, Key=dest_key)
        uploaded_size = head_response['ContentLength']
        print(f"   Uploaded size: {uploaded_size / 1024 / 1024:.2f} MB")
        print(f"   Last modified: {head_response['LastModified']}")
        
        print("\n" + "=" * 80)
        print(f"✅ {table_name.upper()} CONVERSION COMPLETE!")
        print("=" * 80)
        print(f"End time: {datetime.now()}")
        print(f"\nNext step - Load into Redshift:")
        print(f"   COPY agg_{table_name}")
        print(f"   FROM 's3://{DEST_BUCKET}/{dest_key}'")
        print(f"   IAM_ROLE 'arn:aws:iam::108782075105:role/service-role/AmazonRedshift-CommandsAccessRole-20251017T193609'")
        print(f"   CSV IGNOREHEADER 1 REGION 'us-east-1';")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def convert_all_tables():
    """
    Convert all 4 feature tables from Parquet to CSV
    CSVs are placed in same folders as Parquet files
    """
    print("=" * 80)
    print("CONVERTING ALL TABLES - PARQUET TO CSV")
    print("=" * 80)
    print(f"Processing {len(TABLES)} tables...\n")
    
    results = {}
    
    for table_name in TABLES.keys():
        print(f"\n{'=' * 80}")
        print(f"TABLE {list(TABLES.keys()).index(table_name) + 1}/{len(TABLES)}: {table_name.upper()}")
        print(f"{'=' * 80}\n")
        
        success = convert_parquet_to_csv(table_name)
        results[table_name] = success
        
        if success:
            print(f"\n✅ {table_name} completed successfully!")
        else:
            print(f"\n❌ {table_name} failed!")
    
    # Summary
    print("\n" + "=" * 80)
    print("CONVERSION SUMMARY")
    print("=" * 80)
    
    for table_name, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{status:12s} - {table_name}")
    
    successful = sum(results.values())
    print(f"\nTotal: {successful}/{len(TABLES)} tables converted successfully")
    
    if successful == len(TABLES):
        print("\n🎉 ALL TABLES CONVERTED!")
        print("\n📋 Redshift COPY Commands:")
        print("=" * 80)
        
        for table_name, config in TABLES.items():
            print(f"\n-- {table_name}")
            print(f"COPY agg_{table_name}")
            print(f"FROM 's3://{DEST_BUCKET}/{config['dest_key']}'")
            print(f"IAM_ROLE 'arn:aws:iam::108782075105:role/service-role/AmazonRedshift-CommandsAccessRole-20251017T193609'")
            print(f"CSV IGNOREHEADER 1 REGION 'us-east-1';")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert Parquet files to CSV in S3')
    parser.add_argument('--all', action='store_true', help='Convert all 4 tables')
    parser.add_argument('--table', type=str, choices=list(TABLES.keys()), 
                       help='Convert specific table')
    
    args = parser.parse_args()
    
    if args.all:
        # Convert all tables
        convert_all_tables()
    elif args.table:
        # Convert specific table
        convert_parquet_to_csv(args.table)
    else:
        # Default: convert all tables
        print("No arguments provided. Converting all tables...")
        print("Use --table <name> to convert specific table")
        print("Use --all to explicitly convert all tables\n")
        convert_all_tables()
