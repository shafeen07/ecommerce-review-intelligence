"""
E-Commerce Review Intelligence - FIXED Production PySpark Pipeline
FIX: Schema compatibility for Redshift Spectrum loads
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, avg, sum, min, max, stddev,
    when, lit, regexp_replace, round as spark_round,
    year, month, date_trunc, to_date, from_unixtime,
    datediff, lag, length, countDistinct
)
from pyspark.sql.types import DoubleType, StringType, ArrayType, StructType, StructField, IntegerType, DateType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initialize Spark session with optimal configs for EMR"""
    return SparkSession.builder \
        .appName("EcommerceReviewIntelligence") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_data(spark, bucket_name):
    """Load reviews and metadata from S3"""
    logger.info("Loading data from S3...")
    
    # Load reviews
    reviews_path = f"s3://{bucket_name}/raw/reviews/electronics_sample_2M.jsonl.gz"
    df_reviews = spark.read.json(reviews_path)
    logger.info(f"Loaded {df_reviews.count():,} reviews")
    
    # Define metadata schema
    schema = StructType([
        StructField("parent_asin", StringType(), True),
        StructField("title", StringType(), True),
        StructField("main_category", StringType(), True),
        StructField("store", StringType(), True),
        StructField("price", StringType(), True),
        StructField("features", ArrayType(StringType()), True),
        StructField("description", ArrayType(StringType()), True),
        StructField("average_rating", DoubleType(), True),
        StructField("rating_number", IntegerType(), True),
    ])
    
    # Load metadata
    metadata_path = f"s3://{bucket_name}/raw/metadata/meta_Electronics.jsonl.gz"
    df_meta = spark.read.schema(schema).json(metadata_path)
    logger.info(f"Loaded {df_meta.count():,} products")
    
    return df_reviews, df_meta

def prepare_data(df_reviews, df_meta):
    """Join and enrich reviews with metadata"""
    logger.info("Preparing enriched dataset...")
    
    # Filter metadata to sample products
    sample_products = df_reviews.select("parent_asin").distinct()
    df_meta_sample = df_meta.join(sample_products, on="parent_asin", how="inner")
    
    # Rename to avoid column collisions
    df_meta_renamed = df_meta_sample.select(
        col("parent_asin"),
        col("title").alias("product_title"),
        col("main_category"),
        col("store"),
        col("price"),
        col("features"),
        col("description")
    )
    
    # Join reviews with metadata
    df_enriched = df_reviews.join(df_meta_renamed, on="parent_asin", how="left") \
        .withColumnRenamed("title", "review_title")
    
    # Add temporal columns - FIX: Cast to DATE type explicitly
    df_enriched = df_enriched.withColumn(
        "review_date", 
        to_date(from_unixtime(col("timestamp") / 1000))
    ).withColumn(
        "review_year_month",
        to_date(date_trunc("month", "review_date"))  # FIX: Cast TIMESTAMP to DATE
    )
    
    # Parse price
    df_enriched = df_enriched.withColumn(
        "price_numeric",
        regexp_replace(col("price"), "[$,]", "").cast(DoubleType())
    ).withColumn(
        "price_tier",
        when(col("price_numeric").isNull(), "Unknown")
        .when(col("price_numeric") < 20, "Budget (<$20)")
        .when(col("price_numeric") < 50, "Mid ($20-50)")
        .when(col("price_numeric") < 100, "Premium ($50-100)")
        .otherwise("Luxury ($100+)")
    )
    
    # Sentiment categories
    df_enriched = df_enriched.withColumn(
        "sentiment",
        when(col("rating") >= 4, "Positive")
        .when(col("rating") <= 2, "Negative")
        .otherwise("Neutral")
    )
    
    # Text quality
    df_enriched = df_enriched.withColumn(
        "review_length", length(col("text"))
    ).withColumn(
        "is_detailed_review",
        when(col("review_length") >= 200, 1).otherwise(0)
    )
    
    df_enriched.cache()
    logger.info(f"Enriched dataset prepared: {df_enriched.count():,} rows")
    
    return df_enriched

def build_sentiment_trends(df_enriched):
    """Feature 1: Sentiment trends over time - FIXED for Redshift"""
    logger.info("Building Feature 1: Sentiment Trends...")
    
    sentiment_trends = df_enriched.groupBy(
        "store", 
        "review_year_month"
    ).agg(
        count("*").alias("review_count"),
        avg("rating").alias("avg_rating"),
        count(when(col("sentiment") == "Positive", 1)).alias("positive_count"),
        count(when(col("sentiment") == "Negative", 1)).alias("negative_count"),
        count(when(col("sentiment") == "Neutral", 1)).alias("neutral_count")
    ).withColumn(
        "positive_rate",
        spark_round((col("positive_count") / col("review_count") * 100), 2)
    ).withColumn(
        "negative_rate",
        spark_round((col("negative_count") / col("review_count") * 100), 2)
    ).orderBy("store", "review_year_month")
    
    # Calculate month-over-month changes
    windowSpec = Window.partitionBy("store").orderBy("review_year_month")
    sentiment_trends = sentiment_trends.withColumn(
        "prev_avg_rating",
        lag("avg_rating", 1).over(windowSpec)
    ).withColumn(
        "rating_change",
        spark_round(col("avg_rating") - col("prev_avg_rating"), 3)
    ).withColumn(
        "trend_direction",
        when(col("rating_change") > 0.1, "Improving")
        .when(col("rating_change") < -0.1, "Declining")
        .otherwise("Stable")
    )
    
    # FIX #1: Rename column to match Redshift schema
    sentiment_trends = sentiment_trends.withColumnRenamed("review_year_month", "year_month")
    
    # FIX #2: Drop prev_avg_rating (not in Redshift schema)
    # OPTION A: Drop the column
    sentiment_trends = sentiment_trends.drop("prev_avg_rating")
    
    # FIX #3: Ensure avg_rating is properly rounded to 2 decimals
    sentiment_trends = sentiment_trends.withColumn(
        "avg_rating",
        spark_round(col("avg_rating"), 2)
    )
    
    # FIX #4: Reorder columns to match Redshift schema exactly
    sentiment_trends = sentiment_trends.select(
        "store",
        "year_month",
        "review_count",
        "avg_rating",
        "positive_count",
        "negative_count",
        "neutral_count",
        "positive_rate",
        "negative_rate",
        "rating_change",
        "trend_direction"
    )
    
    logger.info(f"Sentiment trends calculated: {sentiment_trends.count():,} data points")
    return sentiment_trends

def build_competitive_analysis(df_enriched):
    """Feature 3: Competitive product analysis - FIXED for Redshift"""
    logger.info("Building Feature 3: Competitive Analysis...")
    
    competitive_analysis = df_enriched.groupBy(
        "parent_asin",
        "product_title",
        "store",
        "main_category",
        "price_tier"
    ).agg(
        count("*").alias("total_reviews"),
        countDistinct("user_id").alias("unique_reviewers"),
        avg("rating").alias("avg_rating"),
        stddev("rating").alias("rating_stddev"),
        count(when(col("sentiment") == "Positive", 1)).alias("positive_reviews"),
        count(when(col("sentiment") == "Negative", 1)).alias("negative_reviews"),
        count(when(col("sentiment") == "Neutral", 1)).alias("neutral_reviews"),
        avg("review_length").alias("avg_review_length"),
        sum("is_detailed_review").alias("detailed_reviews"),
        count(when(col("verified_purchase") == True, 1)).alias("verified_purchases"),
        avg("helpful_vote").alias("avg_helpful_votes"),
        min("review_date").alias("first_review_date"),
        max("review_date").alias("last_review_date")
    ).withColumn(
        "satisfaction_rate",
        spark_round((col("positive_reviews") / col("total_reviews") * 100), 2)
    ).withColumn(
        "negative_rate", 
        spark_round((col("negative_reviews") / col("total_reviews") * 100), 2)
    ).withColumn(
        "verified_rate",
        spark_round((col("verified_purchases") / col("total_reviews") * 100), 2)
    ).withColumn(
        "review_span_days",
        datediff(col("last_review_date"), col("first_review_date"))
    )
    
    # FIX: Round all decimal columns to match Redshift precision
    competitive_analysis = competitive_analysis \
        .withColumn("avg_rating", spark_round(col("avg_rating"), 2)) \
        .withColumn("rating_stddev", spark_round(col("rating_stddev"), 2)) \
        .withColumn("avg_review_length", spark_round(col("avg_review_length"), 2)) \
        .withColumn("avg_helpful_votes", spark_round(col("avg_helpful_votes"), 2))
    
    # Reorder columns to match Redshift schema
    competitive_analysis = competitive_analysis.select(
        "parent_asin",
        "product_title",
        "store",
        "main_category",
        "price_tier",
        "total_reviews",
        "unique_reviewers",
        "avg_rating",
        "rating_stddev",
        "positive_reviews",
        "negative_reviews",
        "neutral_reviews",
        "satisfaction_rate",
        "negative_rate",
        "avg_review_length",
        "detailed_reviews",
        "verified_purchases",
        "verified_rate",
        "avg_helpful_votes",
        "first_review_date",
        "last_review_date",
        "review_span_days"
    ).orderBy(col("total_reviews").desc())
    
    logger.info(f"Competitive analysis: {competitive_analysis.count():,} products")
    return competitive_analysis

def build_success_prediction(competitive_analysis):
    """Feature 4: Product success prediction - FIXED for Redshift"""
    logger.info("Building Feature 4: Success Prediction...")
    
    prediction_features = competitive_analysis.withColumn(
        "success_score",
        spark_round(
            (col("satisfaction_rate") * 0.4) +
            ((100 - col("rating_stddev") * 20) * 0.3) +
            ((col("verified_rate")) * 0.3),
            2
        )
    ).withColumn(
        "review_velocity",
        spark_round(col("total_reviews") / (col("review_span_days") + 1), 2)
    ).withColumn(
        "engagement_score",
        spark_round(
            (col("avg_review_length") / 100) * (col("avg_helpful_votes") + 1),
            2
        )
    ).withColumn(
        "success_category",
        when(
            (col("satisfaction_rate") >= 85) & 
            (col("total_reviews") >= 5000), 
            "Top Performer"
        ).when(
            (col("satisfaction_rate") >= 80) & 
            (col("total_reviews") >= 1000), 
            "Strong Performer"
        ).when(
            col("satisfaction_rate") >= 75, 
            "Average Performer"
        ).otherwise("Underperformer")
    )
    
    # Select columns matching Redshift schema
    prediction_features = prediction_features.select(
        "parent_asin",
        "product_title",
        "store",
        "success_score",
        "success_category",
        "satisfaction_rate",
        "rating_stddev",
        "verified_rate",
        "review_velocity",
        "engagement_score"
    )
    
    logger.info(f"Success prediction features created: {prediction_features.count():,} products")
    return prediction_features

def build_velocity_analysis(df_enriched):
    """Feature 5: Review velocity analysis - FIXED for Redshift"""
    logger.info("Building Feature 5: Velocity Analysis...")
    
    velocity_analysis = df_enriched.groupBy(
        "parent_asin",
        "product_title",
        "store",
        "review_year_month"
    ).agg(
        count("*").alias("monthly_reviews"),
        avg("rating").alias("monthly_avg_rating")
    ).orderBy("parent_asin", "review_year_month")
    
    # Calculate velocity changes
    windowSpec = Window.partitionBy("parent_asin").orderBy("review_year_month")
    velocity_analysis = velocity_analysis.withColumn(
        "prev_month_reviews",
        lag("monthly_reviews", 1).over(windowSpec)
    ).withColumn(
        "velocity_change",
        col("monthly_reviews") - col("prev_month_reviews")
    ).withColumn(
        "velocity_change_pct",
        spark_round(
            ((col("monthly_reviews") - col("prev_month_reviews")) / 
             (col("prev_month_reviews") + 1) * 100),
            2
        )
    ).withColumn(
        "momentum",
        when(col("velocity_change_pct") > 50, "Surging")
        .when(col("velocity_change_pct") > 20, "Growing")
        .when(col("velocity_change_pct") > -20, "Stable")
        .when(col("velocity_change_pct") > -50, "Declining")
        .otherwise("Collapsing")
    )
    
    # FIX #1: Rename column to match Redshift
    velocity_analysis = velocity_analysis.withColumnRenamed("review_year_month", "year_month")
    
    # FIX #2: Round decimal columns
    velocity_analysis = velocity_analysis \
        .withColumn("monthly_avg_rating", spark_round(col("monthly_avg_rating"), 2))
    
    # FIX #3: Cast prev_month_reviews to INTEGER (it's FLOAT from lag())
    velocity_analysis = velocity_analysis \
        .withColumn("prev_month_reviews", col("prev_month_reviews").cast(IntegerType())) \
        .withColumn("velocity_change", col("velocity_change").cast(IntegerType()))
    
    # Reorder columns to match Redshift schema
    velocity_analysis = velocity_analysis.select(
        "parent_asin",
        "product_title",
        "store",
        "year_month",
        "monthly_reviews",
        "monthly_avg_rating",
        "prev_month_reviews",
        "velocity_change",
        "velocity_change_pct",
        "momentum"
    )
    
    logger.info(f"Velocity analysis: {velocity_analysis.count():,} data points")
    return velocity_analysis

def save_results(sentiment_trends, competitive_analysis, prediction_features, 
                 velocity_analysis, bucket_name):
    """Save all features to S3 as Parquet"""
    logger.info("Saving results to S3...")
    
    base_path = f"s3://{bucket_name}/processed"
    
    sentiment_trends.write.mode("overwrite").parquet(f"{base_path}/sentiment_trends/")
    logger.info("✓ Sentiment trends saved")
    
    competitive_analysis.write.mode("overwrite").parquet(f"{base_path}/competitive_analysis/")
    logger.info("✓ Competitive analysis saved")
    
    prediction_features.write.mode("overwrite").parquet(f"{base_path}/prediction_features/")
    logger.info("✓ Prediction features saved")
    
    velocity_analysis.write.mode("overwrite").parquet(f"{base_path}/velocity_analysis/")
    logger.info("✓ Velocity analysis saved")
    
    logger.info("All features saved successfully!")

def main():
    """Main pipeline execution"""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("E-COMMERCE REVIEW INTELLIGENCE PIPELINE - STARTED")
    logger.info("="*80)
    
    # Get bucket name from command line or use default
    bucket_name = sys.argv[1] if len(sys.argv) > 1 else "ecommerce-intelligence-sa"
    logger.info(f"Using S3 bucket: {bucket_name}")
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        logger.info(f"Spark session created: {spark.version}")
        
        # Load data
        df_reviews, df_meta = load_data(spark, bucket_name)
        
        # Prepare enriched dataset
        df_enriched = prepare_data(df_reviews, df_meta)
        
        # Build all features
        sentiment_trends = build_sentiment_trends(df_enriched)
        competitive_analysis = build_competitive_analysis(df_enriched)
        prediction_features = build_success_prediction(competitive_analysis)
        velocity_analysis = build_velocity_analysis(df_enriched)
        
        # Save results
        save_results(
            sentiment_trends, 
            competitive_analysis, 
            prediction_features, 
            velocity_analysis, 
            bucket_name
        )
        
        # Success!
        elapsed = datetime.now() - start_time
        logger.info("="*80)
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {elapsed}")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
