# E-Commerce Review Intelligence Platform

AI-powered analytics chatbot that processes millions of Amazon product reviews to provide actionable business insights through natural language queries.

---

## 🎯 Overview

This project demonstrates end-to-end data engineering and AI integration by building a conversational analytics platform. Users can ask natural language questions about product performance, customer sentiment, and market trends without writing SQL.

**Example Queries:**
- "What are the top products by customer satisfaction?"
- "Which products have declining momentum?"
- "What do customers say about the iPad Stylus?"

---

## 🏗️ Architecture

```
McAuley Amazon Dataset (43.9M reviews)
    ↓
Sampling & Processing (PySpark on AWS EMR)
    ↓
Data Lake (AWS S3)
    ↓
Data Warehouse (AWS Redshift Serverless)
    ↓
RAG Backend (AWS Lambda + Bedrock Claude)
    ↓
User Interface (Web/Local)
```

---

## 📊 Data Pipeline

### Source Data
- **Dataset:** [McAuley-Lab Amazon Review Data](https://amazon-reviews-2023.github.io/) (UC San Diego)
- **Scale:** 43.9 million electronics reviews + metadata
- **Timeframe:** 2018-2023

### Processing
1. **Sampling:** Reduced to 2M reviews based on product diversity and review volume
2. **Transformation:** PySpark aggregations on AWS EMR
3. **Feature Engineering:** Created 4 analytical tables:
   - `sentiment_trends` - Monthly sentiment analysis by store
   - `competitive_analysis` - Product-level metrics with review samples
   - `success_prediction` - ML-based success scoring
   - `review_velocity` - Momentum and growth tracking

### Storage
- **Raw Data:** S3 (Parquet format)
- **Processed Data:** Redshift Serverless (auto-pause enabled)
- **Demo Sample:** 47 products for cost-efficient demonstration

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| **Data Processing** | PySpark, AWS EMR |
| **Storage** | AWS S3, Redshift Serverless |
| **AI/LLM** | AWS Bedrock (Claude Sonnet 4.5) |
| **Backend** | AWS Lambda, Python 3.11 |
| **Infrastructure** | AWS (IAM, CloudWatch) |
| **Notebooks** | Jupyter |

---

## 🚀 Key Features

✅ **Natural Language Queries** - No SQL knowledge required  
✅ **SQL Generation** - Claude converts questions to optimized queries  
✅ **Review Text Analysis** - Analyzes actual customer feedback  
✅ **Scalable Architecture** - Production-ready AWS infrastructure  
✅ **Cost Optimized** - Serverless components with auto-pause  

---

## 📁 Project Structure

```
ecommerce-intelligence/
├── demo/
│   ├── Chatbot Demo.mp4          # Video demonstration
│   └── chatbot_interface.html    # Web UI (Lambda URL removed)
│
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_feature_engineering.ipynb
│   ├── 03_metadata.ipynb
│   └── 04_Transformation_Pipeline.ipynb
│
├── processed data/
│   ├── competitive_analysis_with_reviews.csv
│   ├── prediction_features.csv
│   ├── sentiment_trends.csv
│   └── velocity_analysis.csv
│
└── scripts/
    ├── lambda_function.py        # AWS Lambda backend
    ├── pyspark_pipeline.py       # EMR processing pipeline
    ├── redshift_schema.sql       # Data warehouse schema
    └── parquet_to_csv_s3.py      # Format conversion utility
```

---

## 💡 Technical Highlights

### Data Engineering
- **Big Data Processing:** Handled 43.9M → 2M record sampling with distributed computing
- **ETL Pipeline:** Automated data flow from raw S3 → processed Redshift tables
- **Schema Design:** Optimized for analytical queries with proper indexing and distribution keys

### AI/ML Integration
- **RAG Architecture:** Retrieval-Augmented Generation using structured data
- **Prompt Engineering:** System prompts guide Claude to generate correct SQL syntax
- **Context Management:** Maintains conversation flow for follow-up questions

### Cloud Infrastructure
- **Serverless:** Lambda + Redshift auto-pause minimize costs (~$1-5/month)
- **Scalability:** Architecture supports millions of products with minimal code changes
- **Security:** IAM roles, VPC integration, proper CORS configuration

---

## 📈 Sample Insights

The system can answer questions like:

**Product Performance:**
- Top/bottom performers by satisfaction rate
- Products with high review volume but declining sentiment
- Comparison across product categories

**Customer Feedback:**
- Common themes in positive/negative reviews
- Verified vs. unverified purchase patterns
- Review quality and engagement metrics

**Trend Analysis:**
- Sentiment changes over time by store
- Seasonal patterns in review velocity
- Momentum shifts (surging vs. declining products)

---

## 🎓 Key Learnings

1. **Schema Compatibility:** Solved PySpark Parquet → Redshift type mismatches
2. **Cost Optimization:** Implemented auto-pause and serverless architecture
3. **RAG Design:** Structured data RAG differs from vector-based document retrieval
4. **Production Patterns:** Error handling, logging, and monitoring best practices

---

## 🔮 Future Enhancements

- **Real-time Updates:** Streaming pipeline with Kinesis
- **Advanced ML:** Predictive models for product success
- **Visualization:** Interactive dashboards with QuickSight
- **Multi-modal:** Image analysis of product photos
- **Scale:** Expand to all Amazon categories (100M+ reviews)

---

## 📊 Performance Metrics

- **Data Volume:** 2M reviews → 6K aggregated records
- **Query Response:** 5-15 seconds (includes AI processing)
- **Cold Start:** ~25 seconds (Lambda initialization)
- **Warm Queries:** 8-12 seconds
- **Cost:** <$5/month with serverless architecture

---

## 🔗 Related Resources

- [McAuley Amazon Review Dataset](https://amazon-reviews-2023.github.io/)
- [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

---

## 👤 Author

**Shafeen Ahmed**  
Data Engineer | AI/ML Enthusiast  

*This project demonstrates end-to-end data engineering, cloud architecture, and modern AI integration capabilities.*

---

## 📄 License

Data sourced from McAuley-Lab Amazon Review Dataset (UC San Diego).  
Project code available for educational and portfolio purposes.
