"""
E-Commerce RAG Chatbot - DEMO VERSION
Uses Pandas for fast in-memory queries with review text
"""

import json
import boto3
import pandas as pd
from io import StringIO

# Initialize Bedrock client
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-1')

# CORS headers
CORS_HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
}

# System prompt with enhanced schema including review text
SYSTEM_PROMPT = """You are an expert e-commerce data analyst with access to Amazon review analytics from 2 million reviews.

AVAILABLE TABLES:

1. agg_sentiment_trends: Monthly sentiment over time
   Columns: store, year_month, review_count, avg_rating, positive_count, negative_count, 
   neutral_count, positive_rate, negative_rate, rating_change, trend_direction

2. agg_competitive_analysis: Product metrics WITH sample customer reviews
   Columns: parent_asin, product_title, store, total_reviews, avg_rating, satisfaction_rate,
   verified_rate, sample_reviews (contains actual customer review text separated by '|')

3. agg_success_prediction: ML-based success scores
   Columns: parent_asin, product_title, store, success_score, success_category, 
   satisfaction_rate, review_velocity, engagement_score

4. agg_review_velocity: Monthly momentum tracking
   Columns: parent_asin, product_title, store, year_month, monthly_reviews, 
   monthly_avg_rating, momentum

IMPORTANT: The sample_reviews column contains actual customer feedback. When asked about 
customer opinions or "what customers say", include sample_reviews in your query and summarize them.

Generate SQL queries to answer questions. Use SQLite syntax (Pandas uses SQLite under the hood).
Return queries in <sql>...</sql> tags.

IMPORTANT SQL GUIDELINES:
- Use SQLite syntax (simple JOINs, subqueries in WHERE clause)
- Avoid CTEs (WITH clauses) - use subqueries instead
- Keep queries simple when possible
- Use LIMIT to restrict results

Generate SQL queries. Return in <sql>...</sql> tags."""



# Load dataframes on cold start (global scope)
df_competitive = None
df_sentiment = None
df_success = None
df_velocity = None

def initialize_data():
    """Load CSVs into Pandas DataFrames on Lambda cold start"""
    global df_competitive, df_sentiment, df_success, df_velocity
    
    if df_competitive is not None:
        return  # Already initialized
    
    print("Loading CSV data into memory...")
    
    try:
        df_competitive = pd.read_csv('competitive_analysis_with_reviews.csv')
        print(f"Loaded competitive_analysis: {len(df_competitive)} rows")
        
        df_sentiment = pd.read_csv('sentiment_trends.csv')
        print(f"Loaded sentiment_trends: {len(df_sentiment)} rows")
        
        df_success = pd.read_csv('prediction_features.csv')
        print(f"Loaded success_prediction: {len(df_success)} rows")
        
        df_velocity = pd.read_csv('velocity_analysis.csv')
        print(f"Loaded velocity_analysis: {len(df_velocity)} rows")
        
        print("All data loaded successfully!")
        
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

def execute_sql_query(sql):
    """Execute SQL query using Pandas"""
    try:
        print(f"Executing SQL: {sql}")
        
        # Create temporary SQLite connection for query execution
        import sqlite3
        conn = sqlite3.connect(':memory:')
        
        # Load dataframes into SQLite
        df_competitive.to_sql('agg_competitive_analysis', conn, index=False, if_exists='replace')
        df_sentiment.to_sql('agg_sentiment_trends', conn, index=False, if_exists='replace')
        df_success.to_sql('agg_success_prediction', conn, index=False, if_exists='replace')
        df_velocity.to_sql('agg_review_velocity', conn, index=False, if_exists='replace')
        
        # Execute query
        result_df = pd.read_sql_query(sql, conn)
        conn.close()
        
        print(f"Query returned {len(result_df)} rows")
        
        # Format results as text
        if len(result_df) == 0:
            return {'success': True, 'row_count': 0, 'data': 'No results found'}
        
        # Convert to string representation
        result_text = "Results:\n"
        result_text += " | ".join(result_df.columns) + "\n"
        
        for idx, row in result_df.head(20).iterrows():
            formatted_row = []
            for val in row:
                if pd.isna(val):
                    formatted_row.append('NULL')
                elif isinstance(val, float):
                    formatted_row.append(f"{val:.2f}")
                else:
                    formatted_row.append(str(val))
            result_text += " | ".join(formatted_row) + "\n"
        
        return {'success': True, 'row_count': len(result_df), 'data': result_text}
        
    except Exception as e:
        print(f"SQL execution error: {str(e)}")
        return {'error': str(e)}

def extract_sql(text):
    """Extract SQL query from Claude's response"""
    if '<sql>' in text and '</sql>' in text:
        start = text.find('<sql>') + 5
        end = text.find('</sql>')
        return text[start:end].strip()
    return None

def lambda_handler(event, context):
    """Main Lambda handler"""
    try:
        print(f"Received event: {json.dumps(event)}")
        
        # Initialize data on cold start
        initialize_data()
        
        # Handle OPTIONS preflight request
        if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': CORS_HEADERS,
                'body': ''
            }
        
        # Parse request
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event
        
        user_query = body.get('query', '')
        conversation_history = body.get('conversation_history', [])
        print(f"User query: {user_query}")
        print(f"Conversation history: {len(conversation_history)} messages")

        if not user_query:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Missing query parameter'})
            }
        
        # Call Claude to generate SQL
        print("Calling Bedrock Claude...")

        # Build messages including conversation history
        messages = conversation_history + [{"role": "user", "content": [{"text": user_query}]}]

        response = bedrock_runtime.converse(
            modelId="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
            messages=[{"role": "user", "content": [{"text": user_query}]}],
            system=[{"text": SYSTEM_PROMPT}],
            inferenceConfig={"maxTokens": 2000, "temperature": 0.3}
        )
        
        claude_response = response['output']['message']['content'][0]['text']
        print(f"Claude response length: {len(claude_response)} chars")
        
        # Extract and execute SQL
        sql_query = extract_sql(claude_response)
        print(f"Extracted SQL: {sql_query}")
        
        if sql_query:
            query_result = execute_sql_query(sql_query)
            
            if 'error' not in query_result:
                # Give results back to Claude for interpretation
                print("Calling Claude for interpretation...")
                # Build messages with conversation history for interpretation
                interpret_messages = conversation_history + [
                    {"role": "user", "content": [{"text": user_query}]},
                    {"role": "assistant", "content": [{"text": claude_response}]},
                    {"role": "user", "content": [{"text": f"Query results:\n{query_result['data']}\n\nProvide detailed insights and analysis."}]}
                ]
                
                interpret_response = bedrock_runtime.converse(
                    modelId="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                    messages=interpret_messages,
                    system=[{"text": SYSTEM_PROMPT}],
                    inferenceConfig={"maxTokens": 2000, "temperature": 0.5}
                )
                
                final_response = interpret_response['output']['message']['content'][0]['text']
                print("Successfully generated final response")
                
                return {
                    'statusCode': 200,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({
                        'response': final_response,
                        'sql_executed': sql_query,
                        'row_count': query_result['row_count'],
                        'success': True
                    })
                }
            else:
                print(f"Query execution failed: {query_result['error']}")
                return {
                    'statusCode': 500,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'error': query_result['error'], 'success': False})
                }
        
        # No SQL needed
        print("No SQL query extracted, returning direct response")
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'response': claude_response, 'success': True})
        }
        
    except Exception as e:
        print(f"Lambda handler error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e), 'success': False})
        }