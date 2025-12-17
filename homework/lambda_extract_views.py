import requests
import json
import datetime
import boto3
import os

def lambda_handler(event, context):
    # Configuration - matches your extract_edits style
    USER_NAME = "luciana" 
    BUCKET_NAME = f"{USER_NAME}-wikidata"
    
    # 1. Handle Date Logic
    # Check if a date was passed in the test event: {"date": "YYYY-MM-DD"}
    if "date" in event and event["date"]:
        query_date_str = event["date"]
        print(f"Using provided date: {query_date_str}")
    else:
        
        query_date = datetime.datetime.now() - datetime.timedelta(days=21)
        query_date_str = query_date.strftime("%Y-%m-%d")
        print(f"Using date: {query_date_str}")
    
    # 2. Extract: Fetch from Wikipedia Pageviews API
    url_date = query_date_str.replace("-", "/")
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{url_date}"
    
    # Use proper User-Agent as demonstrated in your tutorial
    headers = {
        "User-Agent": f"WikiLambda/1.0 ({USER_NAME}@example.com)"
    }
    
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        
        # 3. Transform: Create JSON Lines
        # Use correct path ["items"][0]["articles"] as found in your debug step
        top_articles = data["items"][0]["articles"]
        current_time = datetime.datetime.now(datetime.timezone.utc)
        
        json_lines = ""
        for page in top_articles:
            record = {
                "title": page["article"],
                "views": page["views"],
                "rank": page["rank"],
                "date": query_date_str,
                "retrieved_at": current_time.replace(tzinfo=None).isoformat()
            }
            json_lines += json.dumps(record) + "\n"
            
        # 4. Load: Upload to S3
        s3 = boto3.client('s3')
        s3_key = f"raw-views/raw-views-{query_date_str}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json_lines
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully processed {len(top_articles)} records for {query_date_str}")
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing {query_date_str}: {str(e)}")
        }