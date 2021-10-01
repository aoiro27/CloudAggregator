import requests
import base64
import json
import hmac
import hashlib
import urllib.parse
import boto3
from requests_oauthlib import OAuth1Session
import os

CONSUMER_KEY        = os.environ["CONSUMER_KEY"]
CONSUMER_SECRET_KEY = os.environ["CONSUMER_SECRET_KEY"]
ACCESS_TOKEN        = os.environ["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = os.environ["ACCESS_TOKEN_SECRET"]
AZURE_COLLECTION_ID = os.environ["AZURE_COLLECTION_ID"]
AWS_COLLECTION_ID = os.environ["AWS_COLLECTION_ID"]
GCP_COLLECTION_ID = os.environ["GCP_COLLECTION_ID"]
AWSFAILURE_COLLECTION_ID = os.environ["AWSFAILURE_COLLECTION_ID"]
AZUREFAILURE_COLLECTION_ID = os.environ["AZUREFAILURE_COLLECTION_ID"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_DB_NAME = os.environ["S3_DB_NAME"]
twitter = OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

def twitter_search(query):
    results = twitter.get('https://api.twitter.com/1.1/search/tweets.json' + query)
    return json.loads(results.text)

def get_searchquery(id,refresh_url):
    count=100
    query = '?q=' + id + '%20-rt%20lang%3Aja&src=typed_query&f=live&result_type=recent&count=' + str(count)
    if refresh_url != "":
        query = refresh_url
    return query
    
def put_collections(collection_id,tweets):
    
    data = {}
    data["id"] = collection_id
    d = []
    msg = []
    
    for r in tweets["statuses"]:
        msg.append({"tweet_id":r["id"],"created_at": r["created_at"]})
    
    msg.sort(key=lambda x: x["created_at"])
    
    for m in msg:
        d.append({"op":"add","tweet_id":m["tweet_id"]})
    
    data["changes"] = d
    print(data)
    results = twitter.post('https://api.twitter.com/1.1/collections/entries/curate.json',data=json.dumps(data))
    print(results.text)
    
def lambda_handler(event, context):
    
    S3_client = boto3.client('s3')    
    #DBから情報取得
    response = S3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_DB_NAME)
    sincelist = json.loads(response["Body"].read())
    
    #azure#########################################
    a = [x for x in sincelist if x['id'] == "azure"]
    index = sincelist.index(a[0])
    
    query = get_searchquery("%23azure",sincelist[index]["refresh_url"])
    print(query)
    results = twitter_search(query)
    put_collections(AZURE_COLLECTION_ID,results)
    sincelist[index]["refresh_url"] = results["search_metadata"]["refresh_url"]
    S3_client.put_object(Body=json.dumps(sincelist,ensure_ascii=False), Bucket=S3_BUCKET_NAME, Key=S3_DB_NAME)
    #azure#########################################
    
    
    #aws#########################################
    a = [x for x in sincelist if x['id'] == "aws"]
    index = sincelist.index(a[0])
    
    query = get_searchquery("%23aws",sincelist[index]["refresh_url"])
    print(query)
    results = twitter_search(query)
    put_collections(AWS_COLLECTION_ID,results)
    sincelist[index]["refresh_url"] = results["search_metadata"]["refresh_url"]
    S3_client.put_object(Body=json.dumps(sincelist,ensure_ascii=False), Bucket=S3_BUCKET_NAME, Key=S3_DB_NAME)
    #aws#########################################
    
    #gcp#########################################
    a = [x for x in sincelist if x['id'] == "gcp"]
    index = sincelist.index(a[0])
    
    query = get_searchquery("%23gcp",sincelist[index]["refresh_url"])
    print(query)
    results = twitter_search(query)
    put_collections(GCP_COLLECTION_ID,results)
    sincelist[index]["refresh_url"] = results["search_metadata"]["refresh_url"]
    S3_client.put_object(Body=json.dumps(sincelist,ensure_ascii=False), Bucket=S3_BUCKET_NAME, Key=S3_DB_NAME)
    #gcp#########################################
    
    #awsfailure#########################################
    a = [x for x in sincelist if x['id'] == "awsfailure"]
    index = sincelist.index(a[0])
    
    query = get_searchquery("aws障害",sincelist[index]["refresh_url"])
    print(query)
    results = twitter_search(query)
    put_collections(AWSFAILURE_COLLECTION_ID,results)
    sincelist[index]["refresh_url"] = results["search_metadata"]["refresh_url"]
    S3_client.put_object(Body=json.dumps(sincelist,ensure_ascii=False), Bucket=S3_BUCKET_NAME, Key=S3_DB_NAME)
    #awsfailure#########################################    
    
    #azurefailure#########################################
    a = [x for x in sincelist if x['id'] == "azurefailure"]
    index = sincelist.index(a[0])
    
    query = get_searchquery("azure障害",sincelist[index]["refresh_url"])
    print(query)
    results = twitter_search(query)
    put_collections(AZUREFAILURE_COLLECTION_ID,results)
    sincelist[index]["refresh_url"] = results["search_metadata"]["refresh_url"]
    S3_client.put_object(Body=json.dumps(sincelist,ensure_ascii=False), Bucket=S3_BUCKET_NAME, Key=S3_DB_NAME)
    #azurefailure#########################################