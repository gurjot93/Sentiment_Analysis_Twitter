import csv
import tweepy
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers, Elasticsearch
from datetime import datetime

#region CONNECTION AND DECALARATIONS
consumerKey = "jVvsx5cvnvOsriDIVl4Sv28MB"
consumerSecret = "irR4mFwviHNiCBDmDDOkiqa9YsEdNDNEn2BXYfaByV8R6eCSPo"
accessToken = "1044941494401077248-oL6mfkb4gfWtkYUwHNrUvjyKAQaXXY"
accessTokenSecret = "iKcBFtxPhrHVbM7XyPPNWSgK7REetu2GnKkrRO2vqmp7X"

#generating authentication for twitter tweepy
auth = tweepy.OAuthHandler(consumerKey,consumerSecret)
auth.set_access_token(accessToken,accessTokenSecret)
api = tweepy.API(auth)

polarity=0
sentimental_analysis=[]
polarity_list=[]
polarity_list1=[]
listItem=[]
cleanTweetFile = 'clean_tweets.csv'
tweetsFile = 'tweets_user.csv'
sentimentFile = 'sentimental_analysis.csv'
lexiconFile = "concreteness.json"

#endregion

#region USER INPUT FOR TWEETS
def FetchTweets():
    inputData = input("Enter Twitter profilename in comma separated format: ")
    yourstring = inputData.split(",")
    queries = []
    for item in yourstring:
        queries.append("@{0}".format(item)) #replace the (,) with (@) and appending into queries[]
    return queries

#endregion

#region WRITE TWEETS
def getProfileTweets(query):
    api = tweepy.API(auth)
    try:
        new_tweets = api.user_timeline(query,count=200) #get 200 tweets from a Twitter profileName
    except tweepy.error.TweepError as e:
        tweets = [json.loads(e.reason.text)] #exception 
    return new_tweets


#function to fetch tweets from Tweepy Api to a csv file
def WriteFeeds(queries):
    with open(tweetsFile, 'w', newline='',encoding="utf-8-sig") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['id','user','created_at','text'])
        for query in queries:
            new_tweets = getProfileTweets(query)
            for tweet in new_tweets:
                writer.writerow([tweet.id_str,tweet.user.screen_name,tweet.created_at,tweet.text])
#endregion

#region FILTER FEEDS
def ReplaceChar(inputString, replaceItems, newItems):
    # Iterate over the replaceItems items to be replaced
    for item in replaceItems :
        # Check if string is in the main string
        if item in inputString:
            # Replace the string
            inputString = inputString.replace(item, newItems)    
    return  inputString

#function to clean tweets for sentimental analysis
def GetPolarityFeeds():
    with open(tweetsFile,'r',encoding='utf-8-sig') as outputFile:
        dataSet= csv.reader(outputFile)
        y=0
        listItem=[]
        for x in dataSet:
            if y!=0:
                finalText = ReplaceChar(x[y],['@','#',"https://",':','-','!',"\"","\'","\\",".","–","..."],"")
                listItem.append(finalText)
                y=0
            y+=3
        

#inserting cleaned tweets into a csv file
    with open(cleanTweetFile,'w',newline='',encoding='utf8') as outfile:
        writer=csv.writer(outfile)
        writer.writerow(['text'])
        for i in listItem:
            writer.writerow([i])


    ####performing sentiment analysis

    with open(lexiconFile,"r") as f:
        loaded_json=json.load(f)   
        print("START TIME",datetime.utcnow())
 
        for j in listItem:
            temp=j.split(" ")
            polarity=0
            for k in temp:
                for x in loaded_json:
                    if k==x:
                    #iterating the tweets and the lexicon file and calculating the polarity
                        polarity +=int(loaded_json[x])
       
            if(polarity==0):
                polarity_list.append(polarity)
                polarity_list1.append("Neutral")
            elif(polarity<0):
                polarity_list.append(polarity)
                polarity_list1.append("Negative")
            elif(polarity>0):
                polarity_list.append(polarity)
                polarity_list1.append("Positive")
        print("END TIME",datetime.utcnow())

    #creating a csv file to store sentimental anylisis
    with open(sentimentFile,'w',newline='',encoding='utf8') as outfile:
        writer=csv.writer(outfile)
        writer.writerow(['The Twitter Tweet',"The Sentiment(Positive,Negative, or Neutral)","The Sentiment Score"])
        for i in range(0,len(listItem)):
            writer.writerow([listItem[i],polarity_list1[i],polarity_list[i]])

#region ELASTIC SEARCH
def InitiateElasticSearch():
    es = Elasticsearch(["https://admin:FNQWOUKVZVORTXOU@portal-ssl10-37.bmix-dal-yp-cc38e81f-55f5-4558-96fd-aa84b720bc0a.894976941.composedb.com:58282/"])
    with open('sentimental_analysis.csv','r',encoding='utf-8') as f:
       reader = csv.DictReader(f)
       helpers.bulk(es, reader, index='sentiment-analysis1', doc_type='elasticSearch')
    return "Data for Elastic Search uploaded."


def ElasticSearch():
    val = input("yes/no for Elastic Search... ") #User will input YES/NO for initiating elastic search.
    if(val.upper() == "YES"):
        return print(InitiateElasticSearch())
    else:
        return "You entered NO, Sorry!"

#endregion

#region CALLING API
queries = FetchTweets()
WriteFeeds(queries) 
GetPolarityFeeds()
ElasticSearch()
#endregion








    
  