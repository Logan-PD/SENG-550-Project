from textblob import TextBlob as tb
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
import re

def clean_text(text):  
    pat1 = r'@[^ ]+'                   
    pat2 = r'https?://[A-Za-z0-9./]+'  
    pat3 = r'\'s'                      
    pat4 = r'\#\w+'                     
    pat5 = r'&amp '                     
    pat6 = r'[^A-Za-z\s]'               
    combined_pat = r'|'.join((pat1, pat2,pat3,pat4,pat5, pat6))
    text = re.sub(combined_pat,"",text).lower()
    return text.strip()



data=pd.read_csv('testData.csv') #Change to input source
for index, row in data.iterrows():
    if index > 15: #Delete to see whole data source
        break      #Delete to see whole data source
    text = clean_text(row['text'])
    userID = row['userid']
    vader_totals = SentimentIntensityAnalyzer().polarity_scores(text)
    vader_neg = vader_totals['neg']
    vader_neu = vader_totals['neu']
    vader_pos = vader_totals['pos']
    vader_compound = vader_totals['compound']
    tb_polarity = tb(text).sentiment.polarity
    tb_subjectivity = tb(text).sentiment.subjectivity
    # print("Tweet: ", index)
    # print("User: ", userID)
    # print("Score: ", vader_compound)
    # print("Polarity: ",tb_polarity)
    # print("Subjectivity: ",tb_subjectivity)
    # print(text,'\n')