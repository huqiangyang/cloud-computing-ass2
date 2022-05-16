#
#Team:15
#Yuqi Cao 1186642
#Yaxuan Huang 1118801
#Meng Yang 1193990
#Gangdan Shu 1239032
#Zheng Xu 1291694
#
# This file will get the tweet data from couchDB into the format we set
import couchdb
from textblob import TextBlob
from afinn import Afinn
import pygsheets
import pandas as pd
import time
import nltk
from nltk import sent_tokenize
from nltk import word_tokenize
nltk.download('punkt')

# in this part, we need to get the data from couchDB and do the analysis, then send results to Google sheets in order to connect to our front end
# we will output five Google Sheets
# all_place_time: sheet with columns named 'city', 'time_slot', 'emo_degree', 'tweet_count'
# all_place_week: sheet with columns named 'city', 'week_slot', 'emo_degree', 'tweet_count'
# syMel_word: sheet with columns named 'city', 'emo_degree', 'word_num', 'tweet_count'
# syMel_sentence: sheet with columns named 'city', 'emo_degree', 'sentence_num', 'tweet_count'

#connect to server
couchdb_address = "http://yuqicao:7486ocean@xxx.xxx.xxx.xxx:5984"  
couchDBserver = couchdb.Server(couchdb_address)

#check the database exists or not
dbname = ["dbdata_data1","dbdata_data2","dbdata_data3","dbdata_data4","dbdata_data5","dbdata_data6","dbdata_data7","dbdata_data8","dbdata_data9"]
for i in dbname:
    if i in couchDBserver:
        del couchDBserver[i]
dbData1 = couchDBserver.create(dbname[0])
dbData2 = couchDBserver.create(dbname[1])
dbData3 = couchDBserver.create(dbname[2])
dbData4 = couchDBserver.create(dbname[3])
dbData5 = couchDBserver.create(dbname[4])

dbData6 = couchDBserver.create(dbname[5]) # all_place_time
dbData7 = couchDBserver.create(dbname[6]) # all_place_week
dbData8 = couchDBserver.create(dbname[7]) # syMel_word
dbData9 = couchDBserver.create(dbname[8]) # syMel_sentence


while True:
    #we have chosen the area of Melbourne, Sydney, and Canberra
    #we also chose suburbs of Melbourne
    city = [['01096c751debd6e4','Wyndham'],['01dc11312a909502','Melton - Bacchus Marsh'],['0174414074876aed','Whittlesea - Wallan'],['0099f49a1e9468fa','Yarra Ranges'],
            ['01bd76d80f0324ea','Sunbury'],['01eabd04246fc7c6','Macedon Ranges'],['0060c5e74a64c473','Nillumbik - Kinglake'],['587e66303af8a504','Cardinia'],
            ['7d3e345e5d8fdfc7','Knox'],['0afae18a56e6352b','Frankston'],['78e3a3e78bf72396','Dandenong'],['2656dff1df4fa2f8','Kingston'],
            ['045d073d0dd753e5','Manningham - East'],['46c439e31d6691c7','Maribyrnong'],['5280716226da86bb','Monash'],['2b9c9de43d3b163c','Essendon'],
            ['4caa1cb3fba85a4e','Mornington Peninsula'],['746cd6894cb2f59c','Port Phillip'],['01864a8a64df9dc4','Melbourne City'],['0073b76548e5984f','Sydney Inner City'],['066b94a60af1c21d','Canberra East']]

    #we need to consider time in every two hours
    time_range = [[0,2,'0-2'],[2,4,'2-4'],[4,6,'4-6'],[6,8,'6-8'],[8,10,'8-10'],[10,12,'10-12'],
            [12,14,'12-14'],[14,16,'14-16'],[16,18,'16-18'],[18,20,'18-20'],[20,22,'20-22'],[22,24,'22-24']]

    #connect to our tweet databset
    dbtweet = couchDBserver['twitter']


    #format data into new format
    pointer = 1
    ii = 1
    for tw in dbtweet:
        ii += 1
        if ii > 50:
            break
        
        _id = tw
        print(_id)
        tweet = dbtweet[tw]
        
        #get the name of location due to their place_id
        key = tweet["place_id"]         
        for i in range(len(city)):
            if key == city[i][0]:
                location = city[i][1]
                break
            else:
                location = None

        # we get time and decide for which time_slot it should be
        twTime = int(tweet["time"][11:13])
        for i in range(len(time_range)):
            if time_range[i][0] <= twTime and twTime < time_range[i][1]:
                time = time_range[i][2]
                break

        # we get the day of the week and decide for which week_slot it should be
        if tweet["time"][0:3] != '202': #some different format of time of Twitter
            week = tweet["time"][0:3]

        text = tweet["text"]
        # see if "emo" due to two packages of python called TextBlob and Afinn
        if TextBlob(text).polarity < 0: #consider as 'emo' tweets

            # if 'emo', get the word count level due to the numbers of words, and the numbers of sentences of this tweet
            word_count = len(word_tokenize(text))
            if 0 <= word_count < 50:
                word_num = '0-49'
            elif 50 <= word_count < 80:
                word_num = '50-79'
            elif 80 <= word_count < 100:
                word_num = '80-99'
            elif 100 <= word_count < 120:
                word_num = '100-119'
            else:
                word_num = 'over 120'

            sentence_num = len(sent_tokenize(text))

            #decide the degree of the sentiment
            afinn = Afinn()
            sentiment_score = afinn.score(text) 
            if -2 < sentiment_score <= 0:
                sentiment = 'mild'
            elif -4 < sentiment_score <= -2:
                sentiment = 'moderate'
            elif -5 <= sentiment_score <= -4:
                sentiment = 'severe'
            else:
                sentiment = 'no_emo'
        else:
            word_num = 'not_consider'
            sentence_num = 'not_consider'
            sentiment = 'no_emo'

        # we need to combine some information due to what we want for the five final sheets

        # all_place_time: sheet with columns named 'city', 'time_slot', 'emo_degree', 'tweet_count'
        if location != None:
            all_place_time = location + ',' + time + ',' + str(sentiment)
        else:
            all_place_time = None
        
        # all_place_week: sheet with columns named 'city', 'week_slot', 'emo_degree', 'tweet_count'
        if location == 'Melbourne City' or location == 'Melton - Bacchus Marsh' or location == 'Sunbury' or location == 'Sydney Inner City':
            all_place_week = location + ',' + week + ',' + str(sentiment)
        else:
            all_place_week = None

        # syMel_word: sheet with columns named 'city', 'emo_degree', 'word_num', 'tweet_count'
        if (location == 'Melbourne City' or location == 'Sydney Inner City') and word_num != 'not_consider':
            syMel_word = location + ',' + sentiment + ',' + str(word_num)
        else:
            syMel_word = None

        # syMel_sentence: sheet with columns named 'city', 'emo_degree', 'sentence_num', 'tweet_count'
        if (location == 'Melbourne City' or location == 'Sydney Inner City') and sentence_num != 'not_consider':
            syMel_sentence = location + ',' + sentiment + ',' + str(sentence_num)
        else:
            syMel_sentence = None


        doc = {
                '_id' : _id,
                'all_place_time' : all_place_time,
                'all_place_week' : all_place_week,
                'syMel_word' : syMel_word,
                'syMel_sentence' : syMel_sentence
        }


        #save document to the database
        if pointer % 5 == 1:
            dbData1.save(doc)
        elif pointer % 5 == 2:
            dbData2.save(doc)
        elif pointer % 5 == 3:
            dbData3.save(doc)
        elif pointer % 5 == 4:
            dbData4.save(doc)
        elif pointer % 5 == 0:
            dbData5.save(doc)

        pointer += 1

    # here is map reduce part of couchDB
    myMapReduce1 = {
    "_id": "_design/myMapReduce1",
    "language": "javascript",
    "views": {
        "all_place_time": {
        "map": "function (doc) {if (doc.all_place_time != null) {emit(doc.all_place_time, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "all_place_week": {
        "map": "function (doc) {if (doc.all_place_week != null) {emit(doc.all_place_week, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_word": {
        "map": "function (doc) {if (doc.syMel_word != null) {emit(doc.syMel_word, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_sentence": {
        "map": "function (doc) {if (doc.syMel_sentence != null) {emit(doc.syMel_sentence, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }

    dbData1.save(myMapReduce1)
    MPall_place_time = dbData1.view('myMapReduce1/all_place_time', group=True, descending=False)
    for tw in MPall_place_time:
        doc = {
                'all_place_time_key' : tw["key"],
                'all_place_time_value' : tw["value"]
        } 
        dbData6.save(doc)

    MPall_place_week = dbData1.view('myMapReduce1/all_place_week', group=True, descending=False)
    for tw in MPall_place_week:
        doc = {
                'all_place_week_key' : tw["key"],
                'all_place_week_value' : tw["value"]
        } 
        dbData7.save(doc)

    MPsyMel_word = dbData1.view('myMapReduce1/syMel_word', group=True, descending=False)
    for tw in MPsyMel_word:
        doc = {
                'syMel_word_key' : tw["key"],
                'syMel_word_value' : tw["value"]
        } 
        dbData8.save(doc)

    MPsyMel_sentence = dbData1.view('myMapReduce1/syMel_sentence', group=True, descending=False)
    for tw in MPsyMel_sentence:
        doc = {
                'syMel_sentence_key' : tw["key"],
                'syMel_sentence_value' : tw["value"]
        } 
        dbData9.save(doc)

    #------------------------------------------------------------------------------------------
        # here is map reduce part of couchDB
    myMapReduce2 = {
    "_id": "_design/myMapReduce2",
    "language": "javascript",
    "views": {
        "all_place_time": {
        "map": "function (doc) {if (doc.all_place_time != null) {emit(doc.all_place_time, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "all_place_week": {
        "map": "function (doc) {if (doc.all_place_week != null) {emit(doc.all_place_week, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_word": {
        "map": "function (doc) {if (doc.syMel_word != null) {emit(doc.syMel_word, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_sentence": {
        "map": "function (doc) {if (doc.syMel_sentence != null) {emit(doc.syMel_sentence, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }

    dbData2.save(myMapReduce2)
    MPall_place_time = dbData2.view('myMapReduce2/all_place_time', group=True, descending=False)
    for tw in MPall_place_time:
        doc = {
                'all_place_time_key' : tw["key"],
                'all_place_time_value' : tw["value"]
        } 
        dbData6.save(doc)

    MPall_place_week = dbData2.view('myMapReduce2/all_place_week', group=True, descending=False)
    for tw in MPall_place_week:
        doc = {
                'all_place_week_key' : tw["key"],
                'all_place_week_value' : tw["value"]
        } 
        dbData7.save(doc)

    MPsyMel_word = dbData2.view('myMapReduce2/syMel_word', group=True, descending=False)
    for tw in MPsyMel_word:
        doc = {
                'syMel_word_key' : tw["key"],
                'syMel_word_value' : tw["value"]
        } 
        dbData8.save(doc)

    MPsyMel_sentence = dbData2.view('myMapReduce2/syMel_sentence', group=True, descending=False)
    for tw in MPsyMel_sentence:
        doc = {
                'syMel_sentence_key' : tw["key"],
                'syMel_sentence_value' : tw["value"]
        } 
        dbData9.save(doc)

    #-----------------------------------------------------------------------------------------------
        # here is map reduce part of couchDB
    myMapReduce3 = {
    "_id": "_design/myMapReduce3",
    "language": "javascript",
    "views": {
        "all_place_time": {
        "map": "function (doc) {if (doc.all_place_time != null) {emit(doc.all_place_time, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "all_place_week": {
        "map": "function (doc) {if (doc.all_place_week != null) {emit(doc.all_place_week, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_word": {
        "map": "function (doc) {if (doc.syMel_word != null) {emit(doc.syMel_word, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_sentence": {
        "map": "function (doc) {if (doc.syMel_sentence != null) {emit(doc.syMel_sentence, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }

    dbData3.save(myMapReduce3)
    MPall_place_time = dbData3.view('myMapReduce3/all_place_time', group=True, descending=False)
    for tw in MPall_place_time:
        doc = {
                'all_place_time_key' : tw["key"],
                'all_place_time_value' : tw["value"]
        } 
        dbData6.save(doc)

    MPall_place_week = dbData3.view('myMapReduce3/all_place_week', group=True, descending=False)
    for tw in MPall_place_week:
        doc = {
                'all_place_week_key' : tw["key"],
                'all_place_week_value' : tw["value"]
        } 
        dbData7.save(doc)

    MPsyMel_word = dbData3.view('myMapReduce3/syMel_word', group=True, descending=False)
    for tw in MPsyMel_word:
        doc = {
                'syMel_word_key' : tw["key"],
                'syMel_word_value' : tw["value"]
        } 
        dbData8.save(doc)

    MPsyMel_sentence = dbData3.view('myMapReduce3/syMel_sentence', group=True, descending=False)
    for tw in MPsyMel_sentence:
        doc = {
                'syMel_sentence_key' : tw["key"],
                'syMel_sentence_value' : tw["value"]
        } 
        dbData9.save(doc)

    #------------------------------------------------------------------------------------------
        # here is map reduce part of couchDB
    myMapReduce4 = {
    "_id": "_design/myMapReduce4",
    "language": "javascript",
    "views": {
        "all_place_time": {
        "map": "function (doc) {if (doc.all_place_time != null) {emit(doc.all_place_time, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "all_place_week": {
        "map": "function (doc) {if (doc.all_place_week != null) {emit(doc.all_place_week, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_word": {
        "map": "function (doc) {if (doc.syMel_word != null) {emit(doc.syMel_word, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_sentence": {
        "map": "function (doc) {if (doc.syMel_sentence != null) {emit(doc.syMel_sentence, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }

    dbData4.save(myMapReduce4)
    MPall_place_time = dbData4.view('myMapReduce4/all_place_time', group=True, descending=False)
    for tw in MPall_place_time:
        doc = {
                'all_place_time_key' : tw["key"],
                'all_place_time_value' : tw["value"]
        } 
        dbData6.save(doc)

    MPall_place_week = dbData4.view('myMapReduce4/all_place_week', group=True, descending=False)
    for tw in MPall_place_week:
        doc = {
                'all_place_week_key' : tw["key"],
                'all_place_week_value' : tw["value"]
        } 
        dbData7.save(doc)

    MPsyMel_word = dbData4.view('myMapReduce4/syMel_word', group=True, descending=False)
    for tw in MPsyMel_word:
        doc = {
                'syMel_word_key' : tw["key"],
                'syMel_word_value' : tw["value"]
        } 
        dbData8.save(doc)

    MPsyMel_sentence = dbData4.view('myMapReduce4/syMel_sentence', group=True, descending=False)
    for tw in MPsyMel_sentence:
        doc = {
                'syMel_sentence_key' : tw["key"],
                'syMel_sentence_value' : tw["value"]
        } 
        dbData9.save(doc)

    #----------------------------------------------------------------------------------------
        # here is map reduce part of couchDB
    myMapReduce5 = {
    "_id": "_design/myMapReduce5",
    "language": "javascript",
    "views": {
        "all_place_time": {
        "map": "function (doc) {if (doc.all_place_time != null) {emit(doc.all_place_time, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "all_place_week": {
        "map": "function (doc) {if (doc.all_place_week != null) {emit(doc.all_place_week, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_word": {
        "map": "function (doc) {if (doc.syMel_word != null) {emit(doc.syMel_word, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        },
        "syMel_sentence": {
        "map": "function (doc) {if (doc.syMel_sentence != null) {emit(doc.syMel_sentence, 1);}}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }

    dbData5.save(myMapReduce5)
    MPall_place_time = dbData5.view('myMapReduce5/all_place_time', group=True, descending=False)
    for tw in MPall_place_time:
        doc = {
                'all_place_time_key' : tw["key"],
                'all_place_time_value' : tw["value"]
        } 
        dbData6.save(doc)

    MPall_place_week = dbData5.view('myMapReduce5/all_place_week', group=True, descending=False)
    for tw in MPall_place_week:
        doc = {
                'all_place_week_key' : tw["key"],
                'all_place_week_value' : tw["value"]
        } 
        dbData7.save(doc)

    MPsyMel_word = dbData5.view('myMapReduce5/syMel_word', group=True, descending=False)
    for tw in MPsyMel_word:
        doc = {
                'syMel_word_key' : tw["key"],
                'syMel_word_value' : tw["value"]
        } 
        dbData8.save(doc)

    MPsyMel_sentence = dbData5.view('myMapReduce5/syMel_sentence', group=True, descending=False)
    for tw in MPsyMel_sentence:
        doc = {
                'syMel_sentence_key' : tw["key"],
                'syMel_sentence_value' : tw["value"]
        } 
        dbData9.save(doc)


    #---------------------------------------------------------------------------------------------
    #final MapReduce
    final_all_place_time = {
    "_id": "_design/final_all_place_time",
    "language": "javascript",
    "views": {
        "final_all_place_time": {
        "map": "function (doc) {emit(doc.all_place_time_key, doc.all_place_time_value);}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }
    dbData6.save(final_all_place_time)

    final_all_place_week = {
    "_id": "_design/final_all_place_week",
    "language": "javascript",
    "views": {
        "final_all_place_week": {
        "map": "function (doc) {emit(doc.all_place_week_key, doc.all_place_week_value);}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }
    dbData7.save(final_all_place_week)

    final_syMel_word = {
    "_id": "_design/final_syMel_word",
    "language": "javascript",
    "views": {
        "final_syMel_word": {
        "map": "function (doc) {emit(doc.syMel_word_key, doc.syMel_word_value);}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }
    dbData8.save(final_syMel_word)

    final_syMel_sentence = {
    "_id": "_design/final_syMel_sentence",
    "language": "javascript",
    "views": {
        "final_syMel_sentence": {
        "map": "function (doc) {emit(doc.syMel_sentence_key, doc.syMel_sentence_value);}",
        "reduce": "function (keys, values) {return sum(values);}"
        }
    }
    }
    dbData9.save(final_syMel_sentence)

    #---------------------------------------------------------------------------------------
    #output our result to Google sheets
    #authorization, connect Google sheet API
    gc = pygsheets.authorize(service_file='comp90024-350004-a60470e5421e.json')


    #---------------------------------------------------------------------------------------
    # all_place_time: sheet with columns named 'city', 'time_slot', 'emo_degree', 'tweet_count'
    all_place_time = dbData6.view('final_all_place_time/final_all_place_time', group=True, descending=False)

    # Create empty dataframe
    df = pd.DataFrame()

    # Create columns
    df['city'] = None
    df['time_slot'] = None
    df['emo_degree'] = None
    df['tweet_count'] = None

    #open the google spreadsheet (where 'all_place_time' is the name of my sheet)
    wks = gc.open('all_place_time').sheet1
    #clear all previous data if there is
    wks.clear()
    #update the first sheet with df, starting at cell A1. 
    wks.set_dataframe(df,(1,1))

    for tweet in all_place_time:
        tw_split = str(tweet["key"]).split(',')

        if tweet["value"] >= 5:
            wks.append_table(values= [tw_split[0],tw_split[1],tw_split[2],tweet["value"]], start='A1', end=None, dimension='ROWS', overwrite=True)
            wks.add_rows(1)


    #---------------------------------------------------------------------------------------
    # all_place_week: sheet with columns named 'city', 'week_slot', 'emo_degree', 'tweet_count'
    all_place_week = dbData7.view('final_all_place_week/final_all_place_week', group=True, descending=False)

    # Create empty dataframe
    df = pd.DataFrame()

    # Create columns
    df['city'] = None
    df['week_slot'] = None
    df['emo_degree'] = None
    df['tweet_count'] = None

    #open the google spreadsheet (where 'all_place_week' is the name of my sheet)
    wks = gc.open('all_place_week').sheet1
    #clear all previous data if there is
    wks.clear()
    #update the first sheet with df, starting at cell A1. 
    wks.set_dataframe(df,(1,1))

    for tweet in all_place_week:
        tw_split = str(tweet["key"]).split(',')

        wks.append_table(values= [tw_split[0],tw_split[1],tw_split[2],tweet["value"]], start='A1', end=None, dimension='ROWS', overwrite=True)
        wks.add_rows(1)

    #---------------------------------------------------------------------------------------
    # syMel_word: sheet with columns named 'city', 'emo_degree', 'word_num', 'tweet_count'
    syMel_word = dbData8.view('final_syMel_word/final_syMel_word', group=True, descending=False)

    # Create empty dataframe
    df = pd.DataFrame()

    # Create columns
    df['city'] = None
    df['emo_degree'] = None
    df['word_num'] = None
    df['tweet_count'] = None

    #open the google spreadsheet (where 'syMel_word' is the name of my sheet)
    wks = gc.open('syMel_word').sheet1
    #clear all previous data if there is
    wks.clear()
    #update the first sheet with df, starting at cell A1. 
    wks.set_dataframe(df,(1,1))

    for tweet in syMel_word:
        tw_split = str(tweet["key"]).split(',')

        wks.append_table(values= [tw_split[0],tw_split[1],tw_split[2],tweet["value"]], start='A1', end=None, dimension='ROWS', overwrite=True)
        wks.add_rows(1)

    #---------------------------------------------------------------------------------------
    # syMel_sentence: sheet with columns named 'city', 'emo_degree', 'sentence_num', 'tweet_count'
    syMel_sentence = dbData9.view('final_syMel_sentence/final_syMel_sentence', group=True, descending=False)

    # Create empty dataframe
    df = pd.DataFrame()

    # Create columns
    df['city'] = None
    df['emo_degree'] = None
    df['sentence_num'] = None
    df['tweet_count'] = None

    #open the google spreadsheet (where 'syMel_sentence' is the name of my sheet)
    wks = gc.open('syMel_sentence').sheet1
    #clear all previous data if there is
    wks.clear()
    #update the first sheet with df, starting at cell A1. 
    wks.set_dataframe(df,(1,1))

    for tweet in syMel_sentence:
        tw_split = str(tweet["key"]).split(',')

        wks.append_table(values= [tw_split[0],tw_split[1],tw_split[2],tweet["value"]], start='A1', end=None, dimension='ROWS', overwrite=True)
        wks.add_rows(1)


    time.sleep(86400) #This system will never stop and update every data (24 hours), except people stop it.
