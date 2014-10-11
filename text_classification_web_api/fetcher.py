__author__ = 'Samuel'
import models
from models import config, mysession, Fetch, Datum, User, get_or_create

import twitter
from datetime import datetime
import time
from text_classification import predictor
from utils import HtmlUtils
htmlutils = HtmlUtils()

tApi = twitter.Api(consumer_key=config['CONSUMER_KEY'], consumer_secret=config['CONSUMER_SECRET'],
access_token_key=config['ACCESS_KEY'], access_token_secret=config['ACCESS_SECRET'])



def save(statuses, fetch_model, fetch_datetime):
    new_users_count = 0
    new_data_count = 0
    counter = 0
    # dictionary with the fetched users external id as key to prevent addition of the same user more than ones
    users_added = {}
    # iterate over all the statuses and insert new data to DB
    for s in statuses:
        counter+=1
        print "\t{0:d}] id: {1:d}".format(counter, s.id)

        user = None
        if users_added.has_key(s.user.id):
            user = users_added[s.user.id]
        else:
            user, user_is_new = get_or_create(mysession, User,
                external_id = s.user.id,
                source = fetch_model.source
            )
            if user_is_new:
                new_users_count+=1
                user.name = s.user.name
                user.screen_name = s.user.screen_name
                user.description = htmlutils.unescape(s.user.description)
                user.profile_image_url = s.user.profile_image_url
                user.verified = s.user.verified
                user.language = s.user.lang.split("-")[0]
                user.fetched_data = s.user.AsDict()
                user.fetched_at = fetch_datetime
                user.created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(s.user.created_at,'%a %b %d %H:%M:%S +0000 %Y'))
                users_added[s.user.id] = user # remember added user in case there he's got more data than this datum

        datum, datum_is_new = get_or_create(mysession, Datum,
            external_id = s.id,
            source = fetch_model.source
        )
        prediction=None
        if datum_is_new:
            new_data_count+=1
            sDict = s.AsDict()
            language = fetch_model.language or s.GetLang()
            datum.language = language
            datum.domain = fetch_model.domain
            prediction = predictor.predict(language, fetch_model.domain, fetch_model.source, [s.text])[0]
            datum.class_value = prediction.value
            datum.gold = 0
            datum.text = htmlutils.unescape(s.text)
            datum.user = user
            if sDict.has_key('hashtags') and len(sDict['hashtags'])>0: datum.hashtags = sDict['hashtags']
            if sDict.has_key('urls') and len(sDict['urls'])>0: datum.urls = sDict['urls']
            if sDict.has_key('media'): datum.media = sDict['media']
            datum.fetched_data = sDict
            datum.fetched_at = fetch_datetime
            datum.created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(s.created_at,'%a %b %d %H:%M:%S +0000 %Y'))
            print("\tnew datum -> insert")
            print("\tpredicted -> class name: {0:s}, class value: {1:s}".format(prediction.name,str(prediction.value)))
        else:
            print("\talready exist, better luck next time :)")

    if new_users_count>0 or new_data_count>0:
        print("\tinsert all {0:d} new users and {1:d} new data items".format(new_users_count, new_data_count))
        mysession.commit()
        print ("\tdone")
    else:
        print("\tno new data to save")


def fetch(fetch_model, term):
    return tApi.GetSearch(term=term,
                #since_id=get_last_id("twitter","bitcoin"),
                #max_id=497057404131893250,
                #until="2014-08-05",
                lang=fetch_model.language, count=fetch_model.results_amount, result_type=fetch_model.result_type)


def start(fetch_model):
    predictor.use_model(fetch_model.language,fetch_model.domain,fetch_model.source)
    while True:
        for term in fetch_model.search_terms:
            fetch_timestamp = time.time()
            fetch_timestamp = datetime.fromtimestamp(fetch_timestamp)
            fetch_timestamp = fetch_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            print ("fetch_timestamp: " + fetch_timestamp)
            print("get " + str(fetch_model.results_amount) + " " + fetch_model.result_type + " statuses related the term: '" + term + "'...")
            statuses = fetch(fetch_model, term)
            print("done")
            print("got " + str(len(statuses)) + " results")
            print("saving data...")
            save(statuses, fetch_model, fetch_timestamp)
            print("done")
            print("waiting " + str(fetch_model.interval_in_min) + " minutes to repeat")
            time.sleep(fetch_model.interval)

if __name__=="__main__":
    language = "en"
    domain = "bitcoin"
    source = "twitter"
    fetch_en_bitcoin_twitter = Fetch(language=language,domain=domain,source=source)
    fetch_en_bitcoin_twitter.search_terms = [
        """ "bitcoin" OR "bitcoins" OR "bitcoin's" OR "BTC" OR "BTC's" OR "#bitcoin" OR "#bitcoins" OR "#BTC" "bitcoin is" OR "bitcoin will" OR "bitcoin price" OR "bitcoin value" """,

        """ "bitcoin" OR "bitcoins" """,
        """ "bitcoins" OR "bitcoin's" """,
        """ "BTC" OR "BTC's" OR "btc" """,

        """ "bitcoin" OR "bitcoins" OR "bitcoin's" OR "BTC" OR "BTC's" OR "#bitcoin" OR "#bitcoins" OR "#BTC" "bitcoin is" OR "bitcoin will" OR "bitcoin price" OR "bitcoin value" """,

        """ "#bitcoin" OR "#bitcoins" OR "#BTC" """,
        """ "bitcoin is" OR "bitcoin will" """,
        """ "bitcoin price" OR "bitcoin value" """
        ]
    start(fetch_en_bitcoin_twitter)



#prediction = predictor.predict("en","bitcoin","twitter",["bitcoin price falling bad"])
#print(prediction)