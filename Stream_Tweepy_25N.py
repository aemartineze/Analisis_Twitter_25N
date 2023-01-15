# 3.1 Extracción de datos desde Twitter

#Cargar librerias

import sqlite3
import tweepy
import time


#Cargar las credenciales Twitter
API_KEY = " "
API_KEY_SECRET = ""  
BEARER_TOKEN = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

api_key = API_KEY
api_key_secret = API_KEY_SECRET
access_token = ACCESS_TOKEN
access_token_secret = ACCESS_TOKEN_SECRET
bearer_token = BEARER_TOKEN

#Creación base de datos
conn = sqlite3.connect("./database/base_tweets.db")
print("Base de datos creada")
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS tweets (id_tweet INTEGER, username TEXT, tweet TEXT, fecha TEXT, idioma TEXT, id_rt_tweet INTEGER, type_rt TEXT)")
print("Tabla creada")

#Streaming de Twitter

# Acceder a la API de Twitter con las credenciales
client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
api = tweepy.API(auth)

# Definir términos a filtrar
search_terms = ['"#25N" lang:es',"#Vivasnosqueremos", "#niunamenos"]

#Definir extracción
class TweetStreamV2(tweepy.StreamingClient):
    new_tweet = {}

    def on_connect(self):
        print("Connected!")


    def on_tweet(self, tweet):
        
        self.new_tweet["id_tweet"] = tweet.id
        self.new_tweet["tweet"] = tweet.text
        self.new_tweet["fecha"] = tweet.created_at
        self.new_tweet["idioma"] = tweet.lang
        if tweet.referenced_tweets == None:
            self.new_tweet["id_rt"] = "N/A"
            self.new_tweet["type_rt"] = "N/A"
        else:  
            if tweet.referenced_tweets[0].type == 'retweeted' or tweet.referenced_tweets[0].type == 'quoted':
                self.new_tweet["id_rt"] = tweet.referenced_tweets[0].id
                self.new_tweet["type_rt"] = tweet.referenced_tweets[0].type
            
        
        time.sleep(0.3)
            
    def on_includes(self, includes):
        self.new_tweet["username"] = includes["users"][0].username
        print("Nuevo usuario", self.new_tweet["username"])
        print("Nuevo tweet", self.new_tweet["tweet"])
        print("fecha", self.new_tweet["fecha"])
        print("idioma", self.new_tweet["idioma"])
        print("tipo", self.new_tweet["type_rt"])
        print("-" * 30)
        # insert tweets in db
        cursor.execute(
            "INSERT INTO tweets VALUES (?,?,?,?,?,?,?)",
            (
                self.new_tweet["id_tweet"],
                self.new_tweet["username"],
                self.new_tweet["tweet"],
                self.new_tweet["fecha"],
                self.new_tweet["idioma"],
                self.new_tweet["id_rt"],
                self.new_tweet["type_rt"]
            ),
        )
        conn.commit()
        
        print("tweet added to db!")
        print("-" * 30)

    
stream = TweetStreamV2(bearer_token)

#Verificar si existe un filtro definido en el Stream
print(stream.get_rules())

# Borrar el filtro si existe
prev_id = stream.get_rules().data[0].id
stream.delete_rules(prev_id)
    

#Añadir la nueva búsqueda
    
for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))


# Realizar la conexión

stream.filter(
        expansions="author_id",
        tweet_fields=["created_at","referenced_tweets","lang"]
        )  





