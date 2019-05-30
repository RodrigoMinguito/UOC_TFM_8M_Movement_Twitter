import time
import tweepy
import csv
import threading
import datetime
import util

def almacenarTweet(tweet,wrTweets,seenTweets):
    texto =  '"'+ ''.join(c for c in tweet.text if ord(c) <= 255).replace("\n","").replace('"', '') + '"'
    msgTweet = [tweet.id, tweet.user.id, tweet.created_at, texto]
    msgTweet = tuple(msgTweet)
    wrTweets.writerow(msgTweet)

def almacenarReTweet(tweet,hashtag,wrRetweets,numAuth):
    msg = [tweet.id,tweet.retweeted_status.id, tweet.user.id_str, tweet.retweeted_status.user.id_str, hashtag,
           tweet.retweeted_status.created_at, tweet.created_at]
    msg = tuple(msg)
    wrRetweets.writerow(msg)
    print(numAuth,"Propietario tweet: ", tweet.user.screen_name, "  Creacion Tweet: ",
          tweet.created_at, " --- RETWEET - Propietario: ", tweet.retweeted_status.user.screen_name,
          "  Creacion: ", tweet.retweeted_status.created_at)

def almacenarAutor(id_str,screen_name,created_at,tweetId,wr):
    msgAutor = [id_str,"'" + screen_name + "'", tweetId, created_at]
    msgAutor = tuple(msgAutor)
    wr.writerow(msgAutor)

def almacenarMencion(id, user_id_str, mencion_id_str, hashtag, created_atRT, created_at, wrMenciones):
    # Si es un retweet,
    # meto escritor original y mencionado -> idTweet, idAutor, idMencionado, hashtag, timeRetweeteo, timeCreacion
    # Si es una mencion
    # meto escritor y mencionado -> idTweet, idEscritor, idMencionado, hashtag, timeCreacion, timeCreacion
    msg = [id, id, user_id_str, mencion_id_str, hashtag, created_atRT, created_at]
    msg = tuple(msg)
    wrMenciones.writerow(msg)

def getAllTweets(hashtag, desde, hasta,wrRetweets,wrMenciones,wrTweets,wr,f1,f2,f3,f4,numAuth=6):
    print("\nThread ", numAuth," - ", datetime.datetime.now()," comienza con el hashtag ", hashtag)
    api = tweepy.API(util.getTokens())
    seenTweets = set()  # set for fast O(1) amortized lookup

    qq=hashtag

    c = tweepy.Cursor(api.search,
                      since_id=1104352145464328192, max_id=1104517480771400000
                      ,q=qq, rpp=100).items()
    w=0
    while True:
            try:
                tweet = c.next()
                if (not hasattr(tweet, "user")):
                    break

                #Guardar todos los tweets
                almacenarTweet(tweet,wrTweets,seenTweets)

                #Guardar todos los autores
                almacenarAutor(tweet.user.id_str, tweet.user.screen_name , tweet.user.created_at, tweet.id, wr)

                # Guardar los retweets
                if hasattr(tweet, 'retweeted_status'):  # Si es un retweet
                    almacenarReTweet(tweet,hashtag,wrRetweets,numAuth)
                    almacenarAutor(tweet.retweeted_status.user.id_str,tweet.retweeted_status.user.screen_name, tweet.retweeted_status.user.created_at,tweet.id, wr)

                entidades = tweet.entities
                try:
                    menciones=entidades["user_mentions"]
                    for mencion in menciones:

                        #print("Mencion de ",mencion["screen_name"])
                        if hasattr(tweet, 'retweeted_status'):
                            if (not tweet.retweeted_status.user.id_str == mencion["id_str"]):   #Para no autometerse como RT
                                almacenarMencion(tweet.retweeted_status.id, tweet.retweeted_status.user.id_str, mencion["id_str"],
                                    hashtag,tweet.retweeted_status.created_at, tweet.created_at,wrMenciones)
                        #        print("\t en RT: ",tweet.retweeted_status.user.screen_name,"--->" , mencion["screen_name"])
                        #y meto siempre el dueño del tweet y el mencionado
                        almacenarMencion(tweet.id, tweet.user.id_str, mencion["id_str"],hashtag, tweet.created_at, tweet.created_at,wrMenciones)
                        #print("\t en dueño: ", tweet.user.screen_name,"--->" ,mencion["screen_name"])

                        almacenarAutor(mencion["id_str"], mencion["screen_name"], "",tweet.id,wr)
                        w = 0

                except Exception as inst:
                    print("ERROR", inst)

            except tweepy.TweepError:
                w=w+1
                print("Thread ", numAuth," - ", datetime.datetime.now()," - intento, ",w,"/15 esperamos con error 429 con el hashtag ", hashtag)
                time.sleep(60 * 1)
                f1.flush()
                f2.flush()
                f3.flush()
                f4.flush()
                continue
            except StopIteration:
                break
            except Exception as inst:
                print("EXCEPT", type(inst), inst.args, inst)
                time.sleep(60 * 1)
                continue

#Configura los ficheros y manda bajar los tweets
def multiThreadHashtags(hashtags, desde, hasta,numAuth):
    print("\nARRANCA",hashtags, desde, hasta,numAuth)
    d=desde.replace(":","").replace(" ","")
    h=desde.replace(":","").replace(" ","")
    nombreFichFlag = "D:\\temp\\UOC\\myfileFlag" + d +"_"+h+".csv"
    nombreFichRetweets = "D:\\temp\\UOC\\Retweets\\myfileRetweets"+str(threading.current_thread().ident)+"___" + d +"_"+h+".csv"
    nombreFichMenciones = "D:\\temp\\UOC\\Menciones\\myfileMenciones"+str(threading.current_thread().ident)+"___" + d +"_"+h+ ".csv"
    nombreFichTweetsUnicos = "D:\\temp\\UOC\\TweetsUnicos\\myfileTweetsUnicos"+str(threading.current_thread().ident)+"___" + d +"_"+h+ ".csv"
    nombreFichAutores = "D:\\temp\\UOC\\Autores\\myfileAutores"+str(threading.current_thread().ident)+"___" + d +"_"+h+ ".csv"

    with open(nombreFichFlag, 'w', newline='') as myfileFlag:
            with open(nombreFichRetweets, 'w', newline='') as myfile1:
                wrRetweets = csv.writer(myfile1)
                with open(nombreFichAutores, 'w', newline='') as myfile:
                    wr = csv.writer(myfile)
                    with open(nombreFichMenciones, 'w', newline='') as myfile2:
                        wrMenciones = csv.writer(myfile2)
                        with open(nombreFichTweetsUnicos, 'w', newline='') as myfile3:
                            wrTweets = csv.writer(myfile3)
                            for hashtag in hashtags:
                                getAllTweets(hashtag, desde, hasta, wrRetweets, wrMenciones,wrTweets,wr,myfile1,myfile,myfile2,myfile3,numAuth)

    print("\nFIN",hashtags, desde, hasta,numAuth)

#Baja los hashtags indicados en hashtagsX, con la posibilidad de meter más processThread y lanzarlos en paralelo
def dumpHashTags(desde, hasta):
    hashtags5 = [ "#8m"]
    hashtags1 = ["mujer"]

    #desde y hasta no se usan al final. Está metido a fuego para poder usar un rango entre dos tweets
    try:
        processThread5 = threading.Thread(target=multiThreadHashtags, args=(hashtags5, desde, hasta, 5,))
        processThread5.start()
        processThread1 = threading.Thread(target=multiThreadHashtags, args=(hashtags1, desde, hasta, 5,))
        processThread1.start()

    except:
        print ("Error: unable to start thread")

def main():
    #Sacar los tweets
    desde = "2019-03-08 00:00:00"
    hasta = "2019-03-08 02:00:00 "
    print(
         "\n============================================================================================================")
    print("  " + desde + "   -   " + hasta)
    print(
         "===========================================================================================================")

    dumpHashTags(desde, hasta)

if __name__ == '__main__':
    main()
