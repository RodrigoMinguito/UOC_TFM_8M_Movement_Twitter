from networkx.algorithms import community as nxcom
import community
import ast
from numpy.linalg import matrix_power
import math
import time
import tweepy
import datetime
import requests
from requests_oauthlib import OAuth1
import random
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import re
from stop_words import get_stop_words


def getUserInfo(id,writer=None,largo=True):
    #print("\ngetUserInfo(" + id + ")")
    consumer_key, consumer_secret, access_token, access_token_secret = getTokens(6)

    header_auth = True
    j=0

    url = 'https://api.twitter.com/1.1/users/show.json'

    headeroauth = OAuth1(consumer_key, consumer_secret,access_token, access_token_secret,signature_type='auth_header')

    query_params = {'user_id': id }

    while True:
        try:
            response = requests.get(url, auth=headeroauth, params=query_params)
            data = response.json()
            cadena=""
            if "errors" in data:
                j = j + 1
                print(j, " ERROR - 88 Esperamos")
                time.sleep(60 * 1)
                if j>2:
                    break
                continue
            else:
                if not(writer==None):
                    try:
                        if largo==True:
                            writer.write("\n\n------"+id+"\n---------\n")
                            cadena1=str(data['name']+ " " +data['screen_name']+ \
                                ' https://twitter.com/'+ data['screen_name'])
                            cadena1 =  ''.join(c for c in cadena1 if ord(c) <= 255).replace("\n","").replace('"', '')
                            writer.write(cadena1)
                            writer.write("\n")

                            cadena2 = str("\nTweets: "+ str(data['statuses_count'])+ \
                                " Siguiendo: "+ str(data['friends_count'])+ \
                                " Seguidores: " + str(data['followers_count'])+ \
                                 " Creado: "+  str(data['created_at']))
                            cadena2 =  ''.join(c for c in cadena2 if ord(c) <= 255).replace("\n","").replace('"', '')
                            writer.write(cadena2)
                            writer.write("\n")

                            cadena3=str("\nDescripción: "+ str(data['description']).replace("\n","--")+ \
                                 " URL: "+ str(data['url']))
                            cadena3 =  ''.join(c for c in cadena3 if ord(c) <= 255).replace("\n","").replace('"', '')
                            writer.write(cadena3)
                            writer.write("\n")
                        else:
                            cadena3 = str("\n" + str(data['description']).replace("\n", " --"))
                            for i in range(0,20):
                                writer.write(cadena3)
                            writer.write("\n")
                        try:
                            if largo==True:
                                cadena=str("\nCambio de estado: "+ data['status']['created_at']+\
                                    " Último Estado: " + data['status']['text'].replace('\n',''))
                                cadena =  ''.join(c for c in cadena if ord(c) <= 255).replace("\n","").replace('"', '')
                                writer.write(cadena)
                        except Exception as inst:
                            break
                        writer.write(cadena)
                        writer.write("\n")
                    except Exception as inst:
                        print(inst)
                        break
                    return data
                else:
                    print("\n",data['name'],data['screen_name'],'https://twitter.com/'+str(data['screen_name']))
                    #print("\tTweets:",data['statuses_count'],"Siguiendo:",data['friends_count'],"Seguidores:",data['followers_count'],"Creado: ",data['created_at'])
                    print("\tDescripción: ", data['description'])
                    #print("\tURL: ",data['url'])
                    #try:
                    #    print("\tcambio de estado: ", data['status']['created_at'],"\n\tÚltimo Estado: ",data['status']['text'].replace('\n',''))
                    #except Exception as inst:
                    #    break
                    return data
                break
        except tweepy.TweepError:
            j = j + 1
            print(j, " ERROR 429 - getUserInfo - Esperamos ")
            if (j>15):
                break
            time.sleep(60 * 2)
            continue
        except Exception as inst:
            break

def getLatestTweets(id,numero):
    #time.sleep(1)
    print("getLatestTweets("+id+","+str(numero)+")")
    api = tweepy.API(get_auth())
    j=0
    k=0
    tweets=set()
    c = tweepy.Cursor(api.user_timeline,id=id,rpp=numero,include_rts=True, tweet_mode='extended').items()
    while True:
        try:
           tweet = c.next()

           if 'retweeted_status' in dir(tweet):
               texto = "RT "+tweet.retweeted_status.full_text
           else:
               texto = tweet.full_text

           texto = '"' + ''.join(c for c in texto if ord(c) <= 255).replace("\n", " -- ").replace('"', '') + '"'
           msgTweet = tuple([str(tweet.created_at), tweet.user.id, texto])
           tweets.add(msgTweet)

           j=j+1
           if (j>numero):
               break
        except tweepy.TweepError:
                k=k+1
                print(k, " ERROR - 88 Esperamos.")
                time.sleep(60 * 1)
                if k>2:
                    break
                continue
        except StopIteration:
                break
        except Exception as inst:
                print("EXCEPT")
                print(type(inst))  # the exception instance
                print(inst.args)  # arguments stored in .args
                print(inst)  # __str__ allows args to be printed directly
                if inst.response:
                    print(inst.response.content)
                time.sleep(10)
                return 0
    return sorted(tweets)

def getTokens(numAuth=6):
    if (numAuth == 1):
        return consumer_key1, consumer_secret1, access_token1, access_token_secret1

    if (numAuth == 2):
        return consumer_key2, consumer_secret2, access_token2, access_token_secret2

    if (numAuth == 3):
        return consumer_key3, consumer_secret3, access_token3, access_token_secret3

    if (numAuth == 4):
        return consumer_key4, consumer_secret4, access_token4, access_token_secret4

    if (numAuth == 5):
        return consumer_key5, consumer_secret5, access_token5, access_token_secret5

    if (numAuth==6):
        r = random.random()
        if ((r>0.8)):
            return getTokens(1)
        if ((r > 0.6)):
            return getTokens(2)
        if ((r > 0.4)):
            return getTokens(3)
        if ((r > 0.2)):
            return getTokens(4)
        if ((r > 0.0)):
            return getTokens(5)

def get_auth(numAuth=6):
    time.sleep(1)
    consumer_key , consumer_secret , access_token, access_token_secret = getTokens(numAuth)
    #print(consumer_key , consumer_secret , access_token, access_token_secret)
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth

emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

palabrasStop=["para","ante","ante","esta","tiene","hace","como","tras","eres","jaja","porque","todo","esto","pues","este","pero","echo","amos","person","persona","nada","bien", \
              "and","que","men","cia","nos","pre","con","com","mar","uno","dos","pro","uma","las","los","les","una","ten","tra","del","era",'"tr',"por","par","qui","mas","sti","ido","est","aci","pan","per" \
              "rio","","res","san","esp","ver","ela","emo","amo","one","per","are","sta","son","ale","ida","end","mis","den","tan","car",
              "mos","des","ere","rio","ana","das","bat","ara","cas","ente","man","ari",'"rt',"ist","esi","art","ina","ann","nna",
              "dan","ilo","ser","dia","tal","gra","tur","can","der"]

def remove_urls (vTEXT):
    vTEXT = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', vTEXT, flags=re.MULTILINE)
    return(vTEXT)

def tratarCadena(t,longitud=3):
    t=remove_urls(t)
    emoji_pattern.sub(r'', t)
    t=t.lower()
    tt = '"' + ''.join(c for c in t if ord(c) <= 255).replace("\n", "").replace('"', '') + '"'
    tt=tt.lower().replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u').replace('ñ','nn')
    tt = tt.replace("  "," ").replace("_"," ").replace("@"," ").replace("#"," ").split(" ")

    ##tt=ascii(tt)
    resultado=[]
    stop_words_en = get_stop_words('english')
    stop_words_sp = get_stop_words('spanish')
    for ttt in tt:
        if (ttt=="pp" or ttt=="cs"):
            resultado.append(ttt)
        if len(ttt) > (longitud-1):
            if (ttt not in palabrasStop) and (ttt not in stop_words_sp) and (ttt not in stop_words_en):
                resultado.append(ttt)
    return (" ".join(resultado))

def best_partition_with_girvan_newman(g):
        # detectamos las comunidades,
        coms_per_level = nxcom.girvan_newman(g)
        # seleccionamos el mejor nivel
        max_mod, m_level, m_numcoms, m_coms = float('-inf'), None, None, None
        for level, communities in enumerate(coms_per_level):
            print("BP",level,communities)
            d = {node: i for i, com in enumerate(communities) for node in com}
            m = community.modularity(d, g)
            if m > max_mod:
                print("Hay {} comunidades en el nivel {}. La modularidad es ".format(len(communities), level, m))
                max_mod = m
                m_level = level
                m_numcoms = len(communities)
                m_coms = copy(communities)
        print("Modularidad máxima: {} ({} comunidades, nivel {})".format(max_mod, m_numcoms, m_level))
        return m_coms

def draw_graph_node_colors(g, nodes=[]):
        #pos = nx.kamada_kawai_layout(g)
        # mostramos las aristas
        #nx.draw_networkx_edges(g, pos=pos)
        #cmap = plt.cm.hsv
        #for i, group in enumerate(nodes):
        #    print(i, "/", len(nodes))
        #    # mostramos los nodos del grupo
        #    nx.draw_networkx_nodes(g, pos=pos, nodelist=group, node_color=cmap(i / len(nodes)))


        nx.draw_networkx_edges(g,pos=nx.spring_layout(g))
        colores=['black','red','yellow','green','blue','pink','blue','lime','plum','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white','white']
        cmap = plt.cm.hsv
        for i, group in enumerate(nodes):
            print (i,"/",len(nodes))
            nx.draw_networkx_nodes(g,pos=nx.spring_layout(g), nodelist=group, node_color=colores[i%len(colores)])

def minus_key(key, dictionary):
    shallow_copy = dict(dictionary)
    del shallow_copy[key]
    return shallow_copy

def ahora():
    return datetime.datetime.now()

