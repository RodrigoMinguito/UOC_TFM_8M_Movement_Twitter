import pandas as pd
from networkx.algorithms.community import modularity_max as nxmod
from pathlib import Path
import os
import threading
import time
import tweepy
import datetime
import csv
import requests
from requests_oauthlib import OAuth1
import util
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

diccionarioAutoresGLOBAL={}

def buscarDicc(diccionario,parametro):
    retorno=parametro
    try:
        retorno=diccionario[parametro]
    except Exception as inst:
        retorno = parametro
    return retorno

def dumpFollowers(sucesos, nombreFich, numAuth):
    header_auth = True

    url = 'https://api.twitter.com/1.1/followers/ids.json'
    consumer_key, consumer_secret, access_token, access_token_secret = util.getTokens(6)
    headeroauth = OAuth1(consumer_key, consumer_secret,access_token, access_token_secret,signature_type='auth_header')
    i=0
    with open(nombreFich, 'a', newline='') as csvfile:
        wr = csv.writer(csvfile)
        for suceso in sucesos:
            j=0
            i=i+1
            print("Thread",numAuth,i,'/',len(sucesos),"Bajamos ", suceso)
            query_params = {'id': suceso }

            while True:
                try:
                    j = j + 1
                    if j>17:
                        break
                    response = requests.get(url, auth=headeroauth, params=query_params)
                    data = response.json()
                    if "errors" in data:
                        print("Thread",numAuth,i,'/',len(sucesos)," ERROR -- ",data,j)
                        time.sleep(60 * 1)
                        continue
                    else:
                        wr.writerow([suceso, data["ids"]])
                        break
                except tweepy.TweepError:
                    print("Thread",numAuth,i,'/',len(sucesos)," ERROR 429 - Esperamos",j)
                    time.sleep(60 * 1)
                    continue
                except Exception as inst:
                    print("Thread",numAuth,i,'/',len(sucesos),"ERROR otro 429 - ",j," Esperamos",type(inst),inst,inst.args)
                    time.sleep(60 * 1)
                    continue


def cortarFecha(cadena):
    d=cadena.split(' ')
    horas=d[1]
    dias=d[0]
    anno=dias.split("-")[0]
    mes = dias.split("-")[1]
    dia = dias.split("-")[2]
    hora=horas.split(":")[0]
    minutos=horas.split(":")[1]
    segundos=horas.split(":")[2]
    return int(anno),int(mes),int(dia),int(hora),int(minutos),int(segundos)

def addToDic(diccionario,clave):
    if clave not in diccionario:
        diccionario[clave]=1
    else:
        diccionario[clave]=diccionario[clave]+1

def unique(tipo,listaNegra,listaBlanca):
    print("INI ", tipo)
    nombreFich = "D:\\temp\\UOC\\myfile_UNIQUE" + tipo + ".csv"
    myFlag = Path(nombreFich)

    if not myFlag.exists():

        ficherosIN = []
        for filename in os.listdir("D:\\temp\\UOC\\"+tipo+"\\"):
                ficherosIN.append("D:\\temp\\UOC\\"+tipo+"\\"+filename)
        seen = set()

        csvfile    = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)

        for nombreFichIN in ficherosIN:
            print(nombreFichIN)
            with open(nombreFichIN) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for linea in csv_reader:

                    if not (tipo=="Autores"):
                        if (listaBlanca==""):
                            volcar = True
                            for tag in listaNegra:
                                if tag in str(linea):
                                    volcar=False
                        else:
                            volcar = False
                            for tag in listaBlanca:
                                if tag in str(linea):
                                    volcar=True
                                    #print(tag,linea)
                    else:
                        volcar=True

                    if volcar:
                        msg=tuple(linea)
                        if (msg not in seen):
                            seen.add(msg)
                            writer.writerow(msg)

    print("FIN ",tipo)

def unicosTweets():
    nombreFich = "D:\\temp\\UOC\\myfile_UNIQUETweets_ord_FECHA.csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        csvfile = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)
        csvfileTweets = open("D:\\temp\\UOC\\TweetsGenuinos.csv", 'w', newline='')
        writerTweets = csv.writer(csvfileTweets)

        seen = set()
        diccionario={}

        ficheroIN="D:\\temp\\UOC\\myfile_UNIQUETweetsUnicos.csv"
        print(ficheroIN)
        with open(ficheroIN) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for linea in csv_reader:
                    id=linea[0]
                    autor=linea[1]
                    fecha=linea[2]
                    texto = linea[3]
                    msg = tuple([fecha, id, autor])
                    seen.add(msg)
                    if ("RT @" not in texto):
                        addToDic(diccionario,texto)

        for msg in sorted(list(seen)):
            writer.writerow(msg)

        lista=[]
        for nombre, veces in diccionario.items():
            if (veces>1):
                lista.append(tuple([veces,nombre]))
        for msg in sorted(lista,reverse=True):
            writerTweets.writerow(msg)

def unicos(tipo):

    print("\n",tipo)

    diccionarioIdReTweets={}
    diccionarioIdTweetOriginal = {}
    diccionarioIdRetweteador = {}
    diccionarioIdAutor = {}
    seen = set()

    nombreFichUnicos = "D:\\temp\\UOC\\myfile_UNIQUE"+tipo+"_ord_FECHA.csv"
    myFlag = Path(nombreFichUnicos)
    if not myFlag.exists():
        csvfileUnicos    = open(nombreFichUnicos, 'w', newline='')
        writerUnicos = csv.writer(csvfileUnicos)

        nombreFichSospechosos = "D:\\temp\\UOC\\myfile_UNIQUE"+tipo+"Sospechosos.csv"
        csvfileSospechosos    = open(nombreFichSospechosos, 'w', newline='')
        writerSospechosos = csv.writer(csvfileSospechosos)

        nombreFichMencionados = "D:\\temp\\UOC\\myfile_UNIQUE"+tipo+"Mencionados.csv"
        csvfileMencionados    = open(nombreFichMencionados, 'w', newline='')
        writerMencionados = csv.writer(csvfileMencionados)

        nombreFichMencionadores = "D:\\temp\\UOC\\myfile_UNIQUE"+tipo+"Mencionadores.csv"
        csvfileMencionadores    = open(nombreFichMencionadores, 'w', newline='')
        writerMencionadores = csv.writer(csvfileMencionadores)

        nombreFichImpactos = "D:\\temp\\UOC\\myfile_UNIQUE"+tipo+"Impactos.csv"
        csvfileImpactos    = open(nombreFichImpactos, 'w', newline='')
        writerImpactos = csv.writer(csvfileImpactos)

        nombreFichVeces = "D:\\temp\\UOC\\myfile_UNIQUE"+tipo+"Veces.csv"
        csvfileVeces    = open(nombreFichVeces, 'w', newline='')
        writerVeces = csv.writer(csvfileVeces)

        nombreFichIN="D:\\temp\\UOC\\myFile_UNIQUE"+tipo+".csv"
        with open(nombreFichIN) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for linea in csv_reader:
                    idTweet=linea[0]
                    idTweetOriginal = linea[1]
                    idRetweteador = linea[2]
                    idAutor=linea[3]
                    hashtag=linea[4]
                    fechaCreacion=linea[5]
                    fechaRetweteo=linea[6]
                    addToDic(diccionarioIdTweetOriginal, idTweetOriginal)
                    addToDic(diccionarioIdRetweteador, idRetweteador)
                    addToDic(diccionarioIdAutor, idAutor)
                    addToDic(diccionarioIdReTweets,idTweetOriginal)

                    msg=tuple([fechaRetweteo,idTweetOriginal,idTweet,idAutor,idRetweteador,fechaCreacion])
                    seen.add(msg)

        for msg in sorted(list(seen)):
            writerUnicos.writerow(msg)

        lista=[]
        for nombre, veces in diccionarioIdTweetOriginal.items():
            if (veces>0):
                lista.append(tuple([veces,nombre]))
        for msg in sorted(lista,reverse=True):
            writerImpactos.writerow(msg)

        lista=[]
        for nombre, veces in diccionarioIdRetweteador.items():
            if (veces>0):
                lista.append(tuple([veces,nombre]))
        for msg in sorted(lista,reverse=True):
            writerMencionadores.writerow(msg)

        lista=[]
        for nombre, veces in diccionarioIdAutor.items():
            if (veces>0):
                lista.append(tuple([veces,nombre]))
        for msg in sorted(lista,reverse=True):
            writerMencionados.writerow(msg)

        lista=[]
        for nombre, veces in diccionarioIdReTweets.items():
            if (veces>0):
                lista.append(tuple([veces,nombre]))
        for msg in sorted(lista,reverse=True):
            writerVeces.writerow(msg)


def generarDiccionario():
    nombreFichAutoresReferencias = "D:\\temp\\UOC\\myfileAutoresReferenciados.csv"
    nombreFichAutoresOrdenado = "D:\\temp\\UOC\\myfileAutoresOrdenados.csv"
    seenTweets = set()  # set for fast O(1) amortized lookup

    diccionarioFechaAlta={}
    diccionarioSucesos = {}
    diccionarioNombre = {}

    csvfileReferencias      = open(nombreFichAutoresReferencias, 'w', newline='')
    csvfileOrdenados        = open(nombreFichAutoresOrdenado, 'w', newline='')
    writerReferencias       = csv.writer(csvfileReferencias)
    writerOrdenados         = csv.writer(csvfileOrdenados)

    nombreFichAutoresIN = "D:\\temp\\UOC\\myfile_UNIQUEAutores.csv"
    with open(nombreFichAutoresIN) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for linea in csv_reader:
                line=str(linea).replace("[","").replace("]","").replace("'","").replace('"',"")
                if line not in seenTweets:
                    seenTweets.add(line)

                    registro=line.split(",")

                    persona=(registro[0]).strip()
                    nombrePersona=registro[1].strip()
                    fecha=registro[3].strip()
                    diccionarioAutoresGLOBAL[persona] = nombrePersona

                    if not persona in diccionarioSucesos:
                        diccionarioSucesos[persona]=0

                    diccionarioSucesos[persona]=diccionarioSucesos[persona]+1
                    diccionarioNombre[persona]=nombrePersona
                    if "2" in fecha:
                        diccionarioFechaAlta[persona]=fecha


    listaFecha=[]
    listaReferencias=[]
    for persona, nombrePersona in diccionarioNombre.items():
        numSucesos  =   diccionarioSucesos[persona]
        if persona in diccionarioFechaAlta:
            fechaAlta   =   diccionarioFechaAlta[persona]
        else:
            fechaAlta = ""
        listaFecha.append(tuple([fechaAlta,persona,nombrePersona,numSucesos]))
        listaReferencias.append(tuple([numSucesos,persona,nombrePersona,fechaAlta]))

    listaFecha=sorted(listaFecha,reverse=True)
    listaReferencias = sorted(listaReferencias, reverse=True)

    for l in listaReferencias:
        writerReferencias.writerow(l)
    for l in listaFecha:
        writerOrdenados.writerow(l)

def tratarAutores():
    nombreFichs=[]
    for filename in os.listdir("D:\\temp\\UOC\\"):
        if ("myfileRetweetsMencionadores.csv" in filename)  or ("myfileRetweetsMencionados.csv" in filename) or ("myfileMencionesMencionadores.csv" in filename) or ("myfileMencionesMencionados.csv" in filename):
            nombreFichs.append("D:\\temp\\UOC\\"+filename)

    myList = []
    myLista = []

    #De las personas que han sido más mencionadas, bajo los followers
    for nombreFich in nombreFichs:
        with open(nombreFich) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for linea in csv_reader:
                if (int(linea[0])>100):
                    myLista.append(linea[1])
    myLista=list(set(myLista))
    NUM_AUTORES=len(myLista)
    print("PERSONAS DE LAS QUE BAJAR INFO:",NUM_AUTORES)

    now = datetime.datetime.now()
    nombreFich = "D:\\temp\\UOC\\myFollowers_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "_" + str(
        now.hour) + ".csv"

    tercio=int(NUM_AUTORES/3)
    sucesos1=myLista[0:tercio] #De 0 a la mitad
    sucesos2=myLista[tercio:tercio*2]
    sucesos3=myLista[tercio*2:99999999999]

    processThread1 = threading.Thread(target=dumpFollowers, args=(sucesos1, nombreFich, 1,))
    processThread1.start()

    time.sleep(5)
    processThread2 = threading.Thread(target=dumpFollowers, args=(sucesos2, nombreFich, 2,))
    processThread2.start()

    time.sleep(5)
    processThread3 = threading.Thread(target=dumpFollowers, args=(sucesos3, nombreFich, 3,))
    processThread3.start()

def unicosAutores():
    nombreFich = "D:\\temp\\UOC\\myfile_UNIQUE_Autores_ID.csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        csvfile    = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)

        nombreFichAutoresIN = "D:\\temp\\UOC\\myfile_UNIQUEAutores.csv"
        seen=set()
        with open(nombreFichAutoresIN) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for linea in csv_reader:
                    id=linea[0]
                    nombre = linea[1]
                    creacion = linea[3]
                    msg=tuple([id,nombre,creacion])
                    if msg not in seen:
                        if not (creacion == "" ):
                            writer.writerow(msg)
                            seen.add(msg)

def rellenarDiccionarioAutores(diccionario,diccionario2):
    print("rellenarDiccionarioAutores")
    nombreFichAutoresIN = "D:\\temp\\UOC\\myfile_UNIQUE_Autores_ID.csv"
    with open(nombreFichAutoresIN) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for linea in csv_reader:
                clave=linea[0]
                msg=tuple([linea[1],linea[2]])
                msg = linea[1]
                diccionario[clave]=msg
                diccionario2[linea[1]]=clave

def rellenarDiccionarioTweets(diccionario):
    print("rellenarDiccionarioTweets")
    ficheroIN = "D:\\temp\\UOC\\myfile_UNIQUETweetsUnicos.csv"
    with open(ficheroIN) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for linea in csv_reader:
            id = linea[0]
            diccionario[id]=str(linea)


def mencionesPuras():
    nombreFichIN="D:\\temp\\UOC\\myFile_UNIQUEMenciones.csv"

    nombreFich = "D:\\temp\\UOC\\myfile_UNIQUEMencionesPuras.csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        csvfile    = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)
        seen=set()

        with open(nombreFichIN) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')

                for linea in csv_reader:
                    fechaCreacion=linea[5]
                    fechaRetweteo=linea[6]
                    if (fechaCreacion == fechaRetweteo):
                        msg = tuple(linea)
                        seen.add(msg)

        for msg in sorted(list(seen)):
            writer.writerow(msg)

def secuenciaSucesos():
    print("Secuencia")

    nombreFich = "D:\\temp\\UOC\\SecuenciaSucesos.csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        csvfile    = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)
        seen=set()

        nombreFichIN = "D:\\temp\\UOC\\myfile_UNIQUERetweets_ord_FECHA.csv"
        with open(nombreFichIN) as csv_file:
           csv_reader = csv.reader(csv_file, delimiter=',')
           for linea in csv_reader:
               seen.add(tuple(linea))
        nombreFichIN = "D:\\temp\\UOC\\myfile_UNIQUEMencionesPuras_ord_FECHA.csv"
        with open(nombreFichIN) as csv_file:
           csv_reader = csv.reader(csv_file, delimiter=',')
           for linea in csv_reader:
               seen.add(tuple(linea))
        for msg in sorted(list(seen)):
            writer.writerow(msg)

def acumuladoMinuto(tipo):
    nombreFich = "D:\\temp\\UOC\\Minutos_"+tipo+".csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        csvfile    = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)
        nombreFichIN = "D:\\temp\\UOC\\myfile_UNIQUE"+tipo+"_ord_FECHA.csv"
        with open(nombreFichIN) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            minutoAnterior=""
            acumulado=0
            for linea in csv_reader:
                minuto=str(linea[0].split(":")[0])+":"+(str(linea[0].split(":")[1])[0])
                print(minuto)
                if minuto==minutoAnterior:
                    acumulado=acumulado+1
                if not(minuto == minutoAnterior):
                    if not (minutoAnterior==""):
                        writer.writerow(tuple([acumulado,minutoAnterior+"0:00"]))
                    minutoAnterior=minuto
                    acumulado=1

def retweetsHechosPorUsuario():
    nombreFich = "D:\\temp\\UOC\\RetweetsHechosPorUsuario.csv"
    nombreFich2 = "D:\\temp\\UOC\\OtrosUsuariosRetuiteadosHechosPorUsuario.csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        diccionarioRetweets={}
        diccionarioAQuienRetuitea = {}
        csvfile    = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)
        csvfile2    = open(nombreFich2, 'w', newline='')
        writer2 = csv.writer(csvfile2)

        nombreFichIN = "D:\\temp\\UOC\\myfile_UNIQUERetweets_ord_FECHA.csv"
        with open(nombreFichIN) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for linea in csv_reader:
                usuarioRetweteador=linea[4]
                usuarioAutor=linea[3]
                idTweetRetweteado=linea[1]
                if usuarioRetweteador not in diccionarioRetweets:
                    diccionarioRetweets[usuarioRetweteador]=idTweetRetweteado
                    diccionarioAQuienRetuitea[usuarioRetweteador] = usuarioAutor
                else:
                    diccionarioRetweets[usuarioRetweteador] = str(diccionarioRetweets[usuarioRetweteador])+","+idTweetRetweteado
                    diccionarioAQuienRetuitea[usuarioRetweteador] = str(diccionarioRetweets[usuarioRetweteador]) + "," + usuarioAutor
        for usuarioRetweteador, ids in diccionarioRetweets.items():
            writer.writerow(tuple([usuarioRetweteador,list(ids.split(","))]))
        for usuarioRetweteador, ids in diccionarioAQuienRetuitea.items():
            writer2.writerow(tuple([usuarioRetweteador,list(ids.split(","))]))

def detectarComportamientosSimilares(numeroMinimo,porcentajeMinimoSimilitud,tipo):
    print("\n\n DetectarComportamientosSimilares("+str(numeroMinimo)+","+str(porcentajeMinimoSimilitud)+","+tipo+")")
    nombreFich = "D:\\temp\\UOC\\"+tipo+"ersSospechosos_Al_"+str(porcentajeMinimoSimilitud)+"_porciento.csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        diccionario = {}
        csvfile    = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)
        nombreFichIN = "D:\\temp\\UOC\\"+tipo+"HechosPorUsuario.csv"
        with open(nombreFichIN) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for linea in csv_reader:
                usuarioRetweteador=linea[0]
                idTweetsRetweteados=linea[1]
                if (len(list(idTweetsRetweteados.split(",")))>(numeroMinimo-1)):
                    diccionario[usuarioRetweteador]=list(idTweetsRetweteados.replace("[","").replace("]","").split(","))
        print("\nTenemos ",len(diccionario),"usuarios que mirar")

        j=0
        diccionario2=diccionario
        for usuarioComparar, RTsComparar in diccionario.items():
            j=j+1
            if j%100==0:
                print(util.ahora(),str(j),"/",len(diccionario)," - Diccionario2",str(len(diccionario2)))
            lista1 = set(RTsComparar)
            lenLista1=len(lista1)
            for usuarioComparado, RTsComparado in diccionario2.items():
                    lista2 = set(RTsComparado)
                    if lenLista1==len(lista2):
                        if lista1==lista2:
                            if not (usuarioComparar == usuarioComparado):
                                print(diccionario_id_datosUsuario[usuarioComparar],diccionario_id_datosUsuario[usuarioComparado], (lista1 & lista2))
                                writer.writerow(tuple([usuarioComparar,usuarioComparado]))
            diccionario2=util.minus_key(usuarioComparar,diccionario2)

def mostarActividadUsuarios(listaUsuarios,nume,diccionario_id_datosUsuario,diccionario_tweets):
    nombreFichIn = "D:\\temp\\UOC\\myfile_UNIQUERetweets_ord_FECHA.csv"
    nombreFichOut = "D:\\temp\\UOC\\LOGS\\LOG"+str(nume)+".txt"

    with open(nombreFichIn) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        #[fechaRetweteo, idTweetOriginal, idTweet, idAutor, idRetweteador, fechaCreacion]
        log=set()
        log2 = set()
        log3 = set()
        listaTweets=set()
        usuariosAnalizados = set()
        for linea in csv_reader:
            fecha=linea[0]
            idTweetOriginal=linea[1]
            idTweet = linea[2]
            idAutor = linea[3]
            idRetweteador = linea[4]
            if (idRetweteador in listaUsuarios):
                log.add(tuple([fecha,idTweetOriginal,idRetweteador]))
                log2.add(tuple([idTweetOriginal, fecha, idRetweteador]))
                listaTweets.add(idTweetOriginal)

        writer = open(nombreFichOut, "a")
        writer.write("\n\n\n\nANALIZAMOS:"+str(listaUsuarios)+"\n\n")
        for id in list(listaTweets):
            try:
                writer.write("\n" + diccionario_tweets[id])
            except Exception as inst:
                print("")

        writer.write("\n\nLog por tiempo")
        for ll in sorted(log):
                writer.write("\n"+str(ll))

        writer.write("\nLog por tweet")
        for ll in sorted(log2):
                writer.write("\n"+str(ll))
        writer.write("\n\n\n")

        nombreFichIn = "D:\\temp\\UOC\\SonElMismoUsuario.csv"
        csvfile    = open(nombreFichIn, 'a', newline='')
        writerSonElMismo = csv.writer(csvfile)
        writerSonElMismo.writerow(listaUsuarios)

def logActividadRetweetersSimilares(porcentajeMinimoSimilitud,diccionario_id_datosUsuario,tipo,diccionario_tweets):
    nombreFichIn = "D:\\temp\\UOC\\" + tipo + "ersSospechosos_Al_" + str(porcentajeMinimoSimilitud) + "_porciento.csv"
    with open(nombreFichIn) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        seen=set()
        for linea in csv_reader:
            usuario1 = linea[0]
            usuario2 = linea[1]
            tuple1=tuple([usuario1,usuario2])
            tuple2 = tuple([usuario2, usuario1])
            if tuple1 not in seen and tuple2 not in seen:
                seen.add(tuple1)
        diccionario={}
        for s in seen:
            elementoComparar0=s[0]
            elementoComparar1=s[1]
            diccionario[elementoComparar0]=elementoComparar0+","+elementoComparar1
            for s2 in seen:
                e0 = s2[0]
                e1 = s2[1]
                if (elementoComparar0==e0 and not(elementoComparar1==e1)):
                    diccionario[elementoComparar0]=diccionario[elementoComparar0]+","+e1

        i=0
        for usuarioComparar, otrosUsuario in diccionario.items():
            print("\n\nANALIZAMOS ",otrosUsuario)
            i=i+1
            mostarActividadUsuarios(list(otrosUsuario.split(",")),i,diccionario_id_datosUsuario,diccionario_tweets)

#Busca usuario con comportamiento sospechoso
def redux(tipo,diccionario_id_datosUsuario,nivelImportanciaRetweetsParaCalculoComunidades):
    print("Redux ",tipo, "nivel",nivelImportanciaRetweetsParaCalculoComunidades)
    nombreFich = "D:\\temp\\UOC\\SonElMismoUsuario.csv"
    prohibidos=set()
    seen=set()
    with open(nombreFich) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for linea in csv_reader:
            for p in linea:
                prohibidos.add(p)
    print("Usuarios descartados por considerarse duplicados: ",len(list(prohibidos)))


    nombreFich = "D:\\temp\\UOC\\Reducido_"+tipo+"_nivel_"+str(nivelImportanciaRetweetsParaCalculoComunidades)+".csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        nombreFichIn = "D:\\temp\\UOC\\myfile_UNIQUERetweetsMencionadores.csv"
        with open(nombreFichIn) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for linea in csv_reader:
                if (int(linea[0])>(nivelImportanciaRetweetsParaCalculoComunidades-1)):
                    seen.add(linea[1])

        nombreFichIn = "D:\\temp\\UOC\\myfile_UNIQUERetweetsMencionadoS.csv"
        with open(nombreFichIn) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for linea in csv_reader:
                if (int(linea[0])>(nivelImportanciaRetweetsParaCalculoComunidades-1)):
                    seen.add(linea[1])

        conjunto=set()
        csvfile    = open(nombreFich, 'w', newline='')
        writer = csv.writer(csvfile)
        nombreFichIn = "D:\\temp\\UOC\\myfile_UNIQUE"+tipo+"_ord_FECHA.csv"
        with open(nombreFichIn) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for linea in csv_reader:
                #fechaRetweteo,idTweetOriginal,idTweet,idAutor,idRetweteador,fechaCreacion
                autor1=linea[4]
                autor2 = linea[3]
                msg  = tuple([autor1, autor2])
                msg2  = tuple([autor2, autor1])
                if not (autor1 == autor2) and autor2 not in prohibidos and autor1 not in prohibidos and msg not in conjunto and msg2 not in conjunto:
                    conjunto.add(msg)
                    msg=tuple([linea[1],linea[2]])
                    try:
                        msg3 = tuple([diccionario_id_datosUsuario[autor1], diccionario_id_datosUsuario[autor2]])
                        msg3 = tuple([autor1, autor2])
                        if autor1 in seen and autor2 in seen:
                            writer.writerow(msg3)
                    except Exception as inst:
                        print(msg)
                        writer.writerow(msg)
                        continue


def detectarComunidades(
        nivelImportanciaRetweetsParaCalculoComunidades,
        numeroDeUsuariosPorComunidad,
        diccionario_id_datosUsuario):

    print("\n\ndetectarComunidades. Nivel:",nivelImportanciaRetweetsParaCalculoComunidades)

    nombreFichIn = "D:\\temp\\UOC\\Reducido_Retweets_nivel_" + str(nivelImportanciaRetweetsParaCalculoComunidades) + ".csv"
    nombreFicheroGrafoIn = "D:\\temp\\UOC\\grafo_nivel_" + str(nivelImportanciaRetweetsParaCalculoComunidades) + ".gml"
    nombreFichComunidades = "D:\\temp\\UOC\\comunidades_nivel_" + str(nivelImportanciaRetweetsParaCalculoComunidades) + ".csv"

    myFlag = Path(nombreFichComunidades)
    if not myFlag.exists():
        fichComunidades = open(nombreFichComunidades, 'w', newline='')
        writerComunidades = csv.writer(fichComunidades)

        print("El fichero de comunidades no existe, Se crea.")
        myFlag = Path(nombreFicheroGrafoIn)
        G = nx.Graph()
        if not myFlag.exists():
            print("El fichero de grafo no existia, se carga")
            with open(nombreFichIn) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for linea in csv_reader:
                    G.add_edge(
                        buscarDicc(diccionario_id_datosUsuario,linea[0]),
                        buscarDicc(diccionario_id_datosUsuario,linea[1]))
                nx.write_gml(G, nombreFicheroGrafoIn)
        else:
            print("El fichero de grafo existia, se carga del gml")
            G = nx.read_gml(nombreFicheroGrafoIn)


        print("Fichero cargado")
        # detectamos comunidades en el grafo de karate con el algoritmo de Clauset-Newman-Moore:
        comunidades = nxmod.greedy_modularity_communities(G)
        print("Clauset-Newman-Moore - Se han detectado {} comunidades:".format(len(comunidades)))

        m_coms2 = sorted(comunidades, key=len, reverse=True)
        com10 = m_coms2[0:100]

        tamTotal = len(G.nodes())
        diccionarioPopularidadPR = nx.pagerank(G)

        total = 0
        for j, comunidad in enumerate(com10):
            tamanno = len(comunidad)
            print("\n\nComunidad Numero {} con {} elementos. Representa el {}% de los usuarios".format(j, tamanno, ( tamanno / tamTotal) * 100))
            total = total + (tamanno / tamTotal) * 100
            print(comunidad)
            col_names = ['Usuario', 'PageRank']
            pagerank = pd.DataFrame(columns=col_names)
            for key in comunidad:
                pagerank.loc[len(pagerank)] = [key, diccionarioPopularidadPR[key]]
            usuariosImportantes = pagerank.sort_values(['PageRank'], ascending=[0])[0:numeroDeUsuariosPorComunidad]['Usuario']
            writerComunidades.writerow(tuple(usuariosImportantes))
        print("Se cubre al {}% de los usuarios".format(total))
    else:
        print("El fichero de comunidades existe, No se hace nada")

def dibujarComunidad(nivelImportanciaRetweetsParaCalculoComunidades,nodosDeCadaComunidad=40):
    print("DibujarComunidad - "+str(nivelImportanciaRetweetsParaCalculoComunidades))

    nombreFicheroGrafoInPNG = "D:\\temp\\UOC\\grafo_nivel_" + str(nivelImportanciaRetweetsParaCalculoComunidades) + ".png"
    nombreFicheroGrafoIn = "D:\\temp\\UOC\\grafo_nivel_" + str(nivelImportanciaRetweetsParaCalculoComunidades) + ".gml"

    myFlag = Path(nombreFicheroGrafoInPNG)
    if not myFlag.exists():
        print("El fichero PNG no existe. Se crea")
        colores=["purple","green","red","orange","black","yellow","pink","brown","gray","darkgray"]
        diccionarioPartidos = {'purple': 'Podemos', 'green': 'VOX', 'red': 'PSOE',
                   'orange': 'Grupo4' , 'black': 'Grupo5', 'yellow': 'Grupo6',
                   'pink': 'Grupo7' , 'brown': 'Grupo8', 'gray': 'Grupo9','darkgray': 'Grupo10'
                   }
        nombreFichIn = "D:\\temp\\UOC\\comunidades_nivel_"+str(nivelImportanciaRetweetsParaCalculoComunidades)+".csv"

        diccionarioColores={}
        seen=set()
        cabeceras=list()
        with open(nombreFichIn) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            i=0
            for linea in csv_reader:
                header=True
                j=0
                for l in linea:
                    j=j+1
                    if i<10 and j<nodosDeCadaComunidad :
                        diccionarioColores[l]=colores[i]
                        seen.add(l)
                        if header:
                            header=False
                            cabeceras.append(l)
                i=i+1

        G = nx.read_gml(nombreFicheroGrafoIn)
        G = max(nx.connected_component_subgraphs(G), key=len)
        G2 = nx.Graph()
        color_map = []
        for node in G.nodes:
            if node in seen:
                G2.add_node(node)
                color_map.append(diccionarioColores[node])
        for edge in G.edges:
            if edge[0] in seen and edge[1] in seen:
                G2.add_edge(edge[0],edge[1])

        UG = G2.to_undirected()
        plt.axis('off')
        plt.tight_layout()
        plt.title('Comunidades de usuarios con más de '+str(nivelImportanciaRetweetsParaCalculoComunidades)+" retuits")
        nx.draw_spring(UG, node_color=color_map, node_size=15, with_labels=False,style="dotted",alpha=0.75,label=color_map)

        patchList = list()
        vistos=set()
        for key in diccionarioColores:
            data_key = mpatches.Patch(color=diccionarioColores[key], label=(diccionarioPartidos[diccionarioColores[key]] ))
            if (diccionarioPartidos[diccionarioColores[key]] not in vistos):
                patchList.append(data_key)
                vistos.add(diccionarioPartidos[diccionarioColores[key]])

        plt.margins(0)
        plt.legend(handles=patchList,loc="lower right",fontsize='x-small')
        plt.tight_layout()
        plt.savefig("D:\\temp\\UOC\\grafo_nivel_" + str(nivelImportanciaRetweetsParaCalculoComunidades) + ".png",dpi=600,figsize=(200,200))
    else:
        print("El fichero gráfico ya existe")

def caracterizarComunidades(
        nivelImportanciaRetweetsParaCalculoComunidades,
        numeroDeTweetsPorUsuario,
        diccionario_nombre_datosUsuario,
        bajarUltimosTweets=0):

    print("caracterizarComunidades")

    nombreFichComunidades = "D:\\temp\\UOC\\comunidades_nivel_" + str(nivelImportanciaRetweetsParaCalculoComunidades) + ".csv"

    with open(nombreFichComunidades) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for linea in csv_reader:
            limpio = []
            limpio2 = []
            for n in linea:
                identificador=diccionario_nombre_datosUsuario[n]
                print(n)
                nombreFichOut="D:\\temp\\UOC\\usuarios\\"+identificador+".txt"
                myFlagFicheroUsuario = Path(nombreFichOut)
                if not myFlagFicheroUsuario.exists():
                    print("No hay tweets bajados del usuario. Forzamos la bajada")

                if ((not myFlagFicheroUsuario.exists()) or (not bajarUltimosTweets==0)):
                    writer = open(nombreFichOut, "a")
                    util.getUserInfo(identificador,writer,False)
                    ultimosTweets = util.getLatestTweets(identificador, numeroDeTweetsPorUsuario)
                    ultimosTweets=set(ultimosTweets)
                    for ll in ultimosTweets:
                        try:
                            writer.write(str(ll[2])+"\n")
                        except Exception as inst:
                            print("EXCEPT")
                            print(type(inst))  # the exception instance
                            print(inst.args)  # arguments stored in .args
                            print(inst)  # __str__ allows args to be printed directly
                file = open(nombreFichOut, "r")
                seen=set()
                for line in file:
                    if line not in seen: #Cada tweet una unica vez por cada usuario
                        t = util.tratarCadena(line)
                        t2 = util.tratarCadena(line, 4)
                        limpio.append(t)
                        limpio2.append(t2)
                        seen.add(line)

            textoCompleto = ' '.join(limpio)
            listaCompleto = textoCompleto.split(' ')
            wordfreq = [[w, textoCompleto.count(w)] for w in list(set(listaCompleto))]
            wordfreq = sorted(wordfreq, key=lambda x: x[1], reverse=True)
            print(wordfreq[0:20])

            textoCompleto = ' '.join(limpio2)
            listaCompleto = textoCompleto.split(' ')
            wordfreq = [[w, textoCompleto.count(w)] for w in list(set(listaCompleto))]
            wordfreq = sorted(wordfreq, key=lambda x: x[1], reverse=True)
            print(wordfreq[0:20])

#Vuelca los datos de un usuario
def caracterizarUsuarios(diccionario):
    listaUsuarios=["'HIDALIATP'","'smilypapiking'","'vmm7773'","'Alvisepf'","'Flor_Eciendo'","'CristinaSegui_'","'agusfeministt'","'monasterioR'","'Adrilastra'","'Renez_kalo'","'eledhmel'","'LEON20199'","'lucianasavarese'","'FSerranoCastro'","'tengounlimon'","'Pietro__Bruno'","'pardodevera'","'alonso_dm'","'Paramount_miss'","'000120o'","'protestona1'","'eldiegoalonso'","'NievesG80421106'","'DoreliDeca'","'AntonioMaestre'","'J_Zaragoza_'","'ririboynic'","'Irene_Montero_'","'Nachoanon'","'M_sofialeivab'","'Mulher_Noticia'","'FidelSanchezP'","'Nashalee_lee'","'DZ_AI_ML'","'Michell69330944'","'IvanGarciaCale4'","'Conexion_AM'","'JulioDMartinez3'","'KarlaCeronFlor1'","'LIPPI2'","'YORFANNY25'","'GPPRIDiputados'","'LuzMelyReyes'","'cadulorena'","'Matilde_Aliende'","'caasdrd'","'Edomex'","'AlexandrapenaRD'","'mathfreireb_'","'AMontasOficial'","'OnmpriNacional'","'Adess8'","'lucasavieira'","'JoseLuisHerzS'","'BelarminioR'","'alfredodelmazo'","'reymund6'","'afelipes7169'","'mayrape83070800'","'CulturaEdomex'","'CManagerAlex'","'andandrea'","'patriiiIeyvaaa'","'sergiagalvan'","'MexiquenseTV'","'yaribeatrizp'","'Cecillia'","'jilguerito95'","'kinssot'","'JOSEISAACSOLAN1'","'rosarifeliz10'","'ma_achaves'","'patv113'","'RMexiquense'","'TerezaMoren'","'marubimo'","'CastigadorY'","'numer344'","'mtgarcia82'","'KilianCD'","'joseantoniokast'","'tere_marinovic'","'amarielgg'","'albertoplaza'","'jtgazmuri'","'VillanuevaRH'","'CocolisoDelSur'","'DeliaDG'","'LordHeller2022'","'EstherRuizCs'","'PatriciaReyesCs'","'toniroldanm'","'RIVAS_Llanera'","'ferrerodu'","'ReinaSonia'","'pepecampostruji'","'emparpm'","'AgustinLaje'","'SurianiSalta'","'AnaBelenMarmora'","'unidadprovida'","'JucumArgentina'","'HoracioRubio97'","'MxlaVidaoficial'","'camila_duro'","'f_secin'","'nicoleprovida'","'prazapublica'","'galizaCIG'","'anaponton'","'e_lores'","'SermosGaliza'","'gzcontrainfo'","'OSaltogz'","'orodil'","'pilaraymara'","'treballadors'","'55gpunto'","'amandaasubiar'","'LuisaMaldonadoM'","'CompromisoRC5'","'CELAGeopolitica'","'aolaPabonC'","'monicaramela'","'wgomezr'","'MaiadelC'","'GracielaMoraEc'","'LaKolmenaEC'","'1alos22'","'FonziDolores'","'pictoline'","'LyaGonzalez1'","'posnomiris'","'alemaan7'","'lsabelleParrish'","'HistoriaEnFotos'","'LaResistencia'","'davidolivas'","'deitemi'","'huggingmimi'","'AriOrsingher'","'MidiaNINJA'","'laurarosba'","'mariandalesio'","'SweetSalvat0re'","'lolaindigomusic'","'soyingridbeck'","'anapolo___'","'miriamrmusic_'","'catyeyer'","'Coordinadora8m'","'Martalar'","'chiaractt'","'nlopeztrujillo'","'MonicaCarrillo'","'subelaradio'","'EspMartina'","'huachacomunista'","'julietacer98'","'fookingspider'"]
    for usuario in listaUsuarios:
        print("\n\nClave ",usuario)
        try:
            id=diccionario[usuario]
            util.getUserInfo(id, None, True)
        except Exception as inst:
            print("El usuario no existe en el diccionario!!")


if __name__ == '__main__':
    nivelesImportancia = [500,200,100,75,50,40,25,15]

    print("DICCIONARIO")
    generarDiccionario()
    tipos=["Retweets","TweetsUnicos","Autores","Menciones"]
    listaNegra=["'DiaDeLaMujer'","'Feminista'","'Feministas'","'mujer'","'8M2019'"]
    listaBlanca = ""
    for t in tipos:
        unique(t,listaNegra,listaBlanca)

    numeroDeTweetsPorUsuario=100
    numeroDeUsuariosPorComunidad=200
    mencionesPuras()
    unicos("Retweets")
    unicos("MencionesPuras")
    secuenciaSucesos()
    unicosTweets()
    unicosAutores()
    acumuladoMinuto("Retweets")
    acumuladoMinuto("MencionesPuras")
    retweetsHechosPorUsuario()

    diccionario_id_datosUsuario = {}
    diccionario_nombre_datosUsuario = {}
    diccionario_tweets = {}
    rellenarDiccionarioAutores(diccionario_id_datosUsuario,diccionario_nombre_datosUsuario)

    rellenarDiccionarioTweets(diccionario_tweets)
    caracterizarUsuarios(diccionario_nombre_datosUsuario)

    nombreFich = "D:\\temp\\UOC\\SonElMismoUsuario.csv"
    myFlag = Path(nombreFich)
    if not myFlag.exists():
        detectarComportamientosSimilares(10, 100, "OtrosUsuariosRetuiteados")
        detectarComportamientosSimilares(10,100,"Retweets")
        logActividadRetweetersSimilares(100,diccionario_id_datosUsuario,"OtrosUsuariosRetuiteados",diccionario_tweets)
        logActividadRetweetersSimilares(100, diccionario_id_datosUsuario,"Retweets",diccionario_tweets)

    for nivelImportanciaRetweetsParaCalculoComunidades in nivelesImportancia:
        redux("Retweets",diccionario_id_datosUsuario,nivelImportanciaRetweetsParaCalculoComunidades)

    for nivelImportanciaRetweetsParaCalculoComunidades in nivelesImportancia:
        print("\n\n\nComunidades a nivel ",nivelImportanciaRetweetsParaCalculoComunidades)
        detectarComunidades(
            nivelImportanciaRetweetsParaCalculoComunidades,
            numeroDeUsuariosPorComunidad,
            diccionario_id_datosUsuario)
        dibujarComunidad(nivelImportanciaRetweetsParaCalculoComunidades)
        caracterizarComunidades(nivelImportanciaRetweetsParaCalculoComunidades,numeroDeTweetsPorUsuario,diccionario_nombre_datosUsuario,0)
