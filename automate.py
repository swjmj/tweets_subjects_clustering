from twint import twint
import schedule
import time
import sql_control as sql
import pandas as pd
from datetime import datetime
import numpy as np

import processing_classes.how_many_words as how_many_w
from input_classes import input_hot_encoder as HE





def output_to_dataframe(twint_output):
	extract_tweet = pd.DataFrame(columns=['user', 'tweet', 'RT', 'URL','date_time'])
	for j in twint_output.tweets_list:
		extract_tweet = extract_tweet.append({'user' : j.username, 'tweet' : j.tweet, 'RT' : j.retweet, 'URL' : j.conversation_id,'date_time': datetime.strptime(j.datestamp+' '+j.timestamp, '%Y-%m-%d %H:%M:%S')}, ignore_index=True)
	#print("Dataframe Contens ", extract_tweet, sep='\n')
	extract_tweet = extract_tweet.sort_values(by=['date_time'], ascending=False).reset_index(drop=True)
	return extract_tweet



#this function gets the tweets that are more recent than those in the SQL database
def tiempos(tiempo_tweeter, tiempo_sql):
	k=0
	while tiempo_tweeter[k]>tiempo_sql:
		k+=1
		if k>=20:
			break
	return min(tiempo_tweeter.shape[0], k)


#this function filters out the users from the tweet that are in the following list (in SQL) and those who don't
#then send the respective instructions
def seguidos_prediccion(usuario_cuenta, seguidos, extract_tweet_part, SQL_all):
	hot_encoder = []
	print('palabras tweet', SQL_all[6])
	print('palabras retweet', SQL_all[7])
	for l in seguidos.split(','):
		hot_encoder.append(HE.comparacion(extract_tweet_part.user, l))

	#print(np.sum(np.sum(np.asarray(hot_encoder), axis=0)))

	if np.sum(np.sum(np.asarray(hot_encoder), axis=0))>0:
		filter_TW = np.sum(np.asarray(hot_encoder), axis=0)
		not_in_Follow = extract_tweet_part[filter_TW==0]
		in_Follow = extract_tweet_part[filter_TW!=0]

		for cuenta_us in range(not_in_Follow.shape[0]):
			sql_control = sql.sql_mod()
			#if not_in_Follow.user.iloc[cuenta_us].lower() == usuario_cuenta.lower():
			print('tengo un TW  para predecir') 
			HW = how_many_w.NOPALABRASTWEET()
			print('tweet a predecir ', not_in_Follow.tweet.iloc[cuenta_us])
			decision = HW.count_words( SQL_all[6],  SQL_all[7], not_in_Follow.tweet.iloc[cuenta_us])
			print('la decision es ', decision)
			if decision!='no' and decision == 0:
				sql_control.writing_work(not_in_Follow.URL.iloc[cuenta_us], decision, 'Si como no',  SQL_all[2], SQL_all[3]) 
			if decision!='no' and decision == 1:
				URL_DB_pred = 'https://mobile.twitter.com/statuses/'+not_in_Follow.URL.iloc[cuenta_us]+'/retweet?p=t'
				sql_control.writing_work(URL_DB_pred, decision, 'Si como no, de dia tan rapido',  SQL_all[2], SQL_all[3]) 				
			#else:
			#	print('tengo un RT para predecir')

	

		for cuenta_us in range(in_Follow.shape[0]):
			sql_control = sql.sql_mod()
			if in_Follow.user.iloc[cuenta_us].lower() == usuario_cuenta.lower():
				print('tengo un TW  para ingresar en DB') 
				sql_control.writing_work(in_Follow.URL.iloc[cuenta_us], 0, 'Si como no, la lluvia molesta',  SQL_all[2], SQL_all[3]) 
			else:
				print('tengo un RT para ingresar en DB')
				URL_DB = 'https://mobile.twitter.com/statuses/'+in_Follow.URL.iloc[cuenta_us]+'/retweet?p=t'
				sql_control.writing_work(URL_DB, 1, 'Jojojo desperte!', SQL_all[2], SQL_all[3] ) 
		print('-----------------------\n', not_in_Follow)
		print('-------------------------\n',in_Follow)	

	print(SQL_all[2], SQL_all[3])
	if np.sum(np.sum(np.asarray(hot_encoder), axis=0))==0:
		for us in range(extract_tweet_part.shape[0]):
			sql_control = sql.sql_mod()
			if extract_tweet_part.user[us].lower() == usuario_cuenta.lower():
				print('tengo un TW')
				sql_control.writing_work(extract_tweet_part.URL[us], 0, 'Buen dia',  SQL_all[2], SQL_all[3]) 
			else:
				URL_DB = 'https://mobile.twitter.com/statuses/'+extract_tweet_part.URL[us]+'/retweet?p=t'
				sql_control.writing_work(URL_DB, 1, 'Me quiero dormir!', SQL_all[2], SQL_all[3] ) 
				print('tengo un RT')
		return
	
#función que hace el scraping y se encarga de procesar el tweet, además llama a las funciones que escriben en DB 

def scraping(SQL_info):

	print('Se va a hacer scraping a: ',SQL_info[1])
	c = twint.Config()
	c.Username = SQL_info[1]
	c.Store_object = True
	c.Limit = 1
	try:
		twint.run.Profile(c)
	except:
		return
	extract_tweet = output_to_dataframe(twint.output)
	print('-------------------------------------la última actividad registrada fue a las',SQL_info[-1], '-------------------------------------')
	time_server = SQL_info[-1]
	print(extract_tweet)

	index_time = tiempos(extract_tweet.date_time, time_server)
	extract_tweet_part = extract_tweet[:index_time]

	if index_time>0:
		seguidos_prediccion(SQL_info[1] ,SQL_info[-2], extract_tweet_part, SQL_info)
		

	if index_time==0:
		print('No hay ningún tweet nuevo')	


	sql_control = sql.sql_mod()
	sql_control.writing_training_time(SQL_info[0], extract_tweet.date_time[0])
	# HW = how_many_w.NOPALABRASTWEET()
	# decision = HW.count_words(usuario, 'texto wikileaks')
	# print(decision)
	twint.output.tweets_list = []
	time.sleep(10) 

def ciclo():
	sql_control = sql.sql_mod()
	for i in sql_control.reading_training():
		scraping(i)
	
	print('-----------------------------------------Termine un ciclo--------------------------------------------')


ciclo()

#schedule.every(1).minutes.do(jobone)
schedule.every().hour.do(ciclo)
# schedule.every().day.at("10:30").do(jobone)
# schedule.every().monday.do(jobone)
# schedule.every().wednesday.at("13:15").do(jobone)

while True:
	schedule.run_pending()
	time.sleep(1)
