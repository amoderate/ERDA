import time

import sqlite3 as sl
from pymongo import MongoClient, GEO2D, GEOSPHERE
import csv
from bson import SON
import json


#set the mongoDB connections

def init_db():
	mongo_con = MongoClient()
	mongo_db = mongo_con.emr_db


def make_geo_index():
	'''Create a new db that contains a searchable geolocation'''
	#this only needs to be run once
	mongo_db.geo_code.create_index([("loc" , GEO2D)])

	list_of_fields = ["response_time", "date", "dispatch_time", "location", "longitude", "latitude"]

	x = [x for x in emr_t.find(fields = list_of_fields)]


	geo_code = mongo_db.geo_code
	bad_geo = mongo_db.bad_geo


	# uncomment if you need to re-run the etl code to avoid concatenating data
	geo_code.remove()
	bad_geo.remove()


	for row in x:
		try:
			geo_code.insert({
				"response_time":row['response_time'], 
				"date" : row['date'],
				"dispatch_time" : row['dispatch_time'],
				'location' : row['location'],
				'longitude': row['longitude'],
				'latitude' : row['latitude'],
				 "loc": [float(row['longitude']), float(row['latitude'])]
				 })
		except:
			bad_geo.insert({
				"response_time":row['response_time'], 
				"date" : row['date'],
				"dispatch_time" : row['dispatch_time'],
				'location' : row['location'],
				'longitude': row['longitude'],
				'latitude' : row['latitude']
				})

			pass

#Test geo index



def load_firestation_density():
	'''Add a count of nearby firestations'''

	mongo_db.fire_geo_code.remove()
	mongo_db.fire_geo_code.create_index([("loc" , GEO2D)])

	fire_geo_code = mongo_db.fire_geo_code
	bad_fire_geo = mongo_db.bad_fire_geo

	#load data from csv into fire_geo_code
	with open('/home/corochan/Downloads/FirehouseData.csv', 'rb') as f:
		reader = csv.reader(f)
		next(reader, None) #skip the header
		for row in reader:

			try:
				fire_geo_code.insert({"loc" : [round(float(row[0]),6), round(float(row[1]),6)]})
			except:
				bad_fire_geo.insert({"longitude" : row[0], "latitude": row[1]})

			print float(row[0])


def get_density(geo_code):
	'''Find the number of firestations within 2 kilometers'''
	fire_geo_code = mongo_db.fire_geo_code

	x =   fire_geo_code.find({"loc": SON([("$near", geo_code), ('$maxDistance', 2.0/111.0)])}).count()

	return x


def find_firestation_density():
	'''for each element in the data - calculate the number of firestaions within 1 km '''
	geo_code = mongo_db.geo_code
	density = mongo_db.geo_code_density
	density.remove()

	location = [x for x in geo_code.find()]
	for row in location:
		# print row['loc']
		row['fire_station_density'] = get_density(row['loc'])
		density.insert(row)