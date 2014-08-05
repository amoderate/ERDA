import time
from pymongo import MongoClient, GEO2D, GEOSPHERE
import csv
from bson import SON
import json

#set the mongoDB connections

def load_geojson():
	'''Load geoJson into MongoDB and create a 2dsphere index'''
	mongo_con = MongoClient()
	mongo_db = mongo_con.erda
	tract = mongo_db.tract

	tract.remove()

	with open('/home/corochan/emr_project/geo_json/dc-gis/census-tracts-2010.geojson', 'rb') as f:
		data = json.load(f)

	for feature in data['features']:
		geometry =  feature['geometry']
		tracts = feature['properties']['TRACT']
		tract.insert({'tract': tracts, 'geometry' : feature['geometry']})

	tract.create_index([('geometry', GEOSPHERE)])

	x = [i for i in tract.find().limit(1)]
	print x



def map_response_data_to_census():
	response_census = mongo_db.response_census

	x = response_census.find({"loc": SON([({'$geoWithin' : {'$geometry' : 
		SON([("type" , "Polygon"), ("coordinates" , 'loc')])}})])}) 
	return x


def make_geo_index():
	'''Create a new db that contains a searchable geolocation'''
	mongo_con = MongoClient()
	mongo_db = mongo_con.erda
	events_collection = mongo_db.events

	#this only needs to be run once
	mongo_db.events_with_census.create_index([("loc" , GEOSPHERE)])



	list_of_fields = ["Response Seconds", "date", "dispatch", "quadrant", "longitude", "latitude"]

	x = [x for x in events_collection.find(fields = list_of_fields)]


	events_with_census = mongo_db.events_with_census
	bad_geo = mongo_db.bad_geo


	# uncomment if you need to re-run the etl code to avoid concatenating data
	events_with_census.remove()
	bad_geo.remove()


	for row in x:
		try:
			events_with_census.insert({
				"Response Seconds":row['Response Seconds'], 
				"date" : row['date'],
				"dispatch" : row['dispatch'],
				'quadrant' : row['quadrant'],
				'longitude': row['longitude'],
				'latitude' : row['latitude'],
				 "loc": [float(row['longitude']), float(row['latitude'])]
				 })
		except:
			bad_geo.insert({
				"Response Seconds":row['Response Seconds'], 
				"date" : row['date'],
				"dispatch" : row['dispatch'],
				'quadrant' : row['quadrant'],
				'longitude': row['longitude'],
				'latitude' : row['latitude']
				})

			pass


def main():

	load_geojson()
	make_geo_index()




if __name__ == "__main__":
	main()

