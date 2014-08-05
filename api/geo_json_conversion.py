import time
from pymongo import MongoClient, GEO2D, GEOSPHERE
import csv
from bson import SON
import json

#set the mongoDB connections

def load_geojson():
	'''Load geoJson into MongoDB and create a 2dsphere index'''
	mongo_con = MongoClient()
	mongo_db = mongo_con.emr_db
	wards = mongo_db.wards

	wards.remove()

	with open('/home/corochan/emr_project/geo_json/dc-gis/wards-2012.geojson', 'rb') as f:
		data = json.load(f)

	for feature in data['features']:
		geometry =  feature['geometry']
		name = feature['properties']['NAME']
		wards.insert({'name': name, 'geometry' : feature['geometry']})

	wards.create_index([('geometry', GEOSPHERE)])

	x = [i for i in wards.find().limit(1)]
	print x
def main():

	load_geojson()


if __name__ == "__main__":
	main()

