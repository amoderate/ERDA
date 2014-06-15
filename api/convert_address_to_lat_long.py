import sys
import csv
import time


from pygeocoder import Geocoder
def get_lat_long(address):
	'''Takes an address and returns lat/long'''
	g  = Geocoder()
	result = g.geocode(address)
	return result[0].coordinates




def import_erda_csv(file_name):
	print(file_name)
	start = time.time()
	data = []
	with open(file_name, 'r') as csv_file:
		reader = csv.reader(csv_file)
		for row in reader:
			data.append(row)
		elapsed = '{:2.2f}'.format(time.time() - start)
		print('Read', len(data) - 1, 'records in', elapsed, 's')
	return data


def to_csv(import_file, output_file):
	'''write lat long out to csv'''
	start = time.time()
	data = import_erda_csv(import_file)

	
	addresses = iter(data)
	with open(output_file, 'w') as csv_out:
		writer = csv.writer(csv_out)
		count = 0
		for address in addresses:
			try:
				lat_long = get_lat_long(str(address)+' Washington, DC')
				writer.writerow([address, lat_long])
				time.sleep(.5)
				count += 1
				print count

			

			except Exception,e: 
				print str(e)
				print 'connection error - sleep for 10 seconds'
				time.sleep(10)
				pass
				
	elapsed = '{:2.2f}'.format(time.time() - start)
	print('Converted', len(data) - 1, 'records in', elapsed, 's')


def usage():
	'''Prints a help string.'''
	print("usage: convert_address_to_lat_long.py <in_filename> <out_file_name>")

if __name__ == "__main__":
	if (len(sys.argv) == 3):
		to_csv(sys.argv[1], sys.argv[2])
	else:
		usage()




