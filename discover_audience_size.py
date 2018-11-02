import os
import pandas

from pysocialwatcher import watcherAPI
from time import time, localtime
from lib import PersistentSet
from string import ascii_lowercase

COLS = ('id','audience_size','name','path','type')

watcher = watcherAPI()
watcher.load_credentials_file('credentials.txt')

autosave_every_N_seconds = 60

def explore():

	def save():
		unvisited_set.save()
		visited_set.save()

	last_update = time()

	working_folder = 'CACHE_audience_size'
	unvisited_set = PersistentSet(working_folder+'/unvisited.txt')
	visited_set = PersistentSet(working_folder+'/visited.txt')
	
	input_dataframe_file = 'CACHE_interest_discovery/dataframe.csv'
	
	'''
	~~~ SEARCH PARAMETERS ~~~
	
	Parameters supported by the API:
	
	"interests",					# we get this from the list
	"behaviors",					# eg. expats
	"education_statuses",
	"family_statuses",
	"relationship_statuses",
	"locales",						# language
	"genders",						# 0, 1, 2
	"age_min",
	"age_max",
	"geo_locations"					# country
	'''
	
	# You can add more parameters here if you like
	country = ['DE']
	language = ['Arabic']
	behaviors = []

	search_param = {
	'geo_locations' : country,
	'locales' : language,
	'behaviors' : behaviors
	}

	output_dataframe_file = working_folder+'/audience_size_'+ '_'.join(",".join(i) for i in search_param.values() if len(i)>0) + '.csv'
		
	if not os.path.isdir(working_folder):
		os.makedirs(working_folder)

	if not os.path.isfile(input_dataframe_file):
		print("ERROR: Could not find input file \"dataframe.csv\"")
		return
		

	if not os.path.isfile(output_dataframe_file):
		print(output_dataframe_file)
		with open(output_dataframe_file,'w') as out_file:
			out_file.write(','.join(COLS)+'\n')

	if unvisited_set.is_empty():
		data = pandas.read_csv(input_dataframe_file)
		for line in data.iterrows():
			audience = line[1]['audience_size']
			fb_id = line[1]['id']
			# We only need the id
			if (audience >= 10000000):
				unvisited_set.add(str(fb_id))

	while True:
		try:
			unvisited_set.remove_from(visited_set)
			for interest in unvisited_set:
				if time() > last_update + autosave_every_N_seconds:
					last_update = time()
					now = localtime()
					print('Autosaving at {}.{}.{} {}.{}.{}'.format(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec))
					save()
				print(interest)
				search_param['interests'] = [interest]
				
				try:
					
					results = watcher.get_search_targeting_from_query_dataframe(search_param)
					results.to_csv(output_dataframe_file, mode='a', encoding='utf-8', header=False, columns=COLS)
					visited_set.add(str(interest))
				except KeyboardInterrupt:
					raise
				except Exception as error:
					print("ERROR:",error)
					continue
				print
			if unvisited_set.is_empty():
				print('Exploration is finished!')
				save()
				return

		except KeyboardInterrupt:
			print('Saving and exiting')
			save()
			return

if __name__ == '__main__':
	explore()

