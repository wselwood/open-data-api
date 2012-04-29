from flaskext.mongoalchemy import BaseQuery
import json

from api import db

import lxml.html

class JSONField(db.StringField):
	def unwrap(self, value, *args, **kwargs):
		"""Pass the json field around as a dictionary internally"""
		return json.loads(value)

class DatasetQuery(BaseQuery):
	
	def build_dictionary(self, grin):
		return dict(internal_id = str(grin.mongo_id), 
				image_reference = grin.image_reference, 
				grin_id = grin.grin_id, 
				data_url = grin.data_url, 
				image_creator = grin.image_creator, 
				original_source = grin.original_source, 
				date_time = grin.date_stamp, 
				center_code = grin.center_code, 
				center_name = grin.center_name, 
				short_desc = grin.short_description, 
				full_desc = grin.full_description, 
				keywords = grin.keyword_list, 
				subjects = grin.subject_list, 
				thumbnail_url = grin.thumbnail_url, 
				thumbnail_type = grin.thumbnail_type,
				thumbnail_height = grin.thumbnail_height, 
				thumbnail_width = grin.thumbnail_width, 
				thumbnail_size = grin.thumbnail_size, 
				small_url = grin.small_url, 
				small_type = grin.small_type, 
				small_height = grin.small_height, 
				small_width = grin.small_width, 
				small_size = grin.small_size, 
				medium_url = grin.medium_url, 
				medium_type = grin.medium_type, 
				medium_height = grin.medium_height, 
				medium_width = grin.medium_width, 
				medium_size = grin.medium_size, 
				large_url = grin.large_url, 
				large_type = grin.large_type, 
				large_height = grin.large_height, 
				large_width = grin.large_width, 
				large_size = grin.large_size)
	
	def convert_results(self, results):
		list1 = []
		for grin in results:
			dictionary = self.build_dictionary(grin)
			list1.append(dictionary)
	
		return list1
	
	#Searches on the code given to each center that releases images
	def filter_by_center_code(self, center_code):
		results = self.filter(Grin.center_code == center_code)
		return self.convert_results(results)

	#Searches based on the GRIN id given to an image - unique for each image
	def filter_by_grin_id(self, grin_id):
		results = self.filter(Grin.grin_id == grin_id)
		return self.convert_results(results)

	#Search on a keyword-type basis on the full description. 
	def filter_by_description(self, description):
		results = self.filter({ 'full_description' : {'$regex' : description} })
		return self.convert_results(results)

	#Search in the keyword list
	#Should this to accept comma separated list of keywords?
	def filter_by_keyword(self, keyword):
		results = self.filter({ 'keyword_list' : keyword})
		return self.convert_results(results)
	
	#Search the subject list
	#List keyword this should probably accept a list of keywords
	def filter_by_subject(self, subject):
		results = self.filter({ 'subject_list' : subject})
		return self.convert_results(results)

	#check for a url in the db, used to stop us reloading pages that already have been loaded.
	def filter_by_url(self, url):
		results = self.filter({ 'data_url' : url})
		return self.convert_results(results)

class Grin(db.Document):
	data_url = db.StringField()
	center_name = db.StringField()
	image_reference = db.StringField()
	date_stamp = db.StringField()
	short_description = db.StringField()
	full_description = db.StringField()
	keyword_list = db.ListField(db.StringField())
	subject_list = db.ListField(db.StringField())
	center_code = db.StringField()
	grin_id = db.StringField()
	image_creator = db.StringField()
	original_source = db.StringField()
	thumbnail_url = db.StringField()
	thumbnail_type = db.StringField()
	thumbnail_width = db.StringField()
	thumbnail_height = db.StringField()
	thumbnail_size = db.StringField()
	small_url = db.StringField()
	small_type = db.StringField()
	small_width = db.StringField()
	small_height = db.StringField()
	small_size = db.StringField()
	medium_url = db.StringField()
	medium_type = db.StringField()
	medium_width = db.StringField()
	medium_height = db.StringField()
	medium_size = db.StringField()
	large_url = db.StringField()
	large_type = db.StringField()
	large_width = db.StringField()
	large_height = db.StringField()
	large_size = db.StringField()
	
	query_class = DatasetQuery


def find_all(string, occurrence):
	found = 0

	while True:
		found = string.find(occurrence, found)
		if found != -1:
			yield found
		else:
			break

		found += 1

def clean_string_list(stringList):
	for entry in stringList:
		yield entry.strip('\n\r ,\t')
	
	
def extract_section(page, start, end):
	startPos = page.find(start)
	startPos = startPos + len(start)
	endPos = page.find(end, startPos)
	return page[startPos:endPos].strip()

def extract_with_start_padding(page, start, end, startPadding, maxSearchLength = 200):
	startPos = page.find(start)
	startPos = startPos + startPadding
	remaining = page[startPos:startPos+maxSearchLength]
	endPos = remaining.find(end)
	return remaining[0:endPos].strip()
	
def get_pages():
	center_list = ['AMES','DFRC','GRC','GSFC','HQ','JPL','JSC','KSC','LARC','MSFC','SSC']
	for center in center_list:
		html = lxml.html.parse('http://grin.hq.nasa.gov/BROWSE/' + center + '.html')
		page = lxml.html.tostring(html, pretty_print=True, method="html")
			
		first_tag = list(find_all(page, '<a href="/ABSTRACTS/'))
		second_tag = list(find_all(page, '" accesskey="z"'))
		
		count = 0
		while count < len(first_tag):
			url = 'http://grin.hq.nasa.gov/ABSTRACTS/' + page[first_tag[count]+20:second_tag[count]]  
			#check if we have already loaded this page.    
			existing = Grin.query.filter_by_url(url)
			if existing == None :  
				get_a_page(url)
			count = count + 1
		
	return ''


def get_a_page(url):
	html = ""
	try :
		html = lxml.html.parse(url)
	except IOError: 
		print "page could not be found " + url
		return 
	
	page = lxml.html.tostring(html, pretty_print=True, method="html")


	centerName = extract_with_start_padding(page, 'NASA Center:', '</td>', 47)

	imageRef = extract_with_start_padding(page, ' : </font></th>', '</td>', 37, 50)

	startPos = page.find('DA:')
	dateTimestamp = page[startPos+3:startPos+10]

	shortDescription = extract_section(page, '<!-- ONE-LINE-DESCRIPTION-BEGIN -->', '<!-- ONE-LINE-DESCRIPTION-END -->')

	description = extract_section(page, '<!-- DESCRIPTION-BEGIN -->', '<!-- DESCRIPTION-END -->')

	keywords = extract_section(page, '<!-- KEYWORD-BEGIN -->', '<!-- KEYWORD-END -->')
	keywordList = keywords.split()

	subjects = extract_section(page, '<!-- SUBJECT-BEGIN -->', '<!-- SUBJECT-END -->').strip(',')
	subjectList = list(clean_string_list(subjects.split(',')))

	centerCode = extract_section(page, '<!-- CENTER-BEGIN -->', '<!-- CENTER-END -->')

	grinID = extract_section(page, '<!-- GRINNUMBER-BEGIN -->', '<!-- GRINNUMBER-END -->')
	
	creator = extract_with_start_padding(page, 'Creator/Photographer:', '</li>', 25)

	origSource = extract_with_start_padding(page, 'Original Source:', '</li>', 20, 60)

	# have to deal with image urls outside of function so we have early bail out.
	startPos = page.find('<td id="r2" headers="c1"><a href="')
	if startPos >= 0 :
		remaining = page[startPos+34:startPos+200]
		endPos = remaining.find('">')
		thumbnailUrl = remaining[0:endPos]
	
		thumbnailType = extract_with_start_padding(page, 'headers="c2 r2"', '</td>', 31, 60)
	
		thumbnailWidth = extract_with_start_padding(page, 'headers="c3 r2"', '</td>', 31, 60)
	
		thumbnailHeight = extract_with_start_padding(page, 'headers="c4 r2', '</td>', 31, 60)
	
		thumbnailSize = extract_with_start_padding(page, 'headers="c5 r2"', '</td>', 30, 60)
	else :
		thumbnailUrl = ''
		thumbnailType = 'None'
		thumbnailWidth = '0'
		thumbnailHeight = '0'
		thumbnailSize = '0'


	startPos = page.find('<td id="r3" headers="c1"><a href="')
	if startPos >= 0 :
		remaining = page[startPos+34:startPos+200]
		endPos = remaining.find('">')
		smallUrl = remaining[0:endPos]
	
		smallType = extract_with_start_padding(page, 'headers="c2 r3"', '</td>', 31, 60)
	
		smallWidth = extract_with_start_padding(page, 'headers="c3 r3"', '</td>', 31, 60)
	
		smallHeight = extract_with_start_padding(page, 'headers="c4 r3"', '</td>', 31, 60)
	
		smallSize = extract_with_start_padding(page, 'headers="c5 r3"', '</td>', 30, 60)
	else :
		smallUrl = ''
		smallType = 'None'
		smallWidth = '0'
		smallHeight = '0'
		smallSize = '0'


	startPos = page.find('<td headers="c1" id="r4"><a href="')
	if startPos >= 0 :
		remaining = page[startPos+34:startPos+200]
		restOfRow3 = page[startPos+31:startPos+5000]
		endPos = remaining.find('">')
		
		mediumUrl = remaining[0:endPos]
	
	
		mediumType = extract_with_start_padding(restOfRow3, 'headers="c2 r3"', '</td>', 31, 60)
	
	
		mediumWidth = extract_with_start_padding(restOfRow3, 'headers="c3 r3"', '</td>', 31, 60)
	
		mediumHeight = extract_with_start_padding(restOfRow3, 'headers="c4 r3"', '</td>', 31, 60)
	
		mediumSize = extract_with_start_padding(restOfRow3, 'headers="c5 r3"', '</td>', 30, 60)
	else :
		mediumUrl = ''
		mediumType = 'None'
		mediumWidth = '0'
		mediumHeight = '0'
		mediumSize = '0'

	startPos = page.find('<td id="r5" headers="c1"><a href="')
	if startPos >= 0 :
		remaining = page[startPos+34:startPos+200]
		restOfRow4 = page[startPos+31:startPos+5000]
		endPos = remaining.find('">')
		largeUrl = remaining[0:endPos]
	
		largeType = extract_with_start_padding(restOfRow4, 'headers="c2 r3"', '</td>', 31, 60)
	
		largeWidth = extract_with_start_padding(restOfRow4, 'headers="c3 r3"', '</td>', 31, 60)
	
		largeHeight = extract_with_start_padding(restOfRow4, 'headers="c4 r3"', '</td>', 31, 60)
		
		largeSize = extract_with_start_padding(restOfRow4, 'headers="c5 r3"', '</td>', 30, 60)
	else :
		largeUrl = ''
		largeType = 'None'
		largeWidth = '0'
		largeHeight = '0'
		largeSize = '0'

	dataset = Grin(data_url = url, center_name = centerName, image_reference = imageRef, date_stamp = dateTimestamp, short_description = shortDescription, full_description = description, keyword_list = keywordList, subject_list = subjectList, center_code = centerCode, grin_id = grinID, image_creator = creator, original_source = origSource, thumbnail_url = thumbnailUrl, thumbnail_type = thumbnailType, thumbnail_width = thumbnailWidth, thumbnail_height = thumbnailHeight, thumbnail_size = thumbnailSize, small_url = smallUrl, small_type = smallType, small_width = smallWidth, small_height = smallHeight, small_size = smallSize, medium_url = mediumUrl, medium_type = mediumType, medium_width = mediumWidth, medium_height = mediumHeight, medium_size = mediumSize, large_url = largeUrl, large_type = largeType, large_width = largeWidth, large_height = largeHeight, large_size = largeSize)
	dataset.save();
