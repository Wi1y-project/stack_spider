import xlwt
import xlrd
import os
import logging
import json
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Item(object):
	field_coll = {}

	def __init__(self, result_object):

		self.field_coll['filename'] = ''
		self.field_coll['excelname'] = ''
		self.field_coll['serial_number'] = ''
		self.field_coll['editor'] = ''
		self.field_coll['error_report'] = ''
		self.field_coll['publisher'] = ''
		self.field_coll['issn'] = ''
		self.field_coll['eissn'] = ''
		self.field_coll['journal_title'] = ''
		self.field_coll['originaljid'] = ''
		self.field_coll['jid'] = ''
		self.field_coll['year'] = ''
		self.field_coll['volume'] = ''
		self.field_coll['issue'] = ''
		self.field_coll['attach'] = ''
		self.field_coll['issue_total'] = ''
		self.field_coll['issue_title'] = ''
		self.field_coll['string_cover_date'] = ''
		self.field_coll['issue_history'] = ''
		self.field_coll['article_id'] = ''
		self.field_coll['doi'] = ''
		self.field_coll['article_seq'] = ''
		self.field_coll['elocation_id'] = ''
		self.field_coll['article_type'] = ''
		self.field_coll['title'] = ''
		self.field_coll['subtitle'] = ''
		self.field_coll['trans_title'] = ''
		self.field_coll['trans_subtitle'] = ''
		self.field_coll['language'] = ''
		self.field_coll['language_alternatives'] = ''
		self.field_coll['abstract'] = ''
		self.field_coll['trans_abstract'] = ''
		self.field_coll['keyword'] = ''
		self.field_coll['trans_keyword'] = ''
		self.field_coll['keyword_alternatives'] = ''
		self.field_coll['subject'] = ''
		self.field_coll['classification'] = ''
		self.field_coll['start_page'] = ''
		self.field_coll['end_page'] = ''
		self.field_coll['page_total'] = ''
		self.field_coll['range_page'] = ''
		self.field_coll['string_pub_date'] = ''
		self.field_coll['received_date'] = ''
		self.field_coll['revised_date'] = ''
		self.field_coll['accepted_date'] = ''
		self.field_coll['online_date'] = ''
		self.field_coll['copyright_statement'] = ''
		self.field_coll['copyright_year'] = ''
		self.field_coll['copyright_holder'] = ''
		self.field_coll['license'] = ''
		self.field_coll['reference'] = ''
		self.field_coll['abs_url'] = ''
		self.field_coll['pageurl'] = ''
		self.field_coll['fulltext_url'] = ''
		self.field_coll['fulltext_pdf'] = ''
		self.field_coll['corresponding'] = ''
		self.field_coll['article_note'] = ''
		self.field_coll['awards'] = ''
		self.field_coll['author_name'] = ''
		self.field_coll['name_alternatives'] = ''
		self.field_coll['collab'] = ''
		self.field_coll['email'] = ''
		self.field_coll['affiliation'] = ''
		self.field_coll['aff_alternatives'] = ''
		self.field_coll['aff_address'] = ''
		self.field_coll['contrib_address'] = ''
		self.field_coll['bio'] = ''

		self.result_object = result_object
		self.xls_name = 'wc_wl_' + self.result_object.name + '_' + self.result_object.result_date + '_' + self.result_object.result_date + '.xls'
		self.arr_title = []
		self.old_sheet = None
		self.new_sheet = None
		self.txt_name = self.result_object.name + '_temp.txt'
		self.row = 0

		if not os.path.exists(self.xls_name):
			logger.info('makedir ' + self.xls_name)
			file = xlwt.Workbook(encoding='utf-8')
			sheet = file.add_sheet(u'sheet1', cell_overwrite_ok=True)

			for fie in self.field_coll:
				self.arr_title.append(fie)

			for col in range(len(self.arr_title)):
				sheet.write(0, col, self.arr_title[col])
			file.save(self.xls_name)

	def write_temp(self):

		with open(self.txt_name, 'a+', encoding='utf8') as f:
			f.write(json.dumps(self.field_coll) + '\n')

	def write_all(self):

		old_book = xlrd.open_workbook(self.xls_name)
		old_sheet = old_book.sheet_by_index(0)

		new_book = xlwt.Workbook(encoding='utf-8')
		new_sheet = new_book.add_sheet(u'sheet1', cell_overwrite_ok=True)

		rows = old_sheet.nrows

		for old_row in range(rows):
			for count, row_value in enumerate(old_sheet.row_values(old_row)):
				new_sheet.write(old_row, count, row_value)
		logger.info('update to local file...')

		with open(self.txt_name, 'r+', encoding='utf8') as f:
			for line in f:

				line = line.replace('\n', '')
				j = json.loads(line)

				new_sheet.write(rows + self.row, 0, j['filename'])
				new_sheet.write(rows + self.row, 1, self.xls_name.replace('.xls', ''))
				new_sheet.write(rows + self.row, 5, j['publisher'])
				new_sheet.write(rows + self.row, 6, j['issn'])
				new_sheet.write(rows + self.row, 7, j['eissn'])
				new_sheet.write(rows + self.row, 8, j['journal_title'])
				new_sheet.write(rows + self.row, 11, j['year'])
				new_sheet.write(rows + self.row, 12, j['volume'])
				new_sheet.write(rows + self.row, 13, j['issue'])
				new_sheet.write(rows + self.row, 17, j['string_cover_date'])
				new_sheet.write(rows + self.row, 19, j['article_id'])
				new_sheet.write(rows + self.row, 20, j['doi'])
				new_sheet.write(rows + self.row, 23, j['article_type'])
				new_sheet.write(rows + self.row, 24, BeautifulSoup(j['title'], 'html.parser').get_text())
				new_sheet.write(rows + self.row, 28, j['language'])
				new_sheet.write(rows + self.row, 30, j['abstract'])
				new_sheet.write(rows + self.row, 32, j['keyword'])
				new_sheet.write(rows + self.row, 37, j['start_page'])
				new_sheet.write(rows + self.row, 38, j['end_page'])
				new_sheet.write(rows + self.row, 39, j['page_total'])
				new_sheet.write(rows + self.row, 41, j['string_pub_date'])
				new_sheet.write(rows + self.row, 42, j['received_date'])
				new_sheet.write(rows + self.row, 43, j['revised_date'])
				new_sheet.write(rows + self.row, 44, j['accepted_date'])
				new_sheet.write(rows + self.row, 45, j['online_date'])
				new_sheet.write(rows + self.row, 46, j['copyright_statement'])
				new_sheet.write(rows + self.row, 47, j['copyright_year'])
				new_sheet.write(rows + self.row, 48, j['copyright_holder'])
				new_sheet.write(rows + self.row, 51, j['abs_url'])
				new_sheet.write(rows + self.row, 52, j['abs_url'])
				new_sheet.write(rows + self.row, 53, j['fulltext_url'])
				new_sheet.write(rows + self.row, 54, j['fulltext_pdf'])
				new_sheet.write(rows + self.row, 55, j['corresponding'])
				new_sheet.write(rows + self.row, 56, j['article_note'])
				new_sheet.write(rows + self.row, 58, j['author_name'])
				new_sheet.write(rows + self.row, 61, j['email'])
				new_sheet.write(rows + self.row, 62, j['affiliation'])
				new_sheet.write(rows + self.row, 64, j['aff_address'])
				new_sheet.write(rows + self.row, 65, j['contrib_address'])
				new_sheet.write(rows + self.row, 66, j['bio'])

				self.row += 1

		new_book.save(self.xls_name)








