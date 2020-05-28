import threading
import requests
import redis
import time
import logging
import random
import progressbar
import os
import faker
import hashlib
import stack_spider.item as items

fake = faker.Factory.create()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
redis_ = redis.Redis(host='10.3.1.12', port='6379', db=10)


def sha256hex(data):
	sha256 = hashlib.sha256()
	sha256.update(data.encode())
	res = sha256.hexdigest()
	return res


class UrlSpider(threading.Thread):

	start_url = ''

	def __init__(self, name, level, result, repeat):
		threading.Thread.__init__(self)
		self.name = name
		self.level = level
		self.result = result
		self.repeat = repeat
		self.level_name = self.name + '##' + self.level

	def left_push(self, url):
		redis_.lpush(self.level_name, url)

	def right_push(self, url):
		redis_.rpush(self.level_name, url)

	def left_pop(self):
		try:
			return str(redis_.lpop(self.level_name), encoding='utf8')
		except TypeError:
			return None

	def right_pop(self):
		try:
			return str(redis_.rpop(self.level_name), encoding='utf8')
		except TypeError:
			return None

	def get_length(self):
		return redis_.llen(self.level_name)

	def set_mark(self, parm):
		redis_.set(self.name + '-' + self.level, parm)

	def get_last_mark(self):
		return str(redis_.get(self.name + '-' + str(int(self.level) - 1)), encoding='utf8')

	def random_ip(self):
		ip_arr = redis_.lrange('ip', 0, -1)
		return str(ip_arr[random.randint(0, len(ip_arr) - 1)], encoding='utf8')

	def get_headers(self):

		headers = {
			'user-agent': fake.user_agent()
		}
		return headers

	def get_mark(self):
		return str(redis_.get(self.name + '-' + self.level), encoding='utf8')

	def get_all_list(self):
		for key in redis_.keys(self.name + "*"):

			key = str(key, encoding='utf8')

			if '#' in key:
				if redis_.llen(key) > 0:
					return True
		return False

	def remove_all_key(self):

		for key in redis_.keys(self.name):
			key = str(key, encoding='utf8')
			redis_.delete(key)

	def exit(self, url):

		if self.level == '1':

			if url is None:
				self.set_mark(1)
			else:
				self.set_mark(0)

		else:
			if url is None and self.get_last_mark() == '1':
				self.set_mark(1)
			else:
				self.set_mark(0)

	def get(self, url):
		return [], None

	def run(self):
		logger.info('urlSpider start! current level: ' + self.level)

		if self.get_all_list() is False:

			self.left_push(self.start_url)

		while True:

			url = self.left_pop()
			self.exit(url)

			if self.get_mark() == '1':

				logger.info(self.name + '-' + self.level + ' :exit!')
				break
			try:
				logger.info('current level: ' + self.level + '     current url: ' + url)
			except TypeError:
				logger.info('No Url To Request: Sleep-5 current level: ' + self.level)
				time.sleep(5)
				continue

			try:

				url_array, status = self.get(url)

				if status != 200:
					raise Exception("Invalid status!", status)

			except Exception as ex:
				logger.warning('error push! ' + str(ex))
				self.left_push(url)
				continue
			if self.result is False:

				if self.repeat is True:

					for item in url_array:
						if redis_.sadd(self.name + '_set', sha256hex(item)) == 1:

							redis_.lpush(self.name + '##' + str(int(self.level) + 1), item)
						else:
							logger.warning('Link repeat!' + item)
				else:

					for item in url_array:
						redis_.lpush(self.name + '##' + str(int(self.level) + 1), item)

			else:

				if self.repeat is True:

					for item in url_array:
						if redis_.sadd(self.name + '_set', sha256hex(item)) == 1:

							redis_.lpush(self.name + '##result', item)
						else:
							logger.warning('Link repeat!' + item)
				else:
					for item in url_array:
						redis_.lpush(self.name + '##result', item)


class ResultSpider(threading.Thread):

	result_date = time.strftime("%Y-%m-%d", time.localtime(time.time()))

	def __init__(self, name, level, last_level):
		threading.Thread.__init__(self)
		self.name = name
		self.level = level
		self.last_level = last_level
		self.level_name = self.name + '##' + self.level
		self.txt_name = self.name + '_temp.txt'

	def left_pop(self):
		try:
			return str(redis_.lpop(self.level_name), encoding='utf8')
		except TypeError:
			return None

	def random_ip(self):
		ip_arr = redis_.lrange('ip', 0, -1)
		return str(ip_arr[random.randint(0, len(ip_arr) - 1)], encoding='utf8')

	def get_headers(self):

		headers = {
			'user-agent': fake.user_agent()
		}
		return headers

	def left_push(self, url):
		redis_.lpush(self.level_name, url)

	def get(self, url):
		return {}, None

	def get_mark(self):
		return str(redis_.get(self.name + '-' + self.last_level), encoding='utf8')

	def wget_download(self, url, path):
		logger.info('wget download!')
		os.system('wget -o ' + path + ' -e "http_proxy=' + self.random_ip() + '" ' + url)

	def file_download(self, url, path):

		if not os.path.exists(self.name):
			os.mkdir(self.name)

		if os.path.exists(path):
			logger.info(path + 'pdf already exist!')
		else:
			try:
				size = 0
				r = requests.get(url, timeout=20, stream=True, headers=self.get_headers())# , proxies={'http': self.random_ip()}

				total_length = int(r.headers.get("Content-Length"))
				with open(path, 'wb') as f:
					widgets = ['Progress: ', progressbar.Percentage(), ' ', progressbar.Bar(marker='#', left='[', right=']'), ' ', progressbar.ETA(), ' ', progressbar.FileTransferSpeed()]
					pbar = progressbar.ProgressBar(widgets=widgets, maxval=total_length).start()
					for chunk in r.iter_content():
						if chunk:
							size += len(chunk)
							f.write(chunk)
						pbar.update(size)
					pbar.finish()
					f.flush()
			except Exception as ex:
				print(ex)
				os.remove(path)
				r = requests.get(url, timeout=20, headers=self.get_headers())#, proxies={'http': self.random_ip()}
				with open(path, 'wb') as f:
					f.write(r.content)

	def exit(self, url):
		if self.get_mark() == '1' and url is None:
			return True
		else:
			return False

	def remove_all_key(self):

		for key in redis_.keys(self.name + '-*'):
			key = str(key, encoding='utf8')
			redis_.delete(key)

	def run(self):
		it = items.Item(self)
		logger.info('resultSpider start!')
		while True:

			url = self.left_pop()

			if self.exit(url) is True:
				logger.info(self.name + '-' + self.level + ' :exit!')

				if os.path.exists(self.txt_name):
					it.write_all()

				self.remove_all_key()
				break

			if url is None:
				continue

			try:
				logger.info('current level: ' + self.level + ' current url: ' + url)

				try:

					result_coll, status = self.get(url)

					if status != 200:
						raise Exception("Invalid status!", status)

				except Exception as ex:
					logger.warning('error push! ' + str(ex))
					self.left_push(url)
					continue

				for field in result_coll:
					it.field_coll[field] = result_coll[field]

				it.write_temp()

			except TypeError:
				continue










