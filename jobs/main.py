import yaml
import os
import subprocess

from yaml.loader import SafeLoader
from datetime import datetime, date

class Reprocessing():
	def __init__(self):
		self.table_configuration = {}

	def create_dictionary_for_table(self):
		try:
			with open('ymls/tables_to_reprocessing.yaml') as f:
				self.table_configuration = yaml.load(f, Loader=SafeLoader)

			for key in self.table_configuration.keys():

				self.table_configuration[key]["days_to_reprocessing"] = (
					datetime.strptime(
						self.table_configuration[key]["start_date"], '%d-%m-%Y'
					).date() - datetime.strptime(
						self.table_configuration[key]["end_date"], '%d-%m-%Y'
					).date()
				).days

		except Exception as e:
			print(str(e))

	def execute(self):
		for key in self.table_configuration.keys():

			script_name = ''

			if 's3_to_postgres.py' in self.table_configuration[key]["script_name"]:
				script_name = f'python {self.table_configuration[key]["script_name"]} {self.table_configuration[key]["rds_host"]} \
				{self.table_configuration[key]["rds_port"]} {self.table_configuration[key]["rds_database"]} \
				{self.table_configuration[key]["rds_user"]} {self.table_configuration[key]["rds_password"]}'
			elif 's3_to_s3.py' in self.table_configuration[key]["script_name"]:
				script_name = f'python {self.table_configuration[key]["script_name"]} {self.table_configuration[key]["source_database_name"]} \
				{self.table_configuration[key]["target_database_name"]} {self.table_configuration[key]["table_bucket"]} \
				{self.table_configuration[key]["table_name"]}'

			p = subprocess.Popen(script_name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			for line in p.stdout.readlines():
				print(line)
			retval = p.wait()
			#print(arch)
			#os.system(f'python {self.table_configuration[key]["script_name"]}')

if __name__ == "__main__":
	reprocessing = Reprocessing()
	reprocessing.create_dictionary_for_table()
	reprocessing.execute()
