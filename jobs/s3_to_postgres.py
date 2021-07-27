import psycopg2
import yaml
import time
import boto3
import io
import sys

from jinjasql import JinjaSql
from yaml.loader import SafeLoader
from datetime import datetime, date, timedelta
from smart_open import smart_open

class CopyFromS3ToPostgres():

	def __init__(self,
		postgres_host,
		postgres_port,
		postgres_database,
		postgres_user,
		postgres_password,
		configuration_file
	):

		self.postgres_host = postgres_host
		self.postgres_port = postgres_port
		self.postgres_database = postgres_database
		self.postgres_user = postgres_user
		self.postgres_password = postgres_password
		self.configuration_file = configuration_file
		self.postgres_connection = None
		self.sql_to_execute = ''

		self.table_configuration = {}
		self.query_params = {}
		self.output_info_file = {}
		self.data_count = 0
		self.data_count_expected = 0

		self.s3_bucket = ''
		self.source_database_name = ''
		self.s3_output_querys = ''

		self.region_name = 'us-east-1'
		self.s3_resource = boto3.resource('s3', region_name=self.region_name)
		self.athena_client = boto3.client('athena', region_name=self.region_name)

	def reset_data_count(self):
		self.data_count = 0
		self.data_count_expected = 0

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

				self.s3_bucket = self.table_configuration[key]["s3_artifacts_bucket"]
				self.source_database_name = self.table_configuration[key]["source_database_name"]
				self.s3_output_querys = f's3://{self.s3_bucket}/{self.table_configuration[key]["s3_output_querys"]}'

				#print(f'Table configuration: {self.table_configuration}')
		except Exception as e:
			print(str(e))

	def create_postgres_conection(self):
		try:
			self.postgres_connection = psycopg2.connect(
				host=self.postgres_host, 
				port=self.postgres_port,
				database=self.postgres_database, 
				user=self.postgres_user,
				password=self.postgres_password
			)

			print(f'{datetime.now()} - Postgres connection made successfully!\n')
		except Exception as e:
			print(str(e))

	def execute_query_athena(self, table):
		#try:
		with open(self.table_configuration[table]["sql_file"]) as f:
			query_with_params = f.read()

			j = JinjaSql(param_style='pyformat')
			query, bind_params = j.prepare_query(query_with_params, self.query_params)
			self.sql_to_execute = (query % bind_params)

		response = self.athena_client.start_query_execution(
			QueryString = (self.sql_to_execute),
			QueryExecutionContext={
				'Database': self.source_database_name
			},
			ResultConfiguration={
				'OutputLocation': self.s3_output_querys + '/' + self.source_database_name,
			}
		)
		self.table_configuration[table]["query_response_filename"] = response['QueryExecutionId']
		print(f'{datetime.now()} - Execution ID: ' + self.table_configuration[table]["query_response_filename"])
			
		query_status = None
		while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
			query_status = self.athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])['QueryExecution']['Status']['State']
			#print(f'Query status: {query_status}')
			#print('...')
			if query_status == 'FAILED' or query_status == 'CANCELLED':
				raise Exception('Athena query with the string \n"{}"\n failed or was cancelled'.format(self.sql_to_execute))
			time.sleep(2)
		print(f'{datetime.now()} - Query for {table} finished.')

		#except Exception as e:
		#	print(str(e))

	def delete_data_from_table(self, table, field):
		date_low = datetime.strptime(self.table_configuration[table]["start_date"], '%d-%m-%Y').date()
		date_high = datetime.strptime(self.table_configuration[table]["end_date"], '%d-%m-%Y').date()

		sql_delete = f'delete from \
		{self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]} \
		where {field} between \'{date_low}\' and \'{date_high}\';'

		try:
			cur = self.postgres_connection.cursor()

			cur.execute(sql_delete)

			cur.close()
			self.postgres_connection.commit()

			print(f'{datetime.now()} - Deleted data from table {self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]}')
		except Exception as e:
			print(str(e))

	def create_homologation_table_postgres(self, table):

		sql_drop = f'drop table if exists \
		{self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]}_homolog;'
		sql_create = f'create table if not exists \
		{self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]}_homolog \
		as (select * from {self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]}) \
		with no data;'

		try:
			cur = self.postgres_connection.cursor()

			cur.execute(sql_drop)
			self.postgres_connection.commit()
			cur.execute(sql_create)

			cur.close()
			self.postgres_connection.commit()

			print(f'{datetime.now()} - Created table {self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]}_homolog')
		except Exception as e:
			print(str(e))

	def copy_data_to_postgres(self, table):
		sql_copy = f'copy {self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]} ' + \
		'''FROM STDIN WITH CSV HEADER NULL AS '';'''

		bucket_url = f'{self.s3_output_querys}/{self.table_configuration[table]["query_response_filename"]}.csv'

		try:
			cur = self.postgres_connection.cursor()

			f = smart_open(bucket_url, 'rb', encoding='utf-8')
			cur.copy_expert(sql_copy, f)

			self.postgres_connection.commit()
			cur.close()

			print(f'{datetime.now()} - Copied {self.s3_output_querys} to postgres table {self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]}')
		except Exception as e:
			print(str(e))

	def create_output_info_dict(self, table):
		self.output_info_file[self.table_configuration[table]["table_name"]] = {}
		self.output_info_file[self.table_configuration[table]["table_name"]]['execution_date'] = date.today()
		self.output_info_file[self.table_configuration[table]["table_name"]]['logs'] = []

	def increase_output_info_dict(self, table, days_to_reprocessing):
		reference_date = date.today() + timedelta(days=days_to_reprocessing)
		self.output_info_file[self.table_configuration[table]["table_name"]]['logs'].append(reference_date) 

	def create_out_file(self, table):
		filename = f'out/{self.table_configuration[table]["table_name"]}.yaml'
		f = open(filename, "w")
		f.close

	def input_out_file(self, table):
		filename = f'out/{self.table_configuration[table]["table_name"]}.yaml'

		with open(filename, "w") as yaml_file:
			yaml.dump(self.output_info_file, yaml_file, default_flow_style=False)

	def get_column_names(self, filename):
		self.s3_resource = boto3.resource('s3', region_name=self.region_name)
		response = self.s3_resource.Bucket(self.s3_bucket) \
					.Object(key='source-querys/' + filename + '.csv') \
					.get()
		file_content = io.BytesIO(response['Body'].read())
		line_count = 0
		for row in file_content:
			if line_count == 0:
				column_names = str(row).replace('b\'\"', '').replace('\"\\n\'', '').split('\",\"')
				line_count += 1
			else:
				break

		self.data_count_expected = line_count

		return tuple(column_names)

	def get_column_values(self, filename):
		self.s3_resource = boto3.resource('s3', region_name=self.region_name)
		response = self.s3_resource.Bucket(self.s3_bucket) \
					.Object(key='source-querys/' + filename + '.csv') \
					.get()
		file_content = io.BytesIO(response['Body'].read())

		column_list_values = []

		line_count = 0
		for row in file_content:
			if line_count != 0:
				column_values = str(row).replace('b\'\"', '').replace('\"\\n\'', '').split('\",\"')
				column_list_values.append(column_values)
			else:
				line_count += 1

		return tuple(column_list_values)

	def insert_data_to_postgres(self, column_names, column_values, postgres_cursor, table):
		columns = (f'{column_names}').replace('\'', '\"')

		sql_query = (f'insert into {self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]}_homolog \
		{columns} values{column_values}').lower()

		try:
			postgres_cursor.execute(sql_query)
			self.postgres_connection.commit()
			self.data_count += 1
		except Exception as e:
			print(str(e))

	def copy_data_to_postgres_with_insert(self, filename, table):
		column_names = self.get_column_names(filename)
		column_list_values = self.get_column_values(filename)

		cur = self.postgres_connection.cursor()
		
		self.create_out_file(table)
		self.input_out_file(table)

		try:
			for column_values in column_list_values:
				self.insert_data_to_postgres(column_names, column_values, cur)

		except Exception as e:
			print(str(e))

		print(f'{datetime.now()} - Insert {self.data_count} rows into postgres table {self.table_configuration[table]["schema"]}.{self.table_configuration[table]["table_name"]}_homolog')
		cur.close()

	def reprocessing_table(self, table):
		end_date = datetime.strptime(self.table_configuration[table]["end_date"], '%d-%m-%Y').date()
		start_count = 0

		self.create_out_file(table)
		self.create_output_info_dict(table)

		while (start_count >= self.table_configuration[table]["days_to_reprocessing"]):
			reference_date = end_date + timedelta(start_count)
			date_diff = (reference_date - date.today()).days

			self.query_params["days_gone"] = date_diff
			print(f'{datetime.now()} - days gone: {self.query_params["days_gone"]}')

			print(f'{datetime.now()} - reference data: {reference_date} | days gone: {date_diff} | start_count: {start_count} | days_to_reprocessing: {self.table_configuration[table]["days_to_reprocessing"]}')

			self.execute_query_athena(table)
			self.copy_data_to_postgres(table)
			self.increase_output_info_dict(table, date_diff)

			start_count -= 1

		self.input_out_file(table)

	def run_reprocessing_for_all_tables(self):
		for key in self.table_configuration.keys():
			if 's3_to_postgres.py' in self.table_configuration[key]["script_name"]:
				self.create_postgres_conection()
				self.delete_data_from_table(key, self.table_configuration[key]["data_field"])
				self.reprocessing_table(key)

if __name__ == "__main__":
	copy_from_s3_to_postgres = CopyFromS3ToPostgres(
		sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], 'ymls/tables_to_reprocessing.yaml'
	)
	copy_from_s3_to_postgres.create_dictionary_for_table()
	copy_from_s3_to_postgres.run_reprocessing_for_all_tables()