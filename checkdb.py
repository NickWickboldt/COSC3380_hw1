import sys
import os
import psycopg2
import copy

first_log = True

def log_sql_commands(sql_command, param = (None,)):
	param_ticker = 0
	#Replaces dynamic variables
	for i in range(len(sql_command)):
		if sql_command[i] == '%' and sql_command[i + 1] == 's':
			sql_command = sql_command[:i] + '\'' + param[param_ticker] + '\'' + sql_command[(i+2):]
			param_ticker+=1

	#First log recreates log file, subsequent logs append to file
	global first_log
	if first_log:
		with open('sql_log.sql', 'w') as file:
			file.write(sql_command + '\n') 
		first_log = False
	else:
		with open('sql_log.sql', 'a') as file:
  			file.write(sql_command + '\n') 

def connection():
	global cursor
	global conn
	print("Enter your postgres password...")
	passwd = input()
	conn = psycopg2.connect(host = "127.0.0.1", dbname="python_test", user = "postgres", password=passwd)
	cursor = conn.cursor()

	# with open("tc2.sql", 'r') as sql_file:
	# 	sql_commands = sql_file.read()
	# try: 
	# 	cursor.execute(sql_commands)
	# except psycopg2.DatabaseError as e: 
	# 	print("Invalid PostgreSQL detected: ", e)
	# 	sys.exit()

	conn.commit()
     
def close_connection():
	cursor.close()
	conn.close()

def get_input_file():
    if len(sys.argv) != 2:
        print("Usage: python3 checkdb.py database=<filename>.txt")
        sys.exit(1)

    arg = sys.argv[1]
    if not arg.startswith("database="):
        print("Invalid argument. Use the format: database=<filename>.txt")
        sys.exit(1)

    file_name = arg.split("=", 1)[1]

    if not file_name.endswith(".txt"):
        print(f"Error: {file_name} is not a .txt file.")
        sys.exit(1)
    
    if not os.path.exists(file_name):
        print(f"Error: File '{file_name}' does not exist.")
        sys.exit(1)

    return file_name

def get_table_names(input_line):
	#Extracts table names from input lines, used for table validation
	table_name = ""
	for i in range(len(input_line)):
		if input_line[i] != '(':
			table_name+=input_line[i]
		else:
			return table_name

def get_column_names(input_line, tbl_name):
	column_names = []
	column = ""
	for i in range(len(input_line)):
		if input_line[i] != ')' and input_line[i] != ',':
			if input_line[i] != '(':
				column += input_line[i]
			if column == tbl_name:
				column = ""
		else:
			if column != tbl_name and column != '':
				column_names.append(column.strip())
				column = ""
	return column_names

def validate_table(table_name):
	#Validates the existence of given tables
	table_name = table_name.lower()
	sql = """SELECT EXISTS (
	SELECT 1
	FROM information_schema.tables
	WHERE table_schema = 'public'
	AND table_name = %s
);
"""
	cursor.execute(sql, (table_name,))
	log_sql_commands(sql, (table_name,))

	return cursor.fetchone()[0]

def validate_columns(table_cols):
	#Validates the existence of given columns within given tables
	sql = """SELECT COUNT(*)
	FROM information_schema.columns
	WHERE table_catalog = 'python_test'
	AND table_schema = 'public'
	AND table_name = %s
	AND column_name = %s;
"""
	for tbl in table_cols:
		columns = table_cols[tbl]
		for i in range(len(columns)):
			if len(columns[i]) > 1:
				for j in range(len(columns[i])):
					col = columns[i]
					if col[j] == 'p' or col[j] == 'f' or col[j] == 'P' or col[j] == 'F':
						col = col[:j]
						columns[i] = col
						break
		# print(tbl)
		cursor_fetches = []
		for i in range(len(columns)):
			# print(columns[i])
			cursor.execute(sql, (tbl.lower(),columns[i].lower()))
			log_sql_commands(sql, (tbl.lower(), columns[i].lower()))
			cursor_fetches.append(cursor.fetchone())
			# print(cursor_fetches)

	for i in range(len(cursor_fetches)):
		if cursor_fetches[i][0] == 0:
			return False
	return True

def get_keys_from_dbms(table_names, keys_dict):
	#Extracts the primary key of a given table
	sql_get_primary = """SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
	ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = %s;
"""
	#Extracts the foreign key(s) of a given table
	sql_get_foreign = """SELECT
  kcu.column_name AS fk_column,
  ccu.table_name AS referenced_table,
  ccu.column_name AS referenced_column
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_name = %s;
"""
	for i in range(len(table_names)):
		key_list = []
		
		cursor.execute(sql_get_primary, (table_names[i].lower(), ))
		log_sql_commands(sql_get_primary, (table_names[i].lower(), ))
		try: 
			key_list.append(cursor.fetchall()[0][0])
		except: 
			continue

		cursor.execute(sql_get_foreign, (table_names[i].lower(), ))
		log_sql_commands(sql_get_foreign, (table_names[i].lower(), ))
		returned = cursor.fetchall()
		if len(returned) > 0:
			key_list.append(returned[0])

		keys_dict[table_names[i]] = key_list
	return keys_dict

def referential_integrity(key_list):
	integrity_list = []
	for table in key_list:
		if len(key_list[table]) > 1:
			# cursor.execute(sql_query, (table.lower(), key_list[table][1][1], table.lower(), key_list[table][1][0], key_list[table][1][1], key_list[table][1][2]))  
			child_table = table.lower()
			child_column = key_list[table][1][0]
			parent_column = key_list[table][1][2]
			table = key_list[table][1][1]

			sql_query = f"""SELECT COUNT(*)
FROM {child_table} AS c
LEFT JOIN {table} AS p
ON c.{child_column} = p.{parent_column}
WHERE p.{parent_column} IS NULL;
"""
			cursor.execute(sql_query)
			log_sql_commands(sql_query, (child_table, table, child_column,parent_column, parent_column))
			if(cursor.fetchone()[0] == 0):
				integrity_list.append('Y')
			else:
				integrity_list.append('N')
		else:
			integrity_list.append('Y')

	return integrity_list

def get_keys_from_input(columns_keys):
	keys = {}
	for tbl in columns_keys:
		for i in range(len(columns_and_keys[tbl])):
			sequence = columns_and_keys[tbl][i]
			if 'pk' in sequence:
				new_sequence = sequence.replace('pk', '')
				keys[tbl] = [new_sequence]
			elif 'PK' in sequence:
				new_sequence = sequence.replace('PK', '')
				keys[tbl] = [new_sequence]
			elif 'fk' in sequence:
				new_sequence = sequence.replace('fk', '')
				new_sequence = new_sequence.split(':')
				new_sequence[1] = new_sequence[1].split('.')
				key_tuple = (new_sequence[0], new_sequence[1][0], new_sequence[1][1])
				keys[tbl] += [key_tuple]
			# print(columns_and_keys[tbl][i])
	return keys

def get_non_primary_key_columns(columns_keys):
	non_key_dict = {}
	for tbl in columns_keys:
		non_keys = []
		for i in range(len(columns_keys[tbl])):
			sequence = columns_keys[tbl][i]
			if not 'pk' in sequence and not 'PK' in sequence:
				if 'fk' in sequence:
					index = sequence.index('fk')
					sequence = sequence[:index]
				elif 'FK' in sequence:
					index = sequence.index('FK')
					sequence = sequence[:index]
				non_keys.append(sequence)
		non_key_dict[tbl] = non_keys
	return non_key_dict

def check_3nf_bcnf(non_p_columns):
	n_list = []
	# print(non_p_columns)
	for tbl in non_p_columns:
		if len(non_p_columns[tbl]) > 1:
			results_list = []
			for i in range(len(non_p_columns[tbl])):
				column = non_p_columns[tbl][i]
				# if 'fk' in column:
				# 	index = column.index('fk')
				# 	column = column[:index]
				# elif 'FK' in column:
				# 	index = column.index('FK')
				# 	column = column[:index]
				for j in range(i + 1, len(non_p_columns[tbl])):
					second_column = non_p_columns[tbl][j]
					# if 'fk' in second_column or 'FK' in second_column:
					# 	continue
					if column != second_column:
						sql_query = f"""SELECT {column}, COUNT(DISTINCT {second_column})
FROM {tbl}
GROUP BY {column}
HAVING COUNT(DISTINCT {second_column}) > 1;
"""
						cursor.execute(sql_query)
						log_sql_commands(sql_query, (column, second_column, tbl, column, second_column))
						results_list.append(cursor.fetchone())
			if None in results_list or len(results_list) == 0:
				n_list.append('N')
			else:
				n_list.append('Y')
		else:
			n_list.append('Y')
		# for i in range(len(non_p_columns[tbl])):
	return n_list

def format_output(filename, tables, referential_list, normalization_list):
	filname_without_type = filename.replace('.txt', '')
	with open(f'{filname_without_type}_output.txt', 'w') as file:
		# Print filename
		file.write(f"{filename}\n")
		file.write("-" * 41)
		file.write("\n")
		# Print header
		file.write(f"{'Table':<10} {'referential':^15} {'normalized':^15}\n")
		file.write(f"{'':<10} {'integrity':^15} {'':<15}\n")
		file.write("-" * 41)
		file.write("\n")
		# Process table data
		for i in range(len(tables)):
			file.write(f"{tables[i]:<10} {referential_list[i]:^15} {normalization_list[i]:^15}\n")
		# Process DB summary
		db_ref_integrity = 'Y'
		for i in range(len(referential_list)):
			if referential_list[i] == 'N':
				db_ref_integrity = 'N'

		db_normalized = 'Y'
		for i in range(len(normalization_list)):
			if normalization_list[i] == 'N':
				db_normalized = 'N'
		file.write("-" * 41)
		file.write("\n")
		file.write(f"{'DB referential integrity:':<30} {db_ref_integrity}\n")
		file.write(f"{'DB normalized:':<30} {db_normalized}\n")

if __name__ == "__main__":
	input_file = get_input_file()
	connection()
	content = ""
	table_names = []
	lines = []
	table_columns = {}
	dbms_keys = {}
	txt_file_keys = {}
	referential_integrity_list = []
	non_primary_key_columns = {}
	normalization_list = []
	#Table extraction
	with open(input_file, 'r') as file:
		for line in file:
			content += line
			lines.append(line)
			tbl = get_table_names(line)
			table_names.append(tbl)
			if not validate_table(tbl):
				sys.exit(f"Given table {tbl} does not exist in current schema.")

	# print("All given tables exist...")
	#Table validity confirmed, moving onto column extraction
	for i in range(len(lines)):
		table_columns[table_names[i]] = get_column_names(lines[i], table_names[i])

	columns_and_keys = copy.deepcopy(table_columns)
	non_primary_key_columns = get_non_primary_key_columns(columns_and_keys)

	#Validating Columns
	if not validate_columns(table_columns):
		sys.exit(f"Provided columns do not exist.")
	# print("All given columns exist...")
    # print(f"Using file: {input_file}")
	dbms_keys = get_keys_from_dbms(table_names, dbms_keys)
	txt_file_keys = get_keys_from_input(columns_and_keys)

	if dbms_keys != txt_file_keys:
		# print("Discrepencies detected between DBMS keys and text file keys.")
		# print("Assumming full key data from input text file.")
		referential_integrity_list = referential_integrity(txt_file_keys)
	else:
		# print("DBMS keys match input file keys.")
		# print("Using DBMS keys for key data.")
		referential_integrity_list = referential_integrity(dbms_keys)

	normalization_list = check_3nf_bcnf(non_primary_key_columns)

	# print("Referential Integrity: ",referential_integrity_list)
	# print("Normalization: ",normalization_list)
	format_output(input_file, table_names, referential_integrity_list, normalization_list)