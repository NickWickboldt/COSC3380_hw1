import sys
import os
import psycopg2

first_log = True

#Run below to create tables necessary for testing tc1.txt

# with open("tc1.sql", 'r') as sql_file:
#   sql_commands = sql_file.read()

# cursor.execute(sql_commands)
# conn.commit()

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
	conn = psycopg2.connect(host = "localhost", dbname="python_test", user = "postgres", password=passwd)
	cursor = conn.cursor()
     
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
				column_names.append(column)
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
);"""
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
	AND column_name = %s
;"""

	for tbl in table_cols:
		columns = table_columns[tbl]
		for i in range(len(columns)):
			if len(columns[i]) > 1:
				for j in range(len(columns[i])):
					col = columns[i]
					if col[j] == 'p' or col[j] == 'f':
						col = col[:j]
						columns[i] = col
						break
		# print(tbl)
		# print(columns)
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

if __name__ == "__main__":
	input_file = get_input_file()
	connection()
	content = ""
	table_names = []
	lines = []
	table_columns = {}
	#Table extraction
	with open(input_file, 'r') as file:
		for line in file:
			content += line
			lines.append(line)
			tbl = get_table_names(line)
			table_names.append(tbl)
			if not validate_table(tbl):
				sys.exit(f"Given table {tbl} does not exist in current schema.")

	print("All given tables exist...")
	#Table validity confirmed, moving onto column extraction
	for i in range(len(lines)):
		table_columns[table_names[i]] = get_column_names(lines[i], table_names[i])

	#Validating Columns
	if not validate_columns(table_columns):
		sys.exit(f"Provided columns do not exist.")
    
	print("All given columns exist...")
    # print(f"Using file: {input_file}")
    



# This here for referential integrity
# 	sql_query = """SELECT COUNT(*) FROM T1
# LEFT JOIN T2 ON T1.k2 = T2.k2;"""
# 	cursor.execute(sql_query)
# 	log_sql_commands(sql_query)
# 	result = cursor.fetchone()
# 	# print(f"Count result: {result[0]}")