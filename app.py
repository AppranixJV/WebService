from flask import Flask, jsonify, request
import pyodbc
import psycopg2
import json
import sys
import os
import mysql.connector

initial_data = [
{"code": "AL", "name": "Alabama"},
{"code": "AK", "name": "Alaska"},
{"code": "AS", "name": "American Samoa"},
{"code": "AZ", "name": "Arizona"},
{"code": "AR", "name": "Arkansas"},
{"code": "CA", "name": "California"},
{"code": "CO", "name": "Colorado"},
{"code": "CT", "name": "Connecticut"},
{"code": "DE", "name": "Delaware"},
{"code": "DC", "name": "District of Columbia"},
{"code": "FM", "name": "Federated States of Micronesia"},
{"code": "FL", "name": "Florida"},
{"code": "GA", "name": "Georgia"},
{"code": "GU", "name": "Guam"},
{"code": "HI", "name": "Hawaii"},
{"code": "ID", "name": "Idaho"},
{"code": "IL", "name": "Illinois"},
{"code": "IN", "name": "Indiana"},
{"code": "IA", "name": "Iowa"},
{"code": "KS", "name": "Kansas"},
{"code": "KY", "name": "Kentucky"},
{"code": "LA", "name": "Louisiana"},
{"code": "ME", "name": "Maine"},
{"code": "MH", "name": "Marshall Islands"},
{"code": "MD", "name": "Maryland"},
{"code": "MA", "name": "Massachusetts"},
{"code": "MI", "name": "Michigan"},
{"code": "MN", "name": "Minnesota"},
{"code": "MS", "name": "Mississippi"},
{"code": "MO", "name": "Missouri"},
{"code": "MT", "name": "Montana"},
{"code": "NE", "name": "Nebraska"},
{"code": "NV", "name": "Nevada"},
{"code": "NH", "name": "New Hampshire"},
{"code": "NJ", "name": "New Jersey"},
{"code": "NM", "name": "New Mexico"},
{"code": "NY", "name": "New York"},
{"code": "NC", "name": "North Carolina"},
{"code": "ND", "name": "North Dakota"},
{"code": "MP", "name": "Northern Mariana Islands"},
{"code": "OH", "name": "Ohio"},
{"code": "OK", "name": "Oklahoma"},
{"code": "OR", "name": "Oregon"},
{"code": "PW", "name": "Palau"},
{"code": "PA", "name": "Pennsylvania"},
{"code": "PR", "name": "Puerto Rico"},
{"code": "RI", "name": "Rhode Island"},
{"code": "SC", "name": "South Carolina"},
{"code": "SD", "name": "South Dakota"},
{"code": "TN", "name": "Tennessee"},
{"code": "TX", "name": "Texas"},
{"code": "UT", "name": "Utah"},
{"code": "VT", "name": "Vermont"},
{"code": "VI", "name": "Virgin Islands"},
{"code": "VA", "name": "Virginia"},
{"code": "WA", "name": "Washington"},
{"code": "WV", "name": "West Virginia"},
{"code": "WI", "name": "Wisconsin"},
{"code": "WY", "name": "Wyoming"}
]

# List of possible environment variables
env_variables = ['MSSQL_STRING', 'MYSQL_STRING', 'POSTGRES_STRING']

# Check each environment variable in the list
for var in env_variables:
    if var in os.environ:
        connection_string = os.environ[var]
        db=var.split('_')[0]
        break

# Connection string

if "postgres" in connection_string:
    options = [option.strip() for option in connection_string.split(',')]
    user, password, host, port, database = [option.split('=')[1].strip('"') for option in options]

    try:
        conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS db (
                ID VARCHAR(255),
                BuildName VARCHAR(255)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS states (
                code VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255)
            )
        """)

        cursor.execute("SELECT EXISTS (SELECT 1 FROM states)")
        table_empty = not cursor.fetchone()[0]

        if table_empty:
            for item in initial_data:
                cursor.execute("INSERT INTO states (code, name) VALUES (%s, %s)", (item['code'], item['name']))

        conn.commit()

    except Exception as e:
        print(str(e))
        sys.exit(1)

elif "SQL" in connection_string:
    # try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        if not cursor.tables(table='db', tableType='TABLE').fetchone():
            cursor.execute("""
                CREATE TABLE db (
                    ID VARCHAR(255),
                    BuildName VARCHAR(255)
                )
            """)

        if not cursor.tables(table='states', tableType='TABLE').fetchone():
            cursor.execute("""
                CREATE TABLE states (
                    code VARCHAR(255) PRIMARY KEY,
                    name VARCHAR(255)
                )
            """)

            for item in initial_data:
                cursor.execute("INSERT INTO states (code, name) VALUES (%s, %s)", (item['code'], item['name']))

            conn.commit()

    # except Exception as e:
    #     print(str(e))
    #     sys.exit(1)

elif "mysql" in connection_string:

    options = [option.strip() for option in connection_string.split(',')]
    user, password, host, port, database, ssl_ca, ssl_disabled = [option.split('=')[1].strip('"') for option in options]

    try:
        conn = mysql.connector.connect(user=user,password=password,host=host,port=port)
        cursor = conn.cursor()
        # Check if the 'testdb' database exists
        cursor.execute("SHOW DATABASES LIKE 'testdb'")
        database_exists = cursor.fetchone()

        # Create the 'testdb' database if it does not exist
        if not database_exists:
            cursor.execute("CREATE DATABASE testdb")

        cursor.execute("USE testdb")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS db (
                ID VARCHAR(255),
                BuildName VARCHAR(255)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS states (
                code VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255)
            )
        """)

        cursor.execute("SELECT EXISTS (SELECT 1 FROM states)")
        table_empty = not cursor.fetchone()[0]

        if table_empty:
            for item in initial_data:
                cursor.execute("INSERT INTO states (code, name) VALUES (%s, %s)", (item['code'], item['name']))

        conn.commit()

    except Exception as e:
        print(str(e))
        sys.exit(1)

# Create Flask app
app = Flask(__name__)

# API endpoint to display initial data
@app.route('/state/list', methods=['GET'])
def get_state_list():
    try:
        cursor.execute("SELECT code, name FROM states")
        rows = cursor.fetchall()
        column_names = [column[0] for column in cursor.description]
        result = []
        for row in rows:
            result.append(dict(zip(column_names, row)))
        return jsonify(result)
    except Exception as e:
        print(str(e))
        return jsonify([])
    
# API endpoint to print the contents of the database
@app.route('/db/build', methods=['GET'])
def get_db_build():
    try:
        cursor.execute("SELECT * FROM db")
        rows = cursor.fetchall()
        column_names = [column[0] for column in cursor.description]
        result = []
        for row in rows:
            result.append(dict(zip(column_names, row)))
        return jsonify(result)
    except Exception as e:
        print(str(e))
        return jsonify([])

# API endpoint to receive POST request and process the data
@app.route('/db/build', methods=['POST'])
def post_db_build():
    try:
        data = json.loads(request.data)
        # Insert data into SQL database
        cursor.execute("INSERT INTO db (ID, BuildName) VALUES (%s, %s)", (data['id'], data['buildName']))
        conn.commit()

        return 'Data written in the database successfully!\n'
    except Exception as e:
        print(str(e))
        return str(e)+'\n'

# API endpoint to retrieve ID and BuildNumber for a given ID
@app.route('/db/build/<id>', methods=['GET'])
def get_db_build_by_id(id):
    try:
        cursor.execute("SELECT ID, BuildName FROM db WHERE ID = %s", id)
        row = cursor.fetchone()

        if row is not None:
            column_names = [column[0] for column in cursor.description]
            result = dict(zip(column_names, row))

            # Store retrieved data in Redis cache
            # redis_client.set(id, result['BuildName'])

            return jsonify(result)
        else:
            return f"No data found for ID: {id}"
    except Exception as e:
        print(str(e))
        return jsonify([])

# Route to display links to other endpoints
@app.route('/', methods=['GET'])
def home():
    return """
    <h1>API Links</h1>
    <ul>
        <li><a href="/state/list">State List</a></li>
        <li><a href="/db/build">Database Build</a></li>
    </ul>
    """

if __name__ == '__main__':
    app.run(debug=True)
