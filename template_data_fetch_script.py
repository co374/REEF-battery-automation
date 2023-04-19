"""
Description:
Python script to fetch data from the REEF's solar PV system inverter, power meter and a 
raspberry pi server via the Modbus/Tcp protocol, then both store it in tables in a local 
database (one for each server), and send it to Adafruit IO feeds to be displayed on a dashboard.


Notes about the servers:
-   All given register addresses for the inverter were one higher than the true addresses.
-   IP and port info given for the RPi never connects to it - either it's down or info is wrong.
-   Power meter can only handle requests some of the time, most times it fails.

Adaptability:
To adapt this script for other modbus servers, edit the server_data dictionary list in 
'Server information' and change the global variables in main.py (add your Adafruit IO details).
Don't use spaces when naming servers and registers or it will cause errors in SQL statements.

For some versions of 'pymodbus', replace the 'pymodbus.client' import with 'pymodbus.client.sync'
(if 'pymodbus.client' library is not found).
"""



#-----------------------------------------------------------------------------------------------#
# Imports                                                                                       #
#-----------------------------------------------------------------------------------------------#



from pymodbus.client import ModbusTcpClient as Client
from pymodbus.payload import BinaryPayloadDecoder as Decoder
from pymodbus.constants import Endian
from pymodbus.exceptions import ConnectionException
import sqlite3
from datetime import datetime
import os


#-----------------------------------------------------------------------------------------------#
#  Server information                                                                           #
#-----------------------------------------------------------------------------------------------#



server_data = [
    {
        'server name': 'Inverter',
        'IP address': '144.173.77.190',
        'port number': 502,
        'unit': 1,
        'registers': [
            {
                'register name': 'AC_power',
                'address': 40091,
                'count': 2,
                'data type': 'float32',
                'units': 'W',
                'conversion function': 'none'
            },
            {
                'register name': 'Total_AC_current',
                'address': 40071,
                'count': 2,
                'data type': 'float32',
                'units': 'A',
                'conversion function': 'none'
            },
            {
                'register name': 'Phase_voltage_AN',
                'address': 40085,
                'count': 2,
                'data type': 'float32',
                'units': 'V',
                'conversion function': 'none'
            }
        ]
    },
    {
        'server name': 'Power_meter',
        'IP address': '144.173.77.134',
        'port number': 502,
        'unit': 5,
        'registers': [
            {
                'register name': 'System_current',
                'address': 18440,
                'count': 2,
                'data type': 'uint32',
                'units': 'A',
                'conversion function': lambda x: x/1000     #given in mA
            },
            {
                'register name': 'System_Ph_N_voltage',
                'address': 18436,
                'count': 2,
                'data type': 'uint32',
                'units': 'V',
                'conversion function': lambda x: x/100     #given in V/100
            },
            {
                'register name': 'System_total_active_power',
                'address': 18476,
                'count': 2,
                'data type': 'int32',
                'units': 'W',
                'conversion function': 'none'
            }
        ]
    }
]



#-----------------------------------------------------------------------------------------------#
#  Helper functions                                                                             #
#-----------------------------------------------------------------------------------------------#



# Uses server information to create a table in the database with the table name as the server
# name and the column names as the register names. Also with interger id and timestamp columns.
def table_creation_string(server):
    string = f"CREATE TABLE IF NOT EXISTS {server['server name']} (id INTEGER PRIMARY KEY, \
              timestamp DATETIME"
    for register in server['registers']:
        string += f", {register['register name']}"
    string += ")"
    return string


# Returns a string containing an "INSERT INTO" SQL statement for
# inserting a null row into the given server's table.
def insertion_query(server):
    string = f"INSERT INTO {server['server name']} (timestamp"
    for register in server['registers']:
        string += f", {register['register name']}"
    string += ") VALUES (?"
    for register in server['registers']:
        string += ', ?'
    string += ')'
    return string


# Returns the corresponding tuple of null values for the above insertion query
def null_tuple(server):
    null_tuple = (None,) # accounting for null timestamp value
    for register in server['registers']:
        null_tuple += (None,)
    return null_tuple


# Updates the timestamp of a data entry/table row.
def update_timestamp(server):
    timestamp = datetime.now()
    table_name = server['server name']
    cursor.execute(f"UPDATE {table_name} SET timestamp = ? WHERE id = ?", (timestamp, row_id))
    conn.commit()


# Explicitly sets the path of the database to be in the same directory the script is in.
def database_path(database_name):
    script_path = os.path.abspath(__file__)
    script_directory = os.path.dirname(script_path)
    database_path = os.path.join(script_directory, database_name)
    return database_path



#-----------------------------------------------------------------------------------------------#
# Setup code                                                                                    #
#-----------------------------------------------------------------------------------------------#



# Create database and tables if they don't already exist.
db_path = database_path('REEF_PV_system_data.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

for server in server_data:
    cursor.execute(table_creation_string(server))



#-----------------------------------------------------------------------------------------------#
# Main code block                                                                               #
#-----------------------------------------------------------------------------------------------#



for server in server_data:


    # Insert a null row into the server's table using two helper functions.  
    cursor.execute(insertion_query(server), null_tuple(server))
    conn.commit()


    # Get the integer id of the null row.
    row_id = cursor.lastrowid


    # Create a ModbusTcpClient object using the server IP address and port number.
    client = Client(host=server['IP address'], port=server['port number'])


    # Attempt to connect to the server with the .connect() method of the ModbusTcpClient object.
    # Raise a connection exception if it fails that is handled by printing an error message
    # to the terminal, adding a timestamp to the null row in the table, and then skipping 
    # to the next server in the loop.
    try: 
        if not client.connect():
            raise ConnectionException
    except ConnectionException:
        print(f"Error connecting to '{server['server name']}'")
        update_timestamp(server)
        continue


    for register in server['registers']:


        # Attempt to read the register using the register information in the 'server_data'
        # dictionary list. Catch reading errors via the .isError() method of the response object
        # and catch connection errors (e.g. unexpected closing of connection) with a try-except clause.
        # Handle errors the same as before, but skip to the next register instead of the next server.
        try:
            response = client.read_holding_registers(address=register['address'], \
                       count=register['count'], unit=server['unit'])
            if response.isError():
                print(f"Error reading from '{server['server name']}; {register['register name']}'")
                update_timestamp(server)
                continue

            registers = response.registers
        except ConnectionException:
            print(f"Error: connection unexpectedly closed while reading {server['server name']}; {register['register name']}")
            update_timestamp(server)
            continue


        # Create Decoder class object and use one of its built in decoding methods to decode the 
        # register's raw data into its value in the correct data type, using the register's data type
        # information specified in the server_data dictionary list.
        decoder = Decoder.fromRegisters(registers, byteorder=Endian.Big, wordorder=Endian.Big)
        if register['data type'] == 'float32':
            value = decoder.decode_32bit_float()
        elif register['data type'] == 'uint32':
            value = decoder.decode_32bit_uint()
        elif register['data type'] == 'int32':
            value = decoder.decode_32bit_int()


        # Apply a unit conversion (if applicable), so that everything is in SI units, then round
        # the value to 3 d.p to keep it tidy and avoid trailing floats in the database tables.
        if register['conversion function'] != 'none':
            value = register['conversion function'](value) 
        value = round(value, 3)


        # Print the value to the terminal alongside the server and register names.
        print(f"{server['server name']}; {register['register name']}: {value} {register['units']}")

        
        # Insert the register's value into the correct database table by using an UPDATE query        
        table_name = server['server name']
        column_name = register['register name']
        cursor.execute(f"UPDATE {table_name} SET {column_name} = ? WHERE id = ?", (value, row_id))
        conn.commit()


    # Add a timestamp to the table row after all non-erroneous register data have been added
    update_timestamp(server)


    # Close the connection to the server again after each server iteration and at the end of the script.
    client.close()