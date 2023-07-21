# Python script managing MariaDB containers (via dockers)
# Executing SQL commands, copying tables, creating new instances/containers, and deleting containers
# Flask to showcase all instances

import mysql.connector
import sys
import os
import subprocess
import docker
from dotenv import load_dotenv
from flask import Flask, render_template
app = Flask(__name__)

# Load environment variables from a .env file
load_dotenv()

x = 0

def get_database_connection():
    try:
        conn = mysql.connector.connect(
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
        )
        return conn
    except mysql.connector.Error as e:
        print(f"couldnt connect to the database: {e}") 
        sys.exit(1)

# Function to execute SQL commands on the database
def execute_sql_commands(conn, sql_commands):
    cursor = conn.cursor()

    try:
        # Split the SQL commands by semicolon
        commands = sql_commands.split(";")

        # Execute each command separately
        for command in commands:
            command = command.strip()
            if command:
                cursor.execute(command)
                conn.commit()

    except mysql.connector.Error as e:
        print(f"error executing your sql commands: {e}")

    cursor.close()


def choose_database_instance():
    while True:
        # List all existing containers
        existing_containers = subprocess.check_output("docker ps -aq --format '{{.Names}}'", shell=True).decode().splitlines()

        print("\n\nplease choose the container/MariaDB instance to run SQL commands:")

        # Print the existing container options with port information
        if existing_containers:
            print("\nexisting mariadb-instances:")
            for i, container_name in enumerate(existing_containers, start=1):
                port = get_container_port(container_name)
                if port:
                    print(f"{i}. container: {container_name} (port: {port})")
                else:
                    print(f"{i}. container: {container_name} (port: non-existent)")
        else:
            print("\nyou currently have no instances. please make one with option b!")

        # Add some empty lines for better readability
        print("\nadditional options:")
        print("a. copy tables between instances")
        print("b. create a new MariaDB instance")
        print("c. delete a MariaDB instance")
        print("d. or just quit the program")

        choice = input("\nenter your choice (container number, 'a', 'b', 'c', or 'd'): ")


        if choice.isdigit() and int(choice) - 1 <= len(existing_containers):
            container_name = existing_containers[int(choice) - 1]
            port = get_container_port(container_name)
            if port:
                os.environ["PORT"] = port
                break
            else:
                print(f"failed to retrieve port for container '{container_name}'.")
                continue
        elif choice == 'a':
            copy_tables()
            continue
        elif choice == 'b':
            create_new_instance()
            continue
        elif choice == 'c':
            delete_container(existing_containers)
            continue
        elif choice == 'd':
            print("\nquitting the program... thanks for using me!")
            global x
            x = 1
            break
        else:
            print("\nthats not an option...")

def delete_container(existing_containers):
    container_number = input("\nenter the number of the container to delete: ")

    if container_number.isdigit() and int(container_number) - 1 < len(existing_containers):
        container_name = existing_containers[int(container_number) - 1]
        try:
            subprocess.run(f"docker rm -f {container_name}", shell=True, check=True)
            print(f"\ncontainer '{container_name}' deleted successfully.")
        except subprocess.CalledProcessError as e:
            print(f"an error occurred while deleting container '{container_name}': {e}")
    else:
        print("\ninvalid container number. try again!")


def get_container_port(container_name):
    try:
        port_output = subprocess.check_output(f"docker port {container_name}", shell=True).decode().strip()
        port_lines = port_output.splitlines()
        if len(port_lines) > 0:
            port = port_lines[0].split(":")[-1]
            return port
        else:
            return None
    except subprocess.CalledProcessError as e:
        print(f"i couldnt retrieve the port '{container_name}': {e}")
        return None

# Function to read SQL commands from a file
def read_sql_commands(file_path):
    with open(file_path, "r") as file:
        sql_commands = file.read()
    return sql_commands

# Function to delete a temporary file
def delete_temp_file(file_path):
    try:
        os.remove(file_path)
    except OSError as e:
        print(f"error deleting the temp file: {e}")

# Function to prompt the user to choose whether to continue or not
def continue_prompt():
    while True:
        choice = input("\ndo you want to continue? (y/n): ")
        if choice.lower() == "y":
            return True
        elif choice.lower() == "n":
            return False
        else:
            print("\nthats not even an option...")

def copy_tables():
    try:
        # List all existing containers
        existing_containers = subprocess.check_output("docker ps -aq --format '{{.Names}}'", shell=True).decode().splitlines()

        print("\nplease choose the source instance:")
        for i, container_name in enumerate(existing_containers, start=1):
            print(f"{i}. {container_name}")

        while True:
            source_instance_choice = input("\nenter the number of the source instance: ")
            if source_instance_choice.isdigit() and 1 <= int(source_instance_choice) <= len(existing_containers):
                source_instance_name = existing_containers[int(source_instance_choice) - 1]
                break
            else:
                print("\ninvalid source instance number.")

        print("\nplease choose the destination instance:")
        for i, container_name in enumerate(existing_containers, start=1):
            print(f"{i}. {container_name}")

        while True:
            destination_instance_choice = input("\nenter the number of the destination instance: ")
            if destination_instance_choice.isdigit() and 1 <= int(destination_instance_choice) <= len(existing_containers):
                destination_instance_name = existing_containers[int(destination_instance_choice) - 1]
                break
            else:
                print("\ninvalid destination instance number.")

        # Establish connections to the source and destination database instances
        os.environ["PORT"] = get_container_port(source_instance_name)
        source_conn = get_database_connection()

        os.environ["PORT"] = get_container_port(destination_instance_name)
        destination_conn = get_database_connection()

        # Prompt the user to choose the source database
        source_cursor = source_conn.cursor()
        source_cursor.execute("SHOW DATABASES")
        databases = source_cursor.fetchall()
        source_cursor.close()

        print("source databases:")
        for i, database in enumerate(databases, start=1):
            print(f"{i}. {database[0]}")

        while True:
            source_database_choice = input("\nenter the number of the source database: ")
            if source_database_choice.isdigit() and 1 <= int(source_database_choice) <= len(databases):
                source_database = databases[int(source_database_choice) - 1][0]
                break
            else:
                print("\ninvalid source database number.")

        # Prompt the user to choose the table to copy
        source_conn.database = source_database
        source_cursor = source_conn.cursor()
        source_cursor.execute("SHOW TABLES")
        tables = source_cursor.fetchall()

        print("\ntables in the source database:")
        for i, table in enumerate(tables, start=1):
            print(f"{i}. {table[0]}")

        while True:
            table_choice = input("\nenter the number of the table to copy: ")
            if table_choice.isdigit() and 1 <= int(table_choice) <= len(tables):
                table_to_copy = tables[int(table_choice) - 1][0]
                break
            else:
                print("\ninvalid table number.")

        # Prompt the user to choose the destination database
        destination_cursor = destination_conn.cursor()
        destination_cursor.execute("SHOW DATABASES")
        databases = destination_cursor.fetchall()
        destination_cursor.close()

        print("\ndestination databases:")
        for i, database in enumerate(databases, start=1):
            print(f"{i}. {database[0]}")

        while True:
            destination_database_choice = input("\nenter the number of the destination database: ")
            if destination_database_choice.isdigit() and 1 <= int(destination_database_choice) <= len(databases):
                destination_database = databases[int(destination_database_choice) - 1][0]
                break
            else:
                print("\ninvalid destination database number.")

        # Get the table structure from the source instance
        source_conn.database = source_database
        source_cursor = source_conn.cursor()
        source_cursor.execute(f"SHOW CREATE TABLE `{table_to_copy}`")
        table_structure = source_cursor.fetchone()[1]
        source_cursor.close()

        # Remove any database-specific syntax from the table structure
        table_structure = table_structure.replace(f"CREATE TABLE `{table_to_copy}`", f"CREATE TABLE `{destination_database}`.`{table_to_copy}`")

        # Check if the destination table already exists
        destination_conn.database = destination_database
        destination_cursor = destination_conn.cursor()
        destination_cursor.execute(f"SHOW TABLES LIKE '{table_to_copy}'")
        existing_table = destination_cursor.fetchone()
        destination_cursor.close()

        # If the table exists, prompt the user to choose whether to append or overwrite data
        if existing_table:
            while True:
                overwrite_choice = input("\nthe destination table already exists. do you want to:\n"
                                         "\n1. append data to the existing table\n"
                                         "\n2. overwrite the existing table\n"
                                         "\nenter your choice (1 or 2): ")
                if overwrite_choice in ["1", "2"]:
                    break
                else:
                    print("\ninvalid choice. please enter 1 or 2.")

            if overwrite_choice == "1":
                # Append data to the existing table
                destination_cursor = destination_conn.cursor()
                source_cursor = source_conn.cursor()
                source_cursor.execute(f"SELECT * FROM `{table_to_copy}`")
                records = source_cursor.fetchall()
                column_names = [desc[0] for desc in source_cursor.description]
                source_cursor.close()

                destination_cursor.executemany(
                    f"INSERT INTO `{destination_database}`.`{table_to_copy}` ({', '.join(column_names)}) "
                    f"VALUES ({', '.join(['%s'] * len(column_names))})",
                    records
                )
                destination_conn.commit()
                destination_cursor.close()

                print(f"\ndata appended to the existing table '{table_to_copy}' in the destination database.")
                return

            elif overwrite_choice == "2":
                # Overwrite the existing table
                destination_cursor = destination_conn.cursor()
                destination_cursor.execute(f"DROP TABLE IF EXISTS `{destination_database}`.`{table_to_copy}`")
                destination_cursor.execute(table_structure)
                destination_cursor.close()

        else:
            # Create the destination table with the same structure
            destination_cursor = destination_conn.cursor()
            destination_cursor.execute(table_structure)
            destination_cursor.close()

        # Select all data from the source table
        source_cursor = source_conn.cursor()
        source_cursor.execute(f"SELECT * FROM `{table_to_copy}`")
        records = source_cursor.fetchall()

        if len(records) == 0:
            print("\nthe source table is empty. there is no data to copy, but the table structure has been copied anyway.")
            return

        # Get the column names from the source cursor
        column_names = [desc[0] for desc in source_cursor.description]
        source_cursor.close()

        # Insert the data into the destination table
        destination_cursor = destination_conn.cursor()
        destination_cursor.executemany(
            f"INSERT INTO `{destination_database}`.`{table_to_copy}` ({', '.join(column_names)}) "
            f"VALUES ({', '.join(['%s'] * len(column_names))})",
            records
        )
        destination_conn.commit()
        destination_cursor.close()

        print(f"\nthe table '{table_to_copy}' has been copied successfully from the source to the destination database.")

    except mysql.connector.Error as e:
        print(f"an error occurred while copying: {e}")

    finally:
        if source_conn.is_connected():
            source_conn.close()
        if destination_conn.is_connected():
            destination_conn.close()

def get_used_ports():
    client = docker.from_env()
    used_ports = set()
    for container in client.containers.list():
        ports = container.attrs["NetworkSettings"]["Ports"]
        for port in ports:
            if ports[port] is not None:
                host_port = ports[port][0]["HostPort"]
                used_ports.add(int(host_port))
    return used_ports

def create_new_instance():
    container_name = input("\nenter a name for the new MariaDB instance: ")

    # Prompt the user to enter the port number for the new instance
    while True:
        port = input("\nenter the port number for the new instance: ")
        if not port.isdigit():
            print("\ninvalid port number. please enter a valid number.")
            continue
        port = int(port)
        if port in get_used_ports():
            print("\nport is already in use. please choose a different port.")
            continue
        break

    # Prompt the user to enter the root password for the new instance
    root_password = input("\nenter the root password for the new instance: ")

    # Create the new MariaDB container using Docker
    command = f"docker run --name {container_name} -p {port}:3306 -e MYSQL_ROOT_PASSWORD={root_password} -d mariadb"
    try:
        subprocess.run(command, shell=True, check=True)
        print(f"\nnew MariaDB instance '{container_name}' created successfully.")
    except subprocess.CalledProcessError as e:
        print(f"\nan error occurred while creating the new instance: {e}")


def get_all_databases_info():
    # List all existing containers
    existing_containers = subprocess.check_output("docker ps -aq --format '{{.Names}}'", shell=True).decode().splitlines()
    database_info = []

    for container_name in existing_containers:
        port = get_container_port(container_name)
        if port:
            os.environ["PORT"] = port
            conn = get_database_connection()
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall()]
            cursor.close()
            conn.close()
            database_info.append({"container_name": container_name, "port": port, "databases": databases})

    return database_info


@app.route('/')
def show_all_databases():
    database_info = get_all_databases_info()
    return render_template('index.html', database_info=database_info)


# Main loop
while True:
    temp_file = "temp_commands.sql"  # Temporary file path to store SQL commands
    sql_commands = ""  # Variable to store SQL commands

    # User prompts in the terminal
    choose_database_instance()
    

    if x == 0:
        # Establish a connection to the database
        conn = get_database_connection()

        try:
            # Open the temporary file in append mode
            with open(temp_file, "a") as file:
                # Prompt the user to enter SQL commands until they indicate they are done
                while True:
                    user_input = input("\nenter your sql commands! (type 'done!' to finish): ")
                    if user_input.lower() == "done!":
                        break

            # Read the contents of the temporary file
            sql_commands = read_sql_commands(temp_file)

            # Execute the SQL commands on the chosen database instance
            execute_sql_commands(conn, sql_commands)
        finally:
            # Delete the temporary file
            delete_temp_file(temp_file)

        # Close the database connection
        conn.close()

        # Prompt the user to continue or not
        if not continue_prompt():
            break
    else:
        break


if __name__ == '__main__':
    app.run()