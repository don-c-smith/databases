import time
import datetime
import mysql.connector
from mysql.connector import Error


class ConnectionError(Exception):
    # Raised when the database connection is broken or not active
    def __init__(self, message="ERROR: the database connection is inactive."):
        self.message = message
        super().__init__(self.message)


def sql_connection():
    # This function actually makes the connection to the MySQL server
    try:  # Try to make the connection
        connection = mysql.connector.connect(
            user='testuser',
            password='testuser',
            database='tooldb',
            host='127.0.0.1',
            allow_local_infile=True)

        if connection.is_connected():  # If the connection is active and healthy
            return connection  # Return the connection object

    except Error as err_msg:  # If the connection fails
        print(f'ERROR: {err_msg}')  # Print the appropriate error message
    return None  # And return nothing


connection = sql_connection()


def connection_check(connection):
    # This function can be repeatedly referenced by component functions to ensure the connection is active
    if connection and connection.is_connected():  # If the object was returned AND the connection is active
        return True  # Return True
    else:  # Otherwise
        return False


def tool_selection(connection):
    # First check if the connection to MySQL is still active and healthy
    if not connection_check(connection):
        raise ConnectionError

    else:
        print('Welcome. Your connection to the ToolSales database is active.')
        while True:  # Start an infinite loop that will keep asking for input until a valid input is provided.
            try:
                r_id = int(input('Please enter your retailer ID number: '))  # Ask for retailer number

                if 1 <= r_id <= 10:  # Check if the integer is between 1 and 10
                    break  # If the integer is between 1 and 10, break out of the loop

                else:
                    print('Invalid input. Enter an integer between 1 and 10.')  # Error message and restart

            except ValueError:  # If integer cast is not successful
                print("Invalid input. Please enter a valid integer between 1 and 10.")  # Error message and restart

    myc = connection.cursor()  # Activate MySQL cursor

    fetch_r_name = "SELECT r_name FROM retailers WHERE r_id = %s"  # Store the query results
    myc.execute(fetch_r_name, (r_id,))  # Run the query separately - best practices to prevent SQL injection

    retailer_name_record = myc.fetchone()
    retailer_name = retailer_name_record[0]  # Store 'clean' retailer name

    print(f'Welcome, representative from {retailer_name}.')  # We did all that for a fancy welcome message.
    print('This form allows you to check sales and associated customer records for a specified tool.')
    time.sleep(1.5)  # Just giving people time to read

    print('Here is a list of all tool manufacturers in the database and their associated manufacturer ID numbers:')
    time.sleep(1.5)

    myc.execute("SELECT m_name, m_id FROM manufacturers ORDER BY m_id ASC")

    for record in myc:
        print(f'Manufacturer: {record[0]} || Manufacturer ID: {record[1]}')  # Cleaning up the output
        time.sleep(0.25)  # Some space between line prints

    while True:  # Start an infinite loop that will keep asking for input until a valid input is provided.
        try:
            m_id = int(input('From the above list, please enter the desired manufacturer ID number: '))

            if 1 <= m_id <= 20:  # Check if the integer is between 1 and 20
                break  # If the integer is between 1 and 20, break out of the loop

            else:
                print('Invalid input. Enter an integer between 1 and 20.')  # Error message and restart

        except ValueError:  # If integer cast is not successful
            print("Invalid input. Please enter a valid integer between 1 and 20.")  # Error message and restart

    # Similar process here - fetching manufacturer name, resisting injection, storing name
    fetch_m_name = "SELECT m_name FROM manufacturers WHERE m_id = %s"
    myc.execute(fetch_m_name, (m_id,))

    manufacturer_name_record = myc.fetchone()
    manufacturer_name = manufacturer_name_record[0]

    # Printing the tool list for the selected manufacturer
    print(f'{manufacturer_name} manufactures the following tools:')
    time.sleep(1)

    myc.execute(f"SELECT t_id, t_name_full FROM tools WHERE m_id = {m_id} ORDER BY t_name_full ASC")
    valid_t_ids = []  # Instantiate an empty list to store the valid t_ids

    for record in myc:
        print(f'Tool ID: {record[0]} Tool Name: {record[1]}')
        valid_t_ids.append(record[0])  # This makes sure input can be limited later on
        time.sleep(0.25)

    while True:  # Start an infinite loop that will keep asking for input until a valid input is provided.
        try:
            t_id = int(input('From the above list, please enter the desired tool ID number: '))

            if t_id in valid_t_ids:  # Check if the provided integer is in the valid list of t_ids just displayed
                break  # If so, break out of the loop

            else:
                print('Invalid input. Enter a tool ID number from the list provided above.')  # Error and restart

        except ValueError:  # If integer cast is not successful
            print("Invalid input. Please enter a valid tool ID number.")  # Error message and restart

    return t_id, r_id  # These are the values we need returned


#t_id, r_id = tool_selection(connection)  # Now store the returned values in the global scope


def fetch_sales(connection, t_id, r_id):  # Function to fetch the sale records
    if not connection_check(connection):  # Check connection
        raise ConnectionError

    else:
        myc = connection.cursor()  # Get the cursor

        print(f'You have selected tool ID {t_id}.')
        time.sleep(1)

        myc.execute(f"SELECT t_name_full FROM tools WHERE t_id = {t_id}")  # Fetching tool name
        tool_name_record = myc.fetchone()
        tool_name = tool_name_record[0]  # Storing tool name

        print(f'Locating sales records for {tool_name}...')
        time.sleep(1)

        # Finding out whether user wants a list of sales records or a total sum of sale value
        print('Do you want a full list of sale records for this tool or a total sum of sales for this tool?')

        while True:  # Start an infinite loop that will keep asking for input until a valid input is provided.
            try:
                method = input('Enter r for record list or s for sum: ')

                if method == 'r' or method == 's':  # Check to see if 'r' or 's' was entered
                    break  # If so, break out of the loop

                else:
                    print('Invalid input. Enter r for record list or s for sum.')  # Error and restart

            except ValueError:  # If a string is not supplied
                print('Invalid input. Enter r for record list or s for sum.')  # Error message and restart

        # Finding out whether user wants all records or just records for a specific date range
        print('Do you want to specify start and end dates for a particular sale period?')

        while True:  # Start an infinite loop that will keep asking for input until a valid input is provided.
            try:
                date_choice = input('Enter y for yes or n for no: ')

                if date_choice == 'y' or date_choice == 'n':  # Check to see if 'r' or 's' was entered
                    break  # If so, break out of the loop

                else:
                    print('Invalid input. Enter y for yes or n for no.')  # Error and restart

            except ValueError:  # If a string is not supplied
                print('Invalid input. Enter y for yes or n for no.')  # Error message and restart

        # Ensuring appropriate date input, both in format and sequence
        if date_choice == 'y':
            while True:  # Loop to ensure valid date entry for start_date
                start_date = input('Enter a start date in YYYY-MM-DD format, e.g. 2019-04-17: ')

                try:
                    # This will raise a ValueError if the input doesn't match the expected format
                    datetime.datetime.strptime(start_date, '%Y-%m-%d')
                    break

                except ValueError:  # Making sure the format matches more generally
                    print('Invalid date format for start date. Please try again.')

            while True:  # Loop to ensure valid date entry for end_date
                end_date = input('Enter an end date in YYYY-MM-DD format, e.g. 2019-04-23: ')

                try:
                    # This will raise a ValueError if the input doesn't match the expected format
                    datetime.datetime.strptime(end_date, '%Y-%m-%d')

                    # Ensure end date isn't before start date
                    if end_date < start_date:
                        print('End date cannot be earlier than start date. Please enter a valid end date.')
                        continue
                    break

                except ValueError:  # Making sure the format matches more generally
                    print('Invalid date format for end date. Please try again.')

        # For sale records with no date range
        if method == 'r' and date_choice == 'n':
            print(f'The full list of sale records for {tool_name} is as follows:')
            time.sleep(1)

            myc.execute("SELECT sale_id, c_id, sale_date, quantity, c_price "
                        "FROM sales WHERE t_id = %s AND r_id = %s ORDER BY sale_date", (t_id, r_id))  # Resist injection

            for record in myc:
                clean_date = record[2].strftime('%Y-%m-%d')  # Convert datetime.date to string
                clean_price = float(record[4])  # Convert Decimal to float
                print(f'Sale ID: {record[0]}, Customer ID: {record[1]}, Sale Date: {clean_date}, '
                      f'Quantity: {record[3]}, Customer Price: {clean_price}')

        # For sale records with a date range
        elif method == 'r' and date_choice == 'y':
            print(f'The full list of sale records for {tool_name} is as follows:')
            time.sleep(1)

            myc.execute("SELECT sale_id, c_id, sale_date, quantity, c_price "
                        "FROM sales WHERE t_id = %s AND r_id = %s "
                        "AND sale_date BETWEEN %s AND %s ORDER BY sale_date", (t_id, r_id, start_date, end_date))

            for record in myc:
                clean_date = record[2].strftime('%Y-%m-%d')  # Convert datetime.date to string
                clean_price = float(record[4])  # Convert Decimal to float
                print(f'Sale ID: {record[0]}, Customer ID: {record[1]}, Sale Date: {clean_date}, '
                      f'Quantity: {record[3]}, Customer Price: {clean_price}')

        # For a sum of sale value with no date range
        elif method == 's' and date_choice == 'n':
            print(f'The total value of sales for {tool_name} is:')

            myc.execute("SELECT T.t_name_full, SUM((S.quantity * S.c_price)) "
                        "FROM tools T, sales S "
                        "WHERE T.t_id = S.t_id AND S.t_id = %s AND S.r_id = %s "
                        "GROUP BY T.t_name_full", (t_id, r_id))

            for record in myc:  # There's only going to be one record
                clean_price = float(record[1])  # Convert Decimal to float
                print(f'Tool: {record[0]} || Sum of sales: {clean_price}')

        # For a sum of sale value with a date range
        elif method == 's' and date_choice == 'y':
            print(f'The total value of sales for {tool_name} is:')

            myc.execute("SELECT T.t_name_full, SUM((S.quantity * S.c_price)) "
                        "FROM tools T, sales S "
                        "WHERE T.t_id = S.t_id AND S.t_id = %s AND S.r_id = %s "
                        "AND sale_date BETWEEN %s AND %s "
                        "GROUP BY T.t_name_full", (t_id, r_id, start_date, end_date))

            for record in myc:  # There's only going to be one record
                clean_price = float(record[1])  # Convert Decimal to float
                print(f'Tool: {record[0]} || Sum of sales: {clean_price}')

    return  # End function


#fetch_sales(connection, t_id, r_id)  # Call the sales-fetching function

#connection.close()  # Close the MySQL connection

# And finally, ensure the script can be run directly from the CLI
def main():
    connection = sql_connection()
    t_id, r_id = tool_selection(connection)
    fetch_sales(connection, t_id, r_id)
    connection.close()


if __name__ == '__main__':
    main()
