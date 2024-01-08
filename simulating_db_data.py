"""
This Python script will first generate data frames based off of specific tables which do not require
randomly-generated data - the manufacturers of tools, the specific tools manufactured, and the retailers are 'closed'
lists which I have pre-populated with appropriate data. They will constitute the 'small-size' relations that will serve
as the foundations to generate the large-size relations, which will represent the 'orders,' 'sales,' and 'customers'
entities.

Using these foundational small-size data frames, I can eventually call on the NumPy and Faker libraries to randomly
generate data with which to populate the large-size relations. The pre-populated tables are found in the 'codebook'
I built in Excel to serve as a reference source for my database - individual Excel tabs are accessible via Pandas.

The end goal is to have 11 Python dataframes, one for each relation in the database, which contain all the
simulated data. Having the data stored in the dataframes gives me more flexibility in terms of generating text output
which can be saved as a .sql script and executed within MySQL on the CLI. It should also make future adjustments and
procedures easier.
"""

# Library imports
import pandas as pd  # To build the dataframes
import numpy as np  # For sampling without replacement to generate unique values
from faker import Faker as fk  # To create values for the customers relation
from random import randint  # To use when randomly-generating values

# Begin by creating the initial dataframes using the Excel tabs, which can be done inside a callable function
def construct_initial_dataframes():
    excel_path = r'C:\Users\doncs\Documents\Ritchie\Databases_1\ToolDB_Codebook.xlsx'  # Store the filepath

    # I need each tab name from the sheet to be stored in a callable object when I read in the data
    manufacturers_tab = 'Manufacturers'
    tools_tab = 'Tools'
    retailers_tab = 'Retailers'

    # I also need to create a datatype mapping for each tab so the import works correctly
    manufacturers_mapping = {'m_id': int,
                             'm_name': str,
                             'country_code': int,
                             'country_name': str,
                             'eu_member': int,
                             'imprint': int,
                             'parent_id': 'Int32',  # Need to specify to convert to Pandas nullable integer type
                             'parent_name': str}

    tools_mapping = {'m_id': int,
                     't_id': int,
                     't_name_trunc': str,
                     't_name_full': str,
                     't_type_code': str,
                     'active': int,
                     'eu_comp': int,
                     'voltage': 'Int32',  # Need to specify to convert to Pandas nullable integer type
                     'init_yom': int}

    retailers_mapping = {'r_id': int,
                         'r_name': str,
                         'country_code': int,
                         'country_name': str,
                         'indep': int,
                         'loc_id': 'Int32',  # Need to specify to convert to Pandas nullable integer type
                         'loc_address': str,
                         'loc_zip': 'Int32'}  # Need to specify to convert to Pandas nullable integer type

    # Read each appropriate Excel tab and store in a Pandas dataframe
    manufacturers = pd.read_excel(excel_path, sheet_name=manufacturers_tab, dtype=manufacturers_mapping)
    tools = pd.read_excel(excel_path, sheet_name=tools_tab, dtype=tools_mapping)
    retailers = pd.read_excel(excel_path, sheet_name=retailers_tab, dtype=retailers_mapping)

    return manufacturers, tools, retailers


manufacturers, tools, retailers = construct_initial_dataframes()  # Save returned values as global-scope dataframes

"""
The next dataframe I'll create will represent the 'build' relation. Because of the structure of my database, the 'build'
relation serves as the relational link between the 'manufacturers' and 'tools' relations, both of which were
pre-populated in Excel as small-scale, foundational relations. The 'build' relation is, in its entirety, two columns 
where each tool id number (t_id) is matched with the appropriate manufacturer id number (m_id). Because the 'tool'
relation (for convenience's sake) already contains this information, I can generate the dataframe for the 'build'
relation by simply extracting those two columns from the 'tools' relation and saving them to a new dataframe object.
"""
def construct_build_dataframe():
    # Copy-and-paste the columns and values we need
    build = tools[['m_id', 't_id']].copy()

    return build  # Return the dataframe


build = construct_build_dataframe()

"""
Now, I need to begin properly simulating data at the scale required by the assignment. I need one dataframe containing
tens of thousands of records and two dataframes containing thousands of records. 

Logically, in the context of my database, what makes the most sense is for the 'orders' dataframe to be the largest, 
as it makes sense that retailers would order more tools than they would actively stock in purchasable inventory and 
that there would be more tools than customers.

The relations serving as the relational 'links' between the large-scale tables can be created using the relations which
represent the entities as references, e.g. given the ER relationship "tools comprise orders placed by retailers," the
'comprise' and 'place' dataframes can be created after the large-scale 'orders' dataframe has been populated, using the
'orders' dataframe as a reference.

Therefore, the next step is to create the 'orders' dataframe and fill it with simulated data. 
"""
def construct_orders_dataframe(tools_dataframe):
    # Start by initializing a random number generator from the NumPy library
    randomgen = np.random.default_rng(44)  # Use a seed value for consistent values when I re-run the function

    # Create a list of unique order_id values
    order_ids = list(range(1, 20001))

    # Randomly generate values for the 'pending' field as SQL boolean
    pending_values = randomgen.choice([0, 1], size=20000)

    # Randomly generate 'order_date' field within the past year
    start_date = pd.to_datetime('2022-07-10')  # Starting 1 year ago
    total_days = (pd.to_datetime('2023-07-10') - start_date).days  # Calculates total number b/t start and end dates
    # Now I can use those two timedelta objects to create randomly-generated order dates
    order_dates = start_date + pd.to_timedelta(randomgen.integers(0, total_days, size=20000), unit='d')

    # The 'ship_date' field has to be generated based on the 'order_date' and 'pending' fields
    # If the order is pending, ship_date is None-type
    # If the order is not pending, ship_date is between 1 and 14 days after the order date
    ship_dates = [order_dates[i] + pd.to_timedelta(randint(1, 15), unit='d') if pending_values[i] == 0
                  else None for i in range(20000)]  # If the order is pending, don't generate a value for ship_date

    # Randomly generate values for 't_id' referencing the tools dataframe
    t_ids = randomgen.choice(tools_dataframe['t_id'], size=20000)

    # Randomly generate values for the 't_quant' field using random integers between 1 and 50
    t_quants = randomgen.integers(1, 51, size=20000)  # Recall that the upper bound isn't inclusive

    # I need to make sure each t_id value (tool id) is associated with a consistent price
    # Randomly generate values for the 'r_price' field as floats between 100 and 8000 dollars with two decimal places
    # Use the uniform distribution to make sure I get random post-decimal quantities
    # By mapping each randomly-generated price to a specific tool inside a dictionary, I should have what I need
    # A strong use case for dictionary comprehension
    tool_price_dict = {t_id: round(randomgen.uniform(100, 8001), 2) for t_id in tools_dataframe['t_id'].unique()}

    # Now I can map the tool IDs in the orders to these prices
    r_prices = [tool_price_dict[t_id] for t_id in t_ids]

    # Finally, I can create and populate the 'orders' dataframe
    orders = pd.DataFrame({
        'order_id': order_ids,
        'order_date': order_dates,
        'pending': pending_values,
        'ship_date': ship_dates,
        't_id': t_ids,
        't_quant': t_quants,
        'r_price': r_prices
    })

    return orders


orders = construct_orders_dataframe(tools)

# Debugging - ensuring that each t_id has a consistent price
# If my code worked, each t_id will have a nunique() value of 1, i.e. each tool will have 1 consistent price value
# unique_price_check = orders.groupby('t_id')['r_price'].nunique()  # Aggregate by t_id, and count unique r_price values
# print(unique_price_check)  # The tool prices are consistent

"""
The next step is to generate and populate the 'comprise' dataframe. The 'comprise' relation contains two values: t_id
and order_id. Because both of these columns exist in the recently-populated orders dataframe, I can generate the 
dataframe for the 'comprise' relation by simply extracting those two columns from the 'tools' relation and saving them 
to a new dataframe object.
"""
def construct_comprise_dataframe():
    # Copy-and-paste the columns and values I need from the orders dataframe
    comprise = orders[['t_id', 'order_id']].copy()

    return comprise  # Return the dataframe


comprise = construct_comprise_dataframe()

# Debugging - ensuring that a small sample of order_id and t_id values match in both dataframes
# print(orders.head())
# print(comprise.head())
# Success! They match

"""
The next step is to generate and populate the 'place' dataframe. The 'place' relation contains two values: order_id and
r_id. Because the retailers data was pre-populated (since it's a small-scale relation,) creating this dataframe should
only require me to randomly assign order_id values to retailers.
"""
def construct_place_dataframe(orders_dataframe, retailers_dataframe):
    # Start by initializing a random number generator from the NumPy library
    randomgen = np.random.default_rng(44)  # Use a seed value so I get consistent values when I re-run the function

    # Randomly assign a retailer id to each of the 20000 orders
    # Use size=len(orders_dataframe) so that I randomly choose retailers as many times as I have orders
    r_id_values = randomgen.choice(retailers_dataframe['r_id'], size=len(orders_dataframe))

    # Now I can create and populate the 'place' dataframe
    place = pd.DataFrame({
        'order_id': orders_dataframe['order_id'],
        'r_id': r_id_values
    })

    return place


place = construct_place_dataframe(orders, retailers)

# Debugging - making sure that r_id values are between 1 and 10 and that the r_id assignment in consistent when re-run
# print(retailers)
# print(orders.head(30))
# print(place.head(30))  # Everything looks good

"""
The next step is to generate and populate the 'stock' dataframe. Current stocking actions can exist independent of the 
orders the retailers have recently made, which is convenient. I will generate 10000 stocking records, choosing randomly
from among the retailers and tools, and simply generating a reasonable random integer for quantity stocked. In terms of
stock dates, I can use a longer timeframe, assuming that some items have been in inventory for a while and haven't been 
sold yet. 
"""
def construct_stock_dataframe(retailers_dataframe, orders_dataframe):
    # Start by initializing a random number generator from the NumPy library
    randomgen = np.random.default_rng(44)  # Use a seed value so I get consistent values when I re-run the function

    # Randomly generate values for 'r_id' referencing the retailers dataframe
    r_id_values = randomgen.choice(retailers_dataframe['r_id'], size=10000)

    # Randomly generate values for 't_id' referencing the orders dataframe instead of tools dataframe
    t_id_values = randomgen.choice(orders_dataframe['t_id'], size=10000)

    # Randomly generate quantities using random integers between 1 and 50
    s_quants = randomgen.integers(1, 51, size=10000)  # Again, upper bound is not inclusive

    # Generate 'stock_date' values within the past two years
    start_date = pd.to_datetime('2020-07-10')  # Start three years ago
    total_days = (pd.to_datetime('2023-07-10') - start_date).days  # Calculates total number b/t start and end dates
    stock_dates = start_date + pd.to_timedelta(randomgen.integers(0, total_days, size=10000), unit='d')

    # Now we can create and populate the 'stock' dataframe
    stock = pd.DataFrame({
        'r_id': r_id_values.astype('int32'),
        't_id': t_id_values.astype('int32'),
        'quantity': s_quants.astype('int32'),
        'stock_date': stock_dates
    })

    return stock


stock = construct_stock_dataframe(retailers, tools)

# Debugging - making sure tool ids, retailer ids, stock dates look okay
# print(stock.head(50))
# Everything seems fine

"""
The next step is to generate and populate the 'inventory' dataframe. This dataframe will very closely mirror the 
'stock' dataframe, but since it represents currently-available inventory, there's no need to keep the stock_date field. 
Instead, the inventory dataframe will replace the stock_date field with a c_price field, which will represent the price 
that the end consumer pays for the tool, which will be marked up by a reasonable random percentage, referencing the 
r_price values in the orders dataframe at the tool level.
"""
def construct_inventory_dataframe(stock_dataframe, orders_dataframe):
    # Cast 'r_id', 't_id', and 'quantity' to integer-type
    stock_dataframe[['r_id', 't_id', 'quantity']] = stock_dataframe[['r_id', 't_id', 'quantity']].fillna(0).astype(int)

    # Then initialize a random number generator from the NumPy library
    randomgen = np.random.default_rng(44)  # Use a seed value so I get consistent values when I re-run the function

    # Get the value from the r_price field for each tool from the orders dataframe, referencing t_id
    tool_price_dict = orders_dataframe.groupby('t_id')['r_price'].first().to_dict()

    # Creating a new dataframe by merging the stock dataframe and r_price values
    # Note that this will add a new column 'r_price' to the stock dataframe
    inventory = stock_dataframe.copy()
    inventory['r_price'] = inventory['t_id'].map(tool_price_dict)  # Mapping in the r_price values for each tool

    # Calculate 'c_price' values by multiplying 'r_price' by a random percent between 110% and 140%
    # I'll use the uniform distribution to draw more granular random markup values
    markup = inventory['r_price'] * (1 + randomgen.uniform(0.1, 0.4, size=len(inventory)))
    inventory['c_price'] = markup.round(2)  # We need to round the marked-up prices to simulate prices in USD

    # Then, I can drop the 'r_price' and 'stock_date' columns as they're not present in the inventory dataframe
    inventory = inventory.drop(columns=['r_price', 'stock_date'])

    # Ensure datatype consistency
    inventory = inventory.astype({'r_id': 'int32', 't_id': 'int32', 'quantity': 'int32'})

    # Reorder columns
    inventory = inventory.reindex(columns=['r_id', 't_id', 'quantity', 'c_price'])

    # Now we can create and populate the 'inventory' dataframe
    return inventory


inventory = construct_inventory_dataframe(stock, orders)

# Debugging - checking for sensible values and matching records in the stock and inventory dataframes
# print(stock.head(50))
# print(inventory.head(50))
# All seems well

"""
Now to construct the sales dataframe. This will be another very large dataframe with randomly generated data. The only
things I should need to reference other dataframes for are inventory to get t_id, and c_price and retailers to choose 
random r_id values. Sales data can be independent from current inventory data - these are essentially business records 
of sales already made, which makes this easier. I'm going to have 50000 historical sale records.
"""
def construct_sales_dataframe(inventory_dataframe, retailers_dataframe):
    # Start by initializing a random number generator from the NumPy library
    randomgen = np.random.default_rng(44)  # Use a seed value so I get consistent values when I re-run the function

    # Create a unique list of sale_id values to serve as the primary key
    sale_ids = list(range(1, 1000001))

    # Generate values for r_id, t_id and c_id
    r_ids = randomgen.choice(retailers_dataframe['r_id'], size=1000000)
    t_ids = randomgen.choice(inventory_dataframe['t_id'], size=1000000)
    c_ids = randomgen.integers(1000000, 9999999, size=1000000)  # I want all customer IDs to be seven-digit integers

    # Generate sale_date assuming the sales have occurred over the past five years
    start_date = pd.to_datetime('2018-07-10')
    total_days = (pd.to_datetime('2023-07-10') - start_date).days  # Calculates total number b/t start and end dates
    sale_dates = start_date + pd.to_timedelta(randomgen.integers(0, total_days, size=1000000), unit='d')

    # Generate quantity of tools sold in each sale as a random integer between 1 and 20
    quantities = randomgen.integers(1, 21, size=1000000)

    # Fetch c_price based on t_id
    price_dict = pd.Series(inventory_dataframe.c_price.values, index=inventory_dataframe.t_id).to_dict()
    c_prices = [price_dict[t_id] for t_id in t_ids]  # Some simple list comprehension

    # Now we can create and populate the 'sales' dataframe
    sales = pd.DataFrame({
        'sale_id': sale_ids,
        'r_id': r_ids,
        'c_id': c_ids,
        'sale_date': sale_dates,
        't_id': t_ids,
        'quantity': quantities,
        'c_price': c_prices
    })

    return sales


sales = construct_sales_dataframe(inventory, retailers)

# Debugging - making sure things look okay
# print(sales.head(50))

"""
For the last step of dataframe construction, I'll generate the customers dataframe. This will involve selecting the
distinct customer id values from the sales database and then using the Faker library to randomly generate names and 
mailing addresses. I also decided that I wanted my customers to be classified with specific proportions - I want 50%
private customers (type 'P') 35% business customers (type 'B') and 15% government customers (type 'G'). So I'll weight
that customer type generation code accordingly.
"""
def construct_customers_dataframe(sales_dataframe):
    # I'm going to use the Faker library to generate fake names and mailing addresses
    fake = fk()

    # First, fetch the *distinct* list of c_id values from the sales dataframe
    # Fetching a distinct list means the list will be unique and can serve as a primary key
    distinct_c_ids = sales_dataframe['c_id'].unique()

    # Then, initialize a dataframe object with those distinct customer id values
    customers = pd.DataFrame(distinct_c_ids, columns=['c_id'])

    # Use the Faker library to generate fake names and addresses and append the columns
    customers['c_name'] = [fake.name() for _ in range(len(customers))]  # As many names as there are distinct c_ids
    customers['c_address'] = [fake.address().replace('\n', ', ') for _ in range(len(customers))]  # Same for addresses

    # Generate customer type values based on chosen proportions and append the column
    choices = ['P', 'B', 'G']
    probabilities = [0.5, 0.35, 0.15]  # Defining desired proportions for each type, see comment above
    customers['c_type'] = np.random.choice(choices, size=len(customers), p=probabilities)  # Reference total # of c_ids

    # Return the dataframe
    return customers


customers = construct_customers_dataframe(sales)

"""
Now that I have all of my dataframes, I need a function that will pull each dataframe's data and convert the contents
into a SQL-compatible INSERT INTO code-block. If the function I write is flexible enough, I should be able to call it
for each dataframe and move fairly quickly.
"""
def generate_sql_inserts(dataframe, tablename, filepath):
    # Open the file path in write mode
    with open(filepath, 'w') as file:
        # Write the first INSERT INTO statement
        file.write(f"INSERT INTO {tablename} VALUES\n")

        # Get the number of rows in the dataframe
        num_rows = len(dataframe.index)

        # Iterate over each row in the dataframe
        for index, row in dataframe.iterrows():
            # Use fillna() to replace all NaN values with 'NULL'. If the value is 'NULL', it will remain 'NULL'
            # If the value is a string, surround it with double quotes
            # I can do this with a lambda function
            formatted_row = row.fillna('NULL').apply(
                lambda x: 'NULL' if x == 'NULL' else (f'"{x}"' if isinstance(x, str) else x)
            ).tolist()

            # Format the date values to YYYY-MM-DD and enclose them in quotes
            formatted_row = [f'"{x.strftime("%Y-%m-%d")}"' if isinstance(x, pd.Timestamp) else x for x in formatted_row]

            # Convert the formatted row to a string, separate fields with a comma-space
            line = "(" + ", ".join(str(x) for x in formatted_row) + ")"

            # Write each iterated line to the file with appropriate comma placement
            if index == num_rows - 1:
                # If it's the last row-insert line, remove comma and keep the newline character
                file.write(line.rstrip(",") + ";\n")
            else:
                # If it's not the last row-insert line, include comma *and* newline character
                file.write(line + ",\n")


generate_sql_inserts(manufacturers, 'manufacturers', 'load_manufacturers_data.sql')  # Manufacturers table

generate_sql_inserts(build, 'build', 'load_build_data.sql')  # Build table

generate_sql_inserts(tools, 'tools', 'load_tools_data.sql')  # Tools table

generate_sql_inserts(comprise, 'comprise', 'load_comprise_data.sql')  # Comprise table

generate_sql_inserts(orders, 'orders', 'load_orders_data.sql')  # Orders table

generate_sql_inserts(place, 'place', 'load_place_data.sql')  # Place table

generate_sql_inserts(retailers, 'retailers', 'load_retailers_data.sql')  # Retailers table

generate_sql_inserts(stock, 'stock', 'load_stock_data.sql')  # Stock table

generate_sql_inserts(inventory, 'inventory', 'load_inventory_data.sql')  # Inventory table

generate_sql_inserts(sales, 'sales', 'load_sales_data.sql')  # Sales table

generate_sql_inserts(customers, 'customers', 'load_customers_data.sql')  # Customers table
