import pandas as pd
import mysql.connector
import time
import os

# Database connection details
DB_HOST = '192.168.255.83'
DB_USER = 'moustafa.mahmoud'
DB_PASSWORD = '$*5f9fBvwzR!yf'
DB_NAME = 'eth' 

# Output files
OUTPUT_FILE = 'transaction_metadata.csv'
CHECKPOINT_FILE = 'checkpoint.txt'

# Connect to the MySQL database
db_connection = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

def get_address_id(address_hash):
    cursor = db_connection.cursor(dictionary=True)
    query = "SELECT id FROM address WHERE hash = %s"
    cursor.execute(query, (address_hash,))
    result = cursor.fetchone()
    cursor.close()
    return result['id'] if result else None

def get_address_hash(address_id):
    cursor = db_connection.cursor(dictionary=True)
    query = "SELECT hash FROM address WHERE id = %s"
    cursor.execute(query, (address_id,))
    result = cursor.fetchone()
    cursor.close()
    return result['hash'] if result else None

def get_erc20_transactions(address_id):
    """
    columns for this query :
    id hash tx_index gas_price_in_wei gas_used block_id block_timestamp total_eth_transfer total_deployment total_erc20_transfer type status base_fee max_fee priority_fee input reverse_hash total_erc721_transfer total_internal_transfer input output tx_id index_in_tx contract_id crypto_amount
    """

    cursor = db_connection.cursor(dictionary=True)
    # Query for getting all erc20 transaction 
    query = """
        SELECT * 
        FROM transaction t 
        JOIN address_address_erc20_transaction atx ON t.id = atx.tx_id 
        WHERE atx.output = %s OR atx.input = %s
        ORDER BY t.block_timestamp ASC 
    """
    cursor.execute(query, (address_id,address_id))
    results = cursor.fetchall()
    cursor.close()
    return results

def get_eth_transactions(address_id):
    """
    columns for this query :
    id hash tx_index gas_price_in_wei gas_used block_id block_timestamp total_eth_transfer total_deployment total_erc20_transfer type status base_fee max_fee priority_fee input reverse_hash total_erc721_transfer total_internal_transfer input output tx_id crypto_amount
    """

    cursor = db_connection.cursor(dictionary=True)
    # Query for getting eth transactions
    query = """
        SELECT * 
        FROM transaction t 
        JOIN address_address_transaction atx ON t.id = atx.tx_id 
        WHERE atx.output = %s OR atx.input = %s
        ORDER BY t.block_timestamp ASC 
    """

    cursor.execute(query, (address_id,address_id))
    results = cursor.fetchall()
    cursor.close()
    return results

def write_checkpoint(index):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(str(index))

def read_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return int(f.read().strip())
    return 0

def print_progress(index, total, num_transactions, address):
    progress = (index / total) * 100
    print(f"Processing address {index + 1}/{total} ({progress:.2f}%), Address: {address}, Transactions: {num_transactions}")

def main():
    # Load the CSV file
    df = pd.read_csv('address_poisoning.csv')

    # Get the last processed index
    last_index = read_checkpoint()

    # Total number of addresses
    total_addresses = len(df)

    # Open the output file in append mode
    with open(OUTPUT_FILE, 'a') as output_file:
        # Write headers if the file is empty
        if os.path.getsize(OUTPUT_FILE) == 0:
            headers = ['from', 'to', 'hash', 'tx_index', 'gas_price_in_wei', 'gas_used', 'block_id', 'block_timestamp', 'total_eth_transfer', 'total_deployment', 'total_erc20_transfer', 'status', 'base_fee', 'max_fee', 'priority_fee', 'input', 'reverse_hash', 'total_erc721_transfer', 'total_internal_transfer', 'index_in_tx', 'contract_id', 'crypto_amount', 'type']
            output_file.write(','.join(headers) + '\n')

        # Iterate over each row
        for index in range(last_index, total_addresses):
            row = df.iloc[index]
            address = row['Address']
            currency = row['Currency']
            
            if currency == 'ETH':
                address_id = get_address_id(address)
                if address_id:
                    erc20_transactions = get_erc20_transactions(address_id)
                    eth_transactions = get_eth_transactions(address_id)
                    transactions = erc20_transactions + eth_transactions
                    num_transactions = len(transactions)
                    print_progress(index, total_addresses, num_transactions, address)
                    for tx in transactions:
                        # tx_metadata = {
                        #    #TODO in here put the entire object from the tx object the first two columns `from` and `to` their values will be retrieved from the `input` and `output` values in the tx object respectively, and using that value we will call this function `this.get_address_hash()` if the value for the `input` or the `output` is the same as the address_id value then its value will be = address otherwise we call the this.get_address_hash() function with the `input` or the `output` value 
                        # }
                        tx_metadata = {
                            'from': address if tx['input'] == address_id else get_address_hash(tx['input']),
                            'to': address if tx['output'] == address_id else get_address_hash(tx['output']),
                            'hash': tx['hash'],
                            'tx_index': tx['tx_index'],
                            'gas_price_in_wei': tx['gas_price_in_wei'],
                            'gas_used': tx['gas_used'],
                            'block_id': tx['block_id'],
                            'block_timestamp': tx['block_timestamp'],
                            'total_eth_transfer': tx['total_eth_transfer'],
                            'total_deployment': tx['total_deployment'],
                            'total_erc20_transfer': tx['total_erc20_transfer'],
                            'status': tx['status'],
                            'base_fee': tx['base_fee'],
                            'max_fee': tx['max_fee'],
                            'priority_fee': tx['priority_fee'],
                            'input': tx['input'],
                            'reverse_hash': tx['reverse_hash'],
                            'total_erc721_transfer': tx['total_erc721_transfer'],
                            'total_internal_transfer': tx['total_internal_transfer'],
                            'index_in_tx': tx['index_in_tx'] if tx in erc20_transactions else None,
                            'contract_id': tx['contract_id'] if tx in erc20_transactions else None,
                            'crypto_amount': tx['crypto_amount'],
                            "type": 'erc20' if tx in erc20_transactions else 'eth'
                        }

                        # Write the transaction metadata to the output file
                        output_file.write(','.join(map(str, tx_metadata.values())) + '\n')
                else:
                    print_progress(index, total_addresses, 0, address)
            else:
                print_progress(index, total_addresses, 0, address)

            # Write the checkpoint after each iteration
            write_checkpoint(index)
        
            # Sleep to avoid overwhelming the database
            time.sleep(0.1)

    print("Processing complete.")

if __name__ == "__main__":
    main()
