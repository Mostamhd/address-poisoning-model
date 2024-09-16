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
OUTPUT_FILE = 'address_poisoning_transactions.csv'
CHECKPOINT_FILE = 'address_poisoning_transactions_checkpoint.txt'

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

def get_address_erc20_transactions(address_id):
    """
    returns a list of all the erc20 transactions IDs for an address  """

    cursor = db_connection.cursor(dictionary=True)
    # Query for getting all erc20 transaction 
    query = """
        SELECT 
            t.tx_id,
        FROM 
            erc20_transfer t
        WHERE 
            t.address_id = %s;
    """
    cursor.execute(query, (address_id))
    results = cursor.fetchall()
    cursor.close()
    return results

def get_tx_erc20_transfers(tx_id):
    """
    returns a list of all the erc20 transactions IDs for an address  """

    cursor = db_connection.cursor(dictionary=True)
    # Query for getting all erc20 transaction 
    query = """
        SELECT 
        tx.hash AS transaction_hash,
        tx.block_id,
        tx.total_eth_transfer As tx_total_eth_transfer,
        tx.total_erc20_transfer As tx_total_erc20_transfers,
        tx.total_internal_transfer As tx_total_internal_transfer,
        tx.total_deployment As tx_total_deployment,
        tx.gas_price_in_wei,
        tx.gas_used,
        tx.base_fee,
        tx.max_fee,
        tx.priority_fee,
        
        input_address.hash AS erc20_tx_from_address,
        output_address.hash AS erc20_tx_to_address,
        t.index_in_tx,
        t.crypto_amount AS erc20_transfer_amount,
        c.name AS erc20_contract_name,
        c.symbol AS contract_symbol,
        c.total_transfer,
        c.total_supply,
        c.circulating_supply
        FROM 
            address_address_erc20_transaction t
        JOIN 
            contract c ON t.contract_id = c.contract_id
        JOIN 
            transaction tx ON t.tx_id = tx.id
        JOIN 
            address input_address ON t.input = input_address.id  
        JOIN 
            address output_address ON t.output = output_address.id  

        WHERE
            t.tx_id = %s;
        """
    cursor.execute(query, (tx_id))
    results = cursor.fetchall()
    cursor.close()
    return results

def get_erc20_transfers_tx_creator(tx_id):
    cursor = db_connection.cursor(dictionary=True)
    query = """
            SELECT a.hash
            FROM address_transaction atx
            JOIN address a ON atx.address_id = a.id
            WHERE atx.tx_id = %s and atx.direction = 0;
            """
    cursor.execute(query, (tx_id,))
    result = cursor.fetchone()
    cursor.close()
    return result['hash'] if result else None

def get_erc20_transfers_tx_reciever(tx_id):
    cursor = db_connection.cursor(dictionary=True)
    query = """
            SELECT a.hash
            FROM address_transaction atx
            JOIN address a ON atx.address_id = a.id
            WHERE atx.tx_id = %s and atx.direction = 1;
            """
    cursor.execute(query, (tx_id,))
    result = cursor.fetchone()
    cursor.close()
    return result['hash'] if result else None

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
    df = pd.read_csv('address_poisoning_addresses_list.csv')

    # Get the last processed index
    last_index = read_checkpoint()

    # Total number of addresses
    total_addresses = len(df)

    # Open the output file in append mode
    with open(OUTPUT_FILE, 'a') as output_file:
        # Write headers if the file is empty
        if os.path.getsize(OUTPUT_FILE) == 0:
            headers = ["tx_from_address", "tx_to_address", "transaction_hash", 'block_id', 'tx_total_eth_transfer', 'tx_total_erc20_transfers', 'tx_total_internal_transfer', 'tx_total_deployment', 'gas_price_in_wei','gas_used','base_fee','max_fee','priority_fee', "erc20_tx_from_address", "erc20_tx_to_address", "index_in_tx", "erc20_transfer_amount", "erc20_contract_name", "contract_symbol", "total_transfer", "total_supply", "circulating_supply"]
        

            output_file.write(','.join(headers) + '\n')

        # Iterate over each row
        for index in range(last_index, total_addresses):
            row = df.iloc[index]
            address = row['Address']
            currency = row['Currency']
            
            address_id = get_address_id(address)
            if address_id:
                erc20_transactions = get_address_erc20_transactions(address_id)
                num_transactions = len(erc20_transactions)
                print_progress(index, total_addresses, num_transactions, address)
                for erc20_transactions_tx in erc20_transactions:
                    erc20_transaction_trasnfers = get_tx_erc20_transfers(erc20_transactions_tx)
                    
                    main_tx_creator = get_erc20_transfers_tx_creator(erc20_transactions_tx)
                    main_tx_reciever = get_erc20_transfers_tx_reciever(erc20_transactions_tx)

                    for tx in erc20_transaction_trasnfers:
                        
                        tx_metadata = {
                            'tx_from_address': main_tx_creator,
                            'tx_to_address': main_tx_reciever,
                            'transaction_hash': tx['transaction_hash'],
                            'block_id': tx['block_id'],
                            'tx_total_eth_transfer': tx['tx_total_eth_transfer'],
                            'tx_total_erc20_transfers': tx['tx_total_erc20_transfers'],
                            'tx_total_internal_transfer': tx['tx_total_internal_transfer'],
                            'tx_total_deployment': tx['tx_total_deployment'],
                            'gas_price_in_wei': tx['gas_price_in_wei'],
                            'gas_used': tx['gas_used'],
                            'base_fee': tx['base_fee'],
                            'max_fee': tx['max_fee'],
                            'priority_fee': tx['priority_fee'],
                            'erc20_tx_from_address': tx['erc20_tx_from_address'],
                            'erc20_tx_to_address': tx['erc20_tx_to_address'],
                            'index_in_tx': tx['index_in_tx'],
                            'erc20_transfer_amount': tx['erc20_transfer_amount'],
                            'erc20_contract_name': tx['erc20_contract_name'],
                            'contract_symbol': tx['contract_symbol'],
                            'total_transfer': tx['total_transfer'],
                            'total_supply': tx['total_supply'],
                            'circulating_supply': tx['circulating_supply']
                        }

                        # Write the transaction metadata to the output file
                        output_file.write(','.join(map(str, tx_metadata.values())) + '\n')
            else:
                print_progress(index, total_addresses, 0, address)

            # Write the checkpoint after each iteration
            write_checkpoint(index)
        
            # Sleep to avoid overwhelming the database
            time.sleep(0.1)

    print("Processing complete.")

if __name__ == "__main__":
    main()
