import pandas as pd
import mysql.connector
import time
import os

# Database connection details
DB_HOST = "192.168.255.83"
DB_USER = "moustafa.mahmoud"
DB_PASSWORD = "$*5f9fBvwzR!yf"
DB_NAME = "eth"

# Output files
OUTPUT_FILE = "address_poisoning_transactions.csv"
CHECKPOINT_FILE = "address_poisoning_transactions_checkpoint.txt"

# Connect to the MySQL database
db_connection = mysql.connector.connect(
    host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
)

# Tracking data for additional features
seen_pairs = set()  # For is_repeat_counterparty
counterparty_counts = {}  # For counterparty_tx_count
last_tx_time = {}  # For burst_flag

# Functions to retrieve address and transaction data
def get_address_id(address_hash):
    cursor = db_connection.cursor(dictionary=True)
    query = "SELECT id FROM address WHERE hash = %s"
    cursor.execute(query, (address_hash,))
    result = cursor.fetchone()
    cursor.close()
    return result["id"] if result else None

def get_address_erc20_transactions(address_id):
    cursor = db_connection.cursor(dictionary=True)
    query = "SELECT tx_id FROM erc20_transfer WHERE address_id = %s and direction = 1;"
    cursor.execute(query, (address_id,))
    results = cursor.fetchall()
    cursor.close()
    return results

def get_tx_erc20_transfers(tx_id):
    cursor = db_connection.cursor(dictionary=True)
    query = """
        SELECT 
            tx.hash AS transaction_hash,
            tx.block_id,
            tx.block_timestamp,
            tx.total_eth_transfer AS tx_total_eth_transfer,
            tx.total_erc20_transfer AS tx_total_erc20_transfers,
            tx.total_internal_transfer AS tx_total_internal_transfer,
            tx.total_deployment AS tx_total_deployment,
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
    cursor.execute(query, (tx_id,))
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
    return result["hash"] if result else None

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
    return result["hash"] if result else None

# Feature calculation functions
def is_repeat_counterparty(from_address, to_address):
    pair = (from_address, to_address)
    if pair in seen_pairs:
        return 1
    else:
        seen_pairs.add(pair)
        return 0

def get_counterparty_tx_count(from_address, to_address):
    pair = (from_address, to_address)
    if pair in counterparty_counts:
        counterparty_counts[pair] += 1
    else:
        counterparty_counts[pair] = 1
    return counterparty_counts[pair]

def is_burst_transaction(current_time, from_address, to_address):
    pair = (from_address, to_address)
    burst_threshold = 5 * 60  # 5 minutes in seconds
    if pair in last_tx_time:
        time_diff = current_time - last_tx_time[pair]
        burst_flag = 1 if time_diff < burst_threshold else 0
    else:
        burst_flag = 0
    last_tx_time[pair] = current_time
    return burst_flag

# Checkpointing functions
def write_checkpoint(index):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(index))

def read_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def print_progress(index, total, num_transactions, address):
    progress = (index / total) * 100
    print(f"Processing address {index + 1}/{total} ({progress:.2f}%), Address: {address}, Transactions: {num_transactions}")

def main():
    # Load the CSV file containing the known phishing addresses
    df = pd.read_csv("dataset/address_poisoning_addresses_list.csv")

    last_index = read_checkpoint()
    total_addresses = len(df)

    with open(OUTPUT_FILE, "a") as output_file:
        # Write headers if the file is empty
        if os.path.getsize(OUTPUT_FILE) == 0:
            headers = [
                "tx_from_address", "tx_to_address", "transaction_hash", "block_id",
                "block_timestamp", "tx_total_eth_transfer", "tx_total_erc20_transfers",
                "tx_total_internal_transfer", "tx_total_deployment", "gas_price_in_wei",
                "gas_used", "base_fee", "max_fee", "priority_fee", "erc20_tx_from_address",
                "erc20_tx_to_address", "index_in_tx", "erc20_transfer_amount", "erc20_contract_name",
                "contract_symbol", "total_transfer", "total_supply", "circulating_supply",
                "is_repeat_counterparty", "counterparty_tx_count", "burst_flag"
            ]
            output_file.write(",".join(headers) + "\n")

        for index in range(last_index, total_addresses):
            row = df.iloc[index]
            address = row["Address"]
            address_id = get_address_id(address)

            if address_id:
                erc20_transactions = get_address_erc20_transactions(address_id)
                print_progress(index, total_addresses, len(erc20_transactions), address)

                for erc20_transaction in erc20_transactions:
                    transfers = get_tx_erc20_transfers(erc20_transaction["tx_id"])
                    tx_creator = get_erc20_transfers_tx_creator(erc20_transaction["tx_id"])
                    tx_receiver = get_erc20_transfers_tx_reciever(erc20_transaction["tx_id"])

                    for tx in transfers:
                        # Additional features
                        current_time = tx["block_timestamp"]
                        tx["is_repeat_counterparty"] = is_repeat_counterparty(tx_creator, tx_receiver)
                        tx["counterparty_tx_count"] = get_counterparty_tx_count(tx_creator, tx_receiver)
                        tx["burst_flag"] = is_burst_transaction(current_time, tx_creator, tx_receiver)

                        tx_metadata = {
                            "tx_from_address": tx_creator,
                            "tx_to_address": tx_receiver,
                            "transaction_hash": tx["transaction_hash"],
                            "block_id": tx["block_id"],
                            "block_timestamp": tx["block_timestamp"],
                            "tx_total_eth_transfer": tx["tx_total_eth_transfer"],
                            "tx_total_erc20_transfers": tx["tx_total_erc20_transfers"],
                            "tx_total_internal_transfer": tx["tx_total_internal_transfer"],
                            "tx_total_deployment": tx["tx_total_deployment"],
                            "gas_price_in_wei": tx["gas_price_in_wei"],
                            "gas_used": tx["gas_used"],
                            "base_fee": tx["base_fee"],
                            "max_fee": tx["max_fee"],
                            "priority_fee": tx["priority_fee"],
                            "erc20_tx_from_address": tx["erc20_tx_from_address"],
                            "erc20_tx_to_address": tx["erc20_tx_to_address"],
                            "index_in_tx": tx["index_in_tx"],
                            "erc20_transfer_amount": f"{tx['erc20_transfer_amount']:.10f}",
                            "erc20_contract_name": tx["erc20_contract_name"],
                            "contract_symbol": tx["contract_symbol"],
                            "total_transfer": tx["total_transfer"],
                            "total_supply": tx["total_supply"].decode("utf-8") if tx["total_supply"] else "",
                            "circulating_supply": f"{tx['circulating_supply']:.10f}",
                            "is_repeat_counterparty": tx["is_repeat_counterparty"],
                            "counterparty_tx_count": tx["counterparty_tx_count"],
                            "burst_flag": tx["burst_flag"],
                                                     
                        }

                        # Write the transaction metadata to the output file
                        output_file.write(",".join(map(str, tx_metadata.values())) + "\n")

            else:
                print_progress(index, total_addresses, 0, address)

            # Write checkpoint after each address to resume later if interrupted
            write_checkpoint(index)

            # Optional delay to avoid overwhelming the database
            time.sleep(0.1)

    print("Processing complete.")

if __name__ == "__main__":
    main()
