from pymongo import MongoClient
from bson import ObjectId, DBRef
import urllib.parse
import datetime
from tqdm import tqdm


def account_balance_checker(cursor, user, file):
    document_list = list(cursor)
    error_messages = []

    username = user["name"]
    user_id = user["_id"]
    user_phone = user["phone"]

    for current_document, next_document in zip(document_list, document_list[1:]):
        current_id = current_document["_id"]
        next_id = next_document["_id"]

        balance_after = eval(current_document["balanceAfter"])
        balance_before = eval(current_document["balanceBefore"])
        balance_before_next = eval(next_document["balanceBefore"])

        if balance_after != balance_before_next:
            error_message = f"BalanceMismatchError: The user {username} with ID: {user_id} and Phone number: {user_phone} has balance mismatch between on transaction ID: {current_id} and transaction ID: {next_id}\n"
            error_messages.append(error_message)

        if balance_after == balance_before:
            error_message = f"BalanceConsistencyError: The user {username} with ID: {user_id} and Phone number: {user_phone} has his balance before and after matching on transaction ID: {current_id}\n"
            error_messages.append(error_message)

    # Write error messages to the file
    for error_message in error_messages:
        file.writelines(error_message)


try:
    # Create a MongoClient object with the provided connection string
    client = MongoClient("mongodb://" + urllib.parse.quote_plus("@fric") + ":" + urllib.parse.quote_plus(
        "Test@2023.") + "@162.255.87.124:60000/bjft_finance_db?retryWrites=true&w=majority")

    # Access the database
    db = client['bjft_finance_db']

    # Display success message
    print("Connection to MongoDB database established successfully.")

    # Get the current date and time
    current_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = f"balance_check_results_{current_date}.txt"

    # Access specific collections
    account_jour_db = db['clt_accounting_journal']
    user_db = db['clt_user']

    # Define the filter string
    user_filter = {"currency": {"$in": ["CAD", "USD", "EUR"]}}

    # Fetch all documents from the collection
    user_list = user_db.find(user_filter)

    with open(output_file, "a") as file:
        for user in user_list:
            # Collect all the user accounts
            accountWrapper = user.get("accountWrapper", [])
            walletaccount_ids = [
                accountWrapper[key].id for key in accountWrapper if isinstance(accountWrapper[key], DBRef)]

            total_wallet_accounts = len(walletaccount_ids)

            with tqdm(total=total_wallet_accounts, desc="Checking wallet accounts", unit="account") as progress_bar:
                for walletaccount in walletaccount_ids:
                    account_filter = {"xuser": '6215ecae6550937a58facb43',
                                      "walletAccount.$id": walletaccount}
                    account_jour = account_jour_db.find(
                        account_filter).sort("createdAt")

                    account_balance_checker(account_jour, user, file)

                    # Update progress bar
                    progress_bar.update()

        file.write("Balance check completed.\n")
        print("Balance check completed")

    # Close the MongoDB client connection
    client.close()

except Exception as e:
    # Handle any exceptions that occur during database connection or operations
    print("An error occurred:", str(e))
