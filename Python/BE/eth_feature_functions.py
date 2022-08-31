### ETH Features Creation From Raw Data ###
from datetime import datetime

def unflatten(dictionary, delimiter):
    resultDict = dict()
    for key, value in dictionary.items():
        parts = key.split(delimiter)
        d = resultDict
        for part in parts[:-1]:
            if part not in d:
                d[part] = dict()
            d = d[part]
        d[parts[-1]] = value
    return resultDict


def get_eth_price(address: str, chain: str, blockNumber: int, apiKey: str) -> int:
    """Gets the ETH price of the address' token at the moment this function is called."""
    
    return request_token_info(address, chain, blockNumber, apiKey)["nativePrice"]["value"]


def get_eth_balance(address: str) -> int:
    """Requests balance of the given address' ETH from the Moralis API."""
    
    moralis_url=f"https://deep-index.moralis.io/api/v2/{address}/balance?chain=eth"
    h_values = {'X-API-KEY': apiKey}
    r = requests.get(moralis_url, headers=h_values)

    return r.json()["balance"]

def get_processed_eth_txn_list_analytics(txnList: list, address: str) -> dict:
        """Returns analytic data from the processed ethereum transaction list"""

        if len(txnList) == 0: return {} # If the list is empty, return an empty dictionary

        analytics = {
            "received": {
                "minValue": None,
                "maxValue": None,
                "avgValue": None
            },
            "sent": {
                "minValue": None,
                "maxValue": None,
                "avgValue": None
            },
            "sentToContract": {
                "minValue": 0,
                "maxValue": 0,
                "avgValue": 0
            },
            "total": {
                "transactions": len(txnList),
                "etherSent": 0,
                "etherReceived": 0,
                "etherSentToContract": 0,
                "sentTxn": 0,
                "receivedTxn": 0,
                "sentToContractTxn": 0,
                "uniqueReceivedFromAddresses": 0,
                "uniqueSentToAddresses": 0
            },
            "time": {
                "avgBetweenSentTxn": None,
                "avgBetweenReceivedTxn": None,
                "avgBetweenTxn": None,
                "diffBetweenFirstAndLastSent": None,
                "diffBetweenFirstAndLastReceived": None,
                "diffBetweenFirstAndLastTxn": None,
            }
        }

        receivedFromAddresses = []
        sentToAddresses = []

        firstReceivedTxnTime = None
        firstSentTxnTime = None
        firstTxnTime = None

        lastReceivedTxnTime = None
        lastSentTxnTime = None
        lastTxnTime = None

        totalTimeBetweenSentTxn = 0
        totalTimeBetweenReceivedTxn = 0
        totalTimeBetweenTxn = 0

        for txn in txnList: # Loop through all transactions 
            
            if firstTxnTime == None: # Track First Transaction Time
                firstTxnTime = txn["TrxDatetime"]
            elif txn["TrxDatetime"] < firstTxnTime:
                firstTxnTime = txn["TrxDatetime"]

            if lastTxnTime == None: # Track Last Transaction Time
                lastTxnTime = txn["TrxDatetime"]
            elif txn["TrxDatetime"] > lastTxnTime:
                lastTxnTime = txn["TrxDatetime"]

            ethValue = txn["TrxAmount"] # Get the ETH value of the transaction

            if txn["TrxToWallet"] == address: # Received Transactions
                analytics["received"]["minValue"] = ethValue if analytics["received"]["minValue"] == None else min(analytics["received"]["minValue"], ethValue)
                analytics["received"]["maxValue"] = ethValue if analytics["received"]["maxValue"] == None else max(analytics["received"]["maxValue"], ethValue)
                analytics["total"]["etherReceived"] += ethValue
                analytics["total"]["receivedTxn"] += 1
                
                if txn["TrxFromWallet"] not in receivedFromAddresses:
                    receivedFromAddresses.append(txn["TrxFromWallet"])
                    analytics["total"]["uniqueReceivedFromAddresses"] += 1

                if firstReceivedTxnTime == None: # Track First Transaction Time
                    firstReceivedTxnTime = txn["TrxDatetime"]
                elif txn["TrxDatetime"] < firstReceivedTxnTime:
                    firstReceivedTxnTime = txn["TrxDatetime"]
                
                if lastReceivedTxnTime == None: # Track Last Transaction Time
                    lastReceivedTxnTime = txn["TrxDatetime"]
                elif txn["TrxDatetime"] > lastReceivedTxnTime:
                    lastReceivedTxnTime = txn["TrxDatetime"]

            else: # Sent Transactions
                analytics["sent"]["minValue"] = ethValue if analytics["sent"]["minValue"] == None else min(analytics["sent"]["minValue"], ethValue)
                analytics["sent"]["maxValue"] = ethValue if analytics["sent"]["maxValue"] == None else max(analytics["sent"]["maxValue"], ethValue)
                analytics["total"]["etherSent"] += ethValue
                analytics["total"]["sentTxn"] += 1
                
                if txn["TrxToWallet"] not in sentToAddresses:
                    sentToAddresses.append(txn["TrxToWallet"])
                    analytics["total"]["uniqueSentToAddresses"] += 1

                if firstSentTxnTime == None: # Track First Transaction Time
                    firstSentTxnTime = txn["TrxDatetime"]
                elif txn["TrxDatetime"] < firstSentTxnTime:
                    firstSentTxnTime = txn["TrxDatetime"]

                if lastSentTxnTime == None: # Track Last Transaction Time
                    lastSentTxnTime = txn["TrxDatetime"]
                elif txn["TrxDatetime"] > lastSentTxnTime:
                    lastSentTxnTime = txn["TrxDatetime"]

                if txn["TrxAmount"] == 0: # Sent Transactions to Contracts
                    analytics["sentToContract"]["minValue"] = ethValue if analytics["sentToContract"]["minValue"] == 0 else min(analytics["sentToContract"]["minValue"], ethValue)
                    analytics["sentToContract"]["maxValue"] = ethValue if analytics["sentToContract"]["maxValue"] == 0 else max(analytics["sentToContract"]["maxValue"], ethValue)
                    analytics["total"]["etherSentToContract"] += ethValue
                    analytics["total"]["sentToContractTxn"] += 1

        # Calculate Average Values
        if analytics["total"]["receivedTxn"] != 0: analytics["received"]["avgValue"] = analytics["total"]["etherReceived"] / analytics["total"]["receivedTxn"]
        if analytics["total"]["sentTxn"] != 0: analytics["sent"]["avgValue"] = analytics["total"]["etherSent"] / analytics["total"]["sentTxn"]
        if analytics["total"]["sentToContractTxn"] != 0: analytics["sentToContract"]["avgValue"] = analytics["total"]["etherSentToContract"] / analytics["total"]["sentToContractTxn"]
        
        # Avoid Errors for "NoneType" variables
        if lastReceivedTxnTime == None: lastReceivedTxnTime = lastSentTxnTime
        if lastSentTxnTime == None: lastSentTxnTime = lastReceivedTxnTime
        if firstReceivedTxnTime == None: firstReceivedTxnTime = firstSentTxnTime
        if firstSentTxnTime == None: firstSentTxnTime = firstReceivedTxnTime

        # Calculate Time Differences
        if analytics["total"]["receivedTxn"] != 0: analytics["time"]["diffBetweenFirstAndLastReceived"] = lastReceivedTxnTime - firstReceivedTxnTime
        if analytics["total"]["sentTxn"] != 0: analytics["time"]["diffBetweenFirstAndLastSent"] = lastSentTxnTime - firstSentTxnTime
        if analytics["total"]["transactions"] != 0: analytics["time"]["diffBetweenFirstAndLastTxn"] = lastSentTxnTime - firstSentTxnTime

        # Calculate Average Time Between Transactions
        if analytics["total"]["sentTxn"] != 0: analytics["time"]["avgBetweenSentTxn"] = analytics["time"]["diffBetweenFirstAndLastSent"] / analytics["total"]["sentTxn"]
        if analytics["total"]["receivedTxn"] != 0: analytics["time"]["avgBetweenReceivedTxn"] = analytics["time"]["diffBetweenFirstAndLastReceived"] / analytics["total"]["receivedTxn"]
        if len(txnList) != 0: analytics["time"]["avgBetweenTxn"] = analytics["time"]["diffBetweenFirstAndLastTxn"] / len(txnList)

        # Register wallet address
        analytics["walletAddress"] = address
            
        # Register current time
        analytics["currentTime"] = datetime.now()
            
        return analytics
    
    

def get_processed_erc20_txn_list_analytics(txnList: list, address: str) -> dict:
        """Returns analytic data from the processed ethereum transaction list"""

        if len(txnList) == 0: return {} # If the list is empty, return an empty dictionary
        listLength = len(txnList)

        analytics = {
            "received": {
                "minValue": None,
                "maxValue": None,
                "avgValue": None
            },
            "sent": {
                "minValue": None,
                "maxValue": None,
                "avgValue": None
            },
            "sentToContract": {
                "minValue": 0,
                "maxValue": 0,
                "avgValue": 0
            },
            "total": {
                "transactions": len(txnList),
                "etherSent": 0,
                "etherReceived": 0,
                "etherSentToContract": 0,
                "sentTxn": 0,
                "receivedTxn": 0,
                "sentToContractTxn": 0,
                "uniqueReceivedFromAddresses": 0,
                "uniqueSentToAddresses": 0
            },
            "time": {
                "avgBetweenSentTxn": None,
                "avgBetweenReceivedTxn": None,
                "avgBetweenTxn": None,
                "diffBetweenFirstAndLastSent": None,
                "diffBetweenFirstAndLastReceived": None,
                "diffBetweenFirstAndLastTxn": None,
            },
            "erc20": {
                "uniqueTokenAddressesSent": 0,
                "uniqueTokenAddressesReceived": 0,
                "mostSentTokenAddress": None,
                "mostReceivedTokenAddress": None,
                "uniqueUnknownValueTokens": 0,
                "transactionsWithUnknownValueTokens": 0
        }
        }

        receivedFromAddresses = []
        sentToAddresses = []
        uniqueTokenAddressesSent = []
        uniqueTokenAddressesReceived = []
        tokenAddressesSent = []
        tokenAddressesReceived = []
        uniqueUnknownValueTokens = []

        firstReceivedTxnTime = None
        firstSentTxnTime = None
        firstTxnTime = None

        lastReceivedTxnTime = None
        lastSentTxnTime = None
        lastTxnTime = None

        totalTimeBetweenSentTxn = 0
        totalTimeBetweenReceivedTxn = 0
        totalTimeBetweenTxn = 0
        transactionsWithUnknownValueTokens = 0

        sentTransactionsWithUnknownValueTokens = 0
        receivedTransactionsWithUnknownValueTokens = 0 
        sentToContractTransactionsWithUnknownValueTokens = 0

        for txn in txnList: # Loop through all transactions 
            foundTokenEthValue = False

            try: # Try to fetch ethereum value of token. If none is found, skip
                ethPrice = int(get_eth_price(txn["ERC20Info"]["tokenAddress"], "eth", txn["blockNumber"], moralisAPIKey)) / (10**18)
                tokenQuantity = txn["ERC20Info"]["eventInfo"]["tokenQuantity"] / (10**txn["ERC20Info"]["tokenDetails"]["decimals"])
                ethValue = ethPrice * tokenQuantity
                foundTokenEthValue = True
            except:
                if txn["ERC20Info"]["tokenAddress"] not in uniqueUnknownValueTokens:
                    uniqueUnknownValueTokens.append(txn["ERC20Info"]["tokenAddress"])
                transactionsWithUnknownValueTokens += 1

            if firstTxnTime == None: # Track First Transaction Time
                firstTxnTime = txn["timestamp"]
            elif txn["timestamp"] < firstTxnTime:
                firstTxnTime = txn["timestamp"]

            if lastTxnTime == None: # Track Last Transaction Time
                lastTxnTime = txn["timestamp"]
            elif txn["timestamp"] > lastTxnTime:
                lastTxnTime = txn["timestamp"] 

            # IMPORTANT NOTE: If ERC20 Transfer() is always executed between a wallet and a contract, then this whole part will never be used
            if txn["ERC20Info"]["eventInfo"]["transferReceiver"] == address: # Received Transactions
                if foundTokenEthValue:
                    analytics["received"]["minValue"] = ethValue if analytics["received"]["minValue"] == None else min(analytics["received"]["minValue"], ethValue)
                    analytics["received"]["maxValue"] = ethValue if analytics["received"]["maxValue"] == None else max(analytics["received"]["maxValue"], ethValue)
                    analytics["total"]["etherReceived"] += ethValue
                    analytics["total"]["receivedTxn"] += 1
                else:
                    receivedTransactionsWithUnknownValueTokens += 1

                
                if txn["sender"] not in receivedFromAddresses:
                    receivedFromAddresses.append(txn["sender"])
                    analytics["total"]["uniqueReceivedFromAddresses"] += 1

                if firstReceivedTxnTime == None: # Track First Transaction Time
                    firstReceivedTxnTime = txn["timestamp"]
                elif txn["timestamp"] < firstReceivedTxnTime:
                    firstReceivedTxnTime = txn["timestamp"]
                    
                if lastReceivedTxnTime == None: # Track Last Transaction Time
                    lastReceivedTxnTime = txn["timestamp"]
                elif txn["timestamp"] > lastReceivedTxnTime:
                    lastReceivedTxnTime = txn["timestamp"]

                if txn["ERC20Info"]["tokenAddress"] not in uniqueTokenAddressesReceived:
                    uniqueTokenAddressesReceived.append(txn["ERC20Info"]["tokenAddress"])

                tokenAddressesReceived.append(txn["ERC20Info"]["tokenAddress"])

            else: # Sent Transactions
                if foundTokenEthValue:
                    analytics["sent"]["minValue"] = ethValue if analytics["sent"]["minValue"] == None else min(analytics["sent"]["minValue"], ethValue)
                    analytics["sent"]["maxValue"] = ethValue if analytics["sent"]["maxValue"] == None else max(analytics["sent"]["maxValue"], ethValue)
                    analytics["total"]["etherSent"] += ethValue
                    analytics["total"]["sentTxn"] += 1
                else:
                    sentTransactionsWithUnknownValueTokens += 1
                    
                if txn["ERC20Info"]["eventInfo"]["transferReceiver"] not in sentToAddresses:
                    sentToAddresses.append(txn["ERC20Info"]["eventInfo"]["transferReceiver"])
                    analytics["total"]["uniqueSentToAddresses"] += 1
                        
                if firstSentTxnTime == None: # Track First Transaction Time
                    firstSentTxnTime = txn["timestamp"]
                elif txn["timestamp"] < firstSentTxnTime:
                    firstSentTxnTime = txn["timestamp"]

                if lastSentTxnTime == None: # Track Last Transaction Time
                    lastSentTxnTime = txn["timestamp"]
                elif txn["timestamp"] > lastSentTxnTime:
                    lastSentTxnTime = txn["timestamp"]

                if txn["ERC20Info"]["tokenAddress"] not in uniqueTokenAddressesSent:
                    uniqueTokenAddressesSent.append(txn["ERC20Info"]["tokenAddress"])

                tokenAddressesSent.append(txn["ERC20Info"]["tokenAddress"])

                if txn["ERC20Info"]["eventInfo"]["transferReceiverIsContract"] == True: # Sent Transactions to Contracts
                    if foundTokenEthValue:
                        analytics["sentToContract"]["minValue"] = ethValue if analytics["sentToContract"]["minValue"] == 0 else min(analytics["sentToContract"]["minValue"], ethValue)
                        analytics["sentToContract"]["maxValue"] = ethValue if analytics["sentToContract"]["maxValue"] == 0 else max(analytics["sentToContract"]["maxValue"], ethValue)
                        analytics["total"]["etherSentToContract"] += ethValue
                        analytics["total"]["sentToContractTxn"] += 1
                    else:
                        sentToContractTransactionsWithUnknownValueTokens += 1

        # Calculate Average Values
        if analytics["total"]["receivedTxn"] - receivedTransactionsWithUnknownValueTokens != 0: analytics["received"]["avgValue"] = analytics["total"]["etherReceived"] / (analytics["total"]["receivedTxn"] - receivedTransactionsWithUnknownValueTokens)
        if analytics["total"]["sentTxn"] - sentTransactionsWithUnknownValueTokens != 0: analytics["sent"]["avgValue"] = analytics["total"]["etherSent"] / (analytics["total"]["sentTxn"] - sentTransactionsWithUnknownValueTokens)
        if analytics["total"]["sentToContractTxn"] - sentToContractTransactionsWithUnknownValueTokens != 0: analytics["sentToContract"]["avgValue"] = analytics["total"]["etherSentToContract"] / (analytics["total"]["sentToContractTxn"] - sentToContractTransactionsWithUnknownValueTokens)
        
        # Avoid Errors for "NoneType" variables
        if lastReceivedTxnTime == None: lastReceivedTxnTime = lastSentTxnTime
        if lastSentTxnTime == None: lastSentTxnTime = lastReceivedTxnTime
        if firstReceivedTxnTime == None: firstReceivedTxnTime = firstSentTxnTime
        if firstSentTxnTime == None: firstSentTxnTime = firstReceivedTxnTime

        # Calculate Time Differences
        if analytics["total"]["receivedTxn"] != 0: analytics["time"]["diffBetweenFirstAndLastReceived"] = lastReceivedTxnTime - firstReceivedTxnTime
        if analytics["total"]["sentTxn"] != 0: analytics["time"]["diffBetweenFirstAndLastSent"] = lastSentTxnTime - firstSentTxnTime
        if analytics["total"]["transactions"] != 0: analytics["time"]["diffBetweenFirstAndLastTxn"] = max(lastReceivedTxnTime, lastSentTxnTime) - min(firstSentTxnTime, firstReceivedTxnTime)

        # Calculate Average Time Between Transactions
        if analytics["total"]["sentTxn"] != 0: analytics["time"]["avgBetweenSentTxn"] = analytics["time"]["diffBetweenFirstAndLastSent"] / analytics["total"]["sentTxn"]
        if analytics["total"]["receivedTxn"] != 0: analytics["time"]["avgBetweenReceivedTxn"] = analytics["time"]["diffBetweenFirstAndLastReceived"] / analytics["total"]["receivedTxn"]
        if listLength != 0: analytics["time"]["avgBetweenTxn"] = analytics["time"]["diffBetweenFirstAndLastTxn"] / listLength

        # Calculate unique ERC20 values
        analytics["erc20"]["uniqueTokenAddressesSent"] = len(uniqueTokenAddressesSent)
        analytics["erc20"]["uniqueTokenAddressesReceived"] = len(uniqueTokenAddressesReceived)
        
        def most_common(lst): # Find most common element
            try:
                return max(set(lst), key=lst.count)
            except ValueError:
                return None
        
        analytics["erc20"]["mostSentTokenAddress"] = most_common(tokenAddressesSent)
        analytics["erc20"]["mostReceivedTokenAddress"] = most_common(tokenAddressesReceived)
        
        # Unknown Tokens
        analytics["erc20"]["uniqueUnknownValueTokens"] = len(uniqueUnknownValueTokens)
        analytics["erc20"]["transactionsWithUnknownValueTokens"] = transactionsWithUnknownValueTokens
        
        # Register wallet address
        analytics["walletAddress"] = address
        
        # Register current time
        analytics["currentTime"] = datetime.now()
        
        return analytics
