### ETH Raw Data Functions from Web3 and Moralis APIs ###

def request_token_info(address: str, chain: str, blockNumber: int, apiKey: str):
    """Requests information of the given address' token from the Moralis API."""
    
    moralis_url=f"https://deep-index.moralis.io/api/v2/erc20/{address}/price?chain={chain}&to_block={blockNumber}"
    h_values = {'X-API-KEY': apiKey}
    r = requests.get(moralis_url, headers=h_values)

    return r.json()


def lint_transaction_list(processedTxnList: list) -> dict:
    # Divide list into Ethereum transfers and ERC20 transfers
    ethList = []
    erc20TransferList = []
    erc20ApprovalList = []
    otherList = []

    for txn in processedTxnList:
        tokenInfo = txn["ERC20Info"]
        
        if tokenInfo == None and txn["ethValue"] != 0: # If it's an ETH transfer, add to ethList
            ethList.append(txn)
        elif tokenInfo != None and tokenInfo["eventInfo"]["type"] == "transfer": # If it's an ERC20 transfer()
            erc20TransferList.append(txn)
        elif tokenInfo != None and tokenInfo["eventInfo"]["type"] == "approval": # If it's an ERC20 approval()
            erc20ApprovalList.append(txn)
        else:
            otherList.append(txn)

    return { # Return each list in dictionary - generate a table for each
        "eth": ethList,
        "erc20Transfer": erc20TransferList,
        "erc20Approval": erc20ApprovalList,
        "other": otherList
    }


