import importlib_resources as resources
import pandas as pd
import math
import sys
import threading
from datetime import datetime, timedelta
import os
from etherscan import Etherscan
from coinmetrics.api_client import CoinMetricsClient
import etherscan
import json
import time
import requests
import mysql.connector

while True:
    conn = mysql.connector.MySQLConnection(user='admin', password='wWusLXWEsxNqaviwGPsP',
                                     host='cryptologic-test-mysql-db.cyage1xxew24.us-east-1.rds.amazonaws.com',
                                     database='cryptologic_BE_Dev')
    c = conn.cursor()
    conn.commit()

    ETHERSCAN_KEY = os.environ.get('ZAZGB7REEJ4TSG38SS9PT6MR55KN83ZZ43')
    Token='0xCF3C8Be2e2C42331Da80EF210e9B1b307C03d36A'
    eth = Etherscan('ZAZGB7REEJ4TSG38SS9PT6MR55KN83ZZ43')
    div=(10**18)
    client = CoinMetricsClient()

    df = pd.DataFrame(columns = ['Trx_From_Wallet', 'Trx_To_Wallet', 'Trx_Amount', 'Trx_Datetime', 'Trx_Hash', 'Trx_Gas', 'Trx_GasPrice', 'Trx_Status', 'Trx_Method_ID'])


    df = pd.DataFrame(columns = ['wallet', 'balance', 'mv_time_frame', 'last_trx_time'])


    conn = mysql.connector.MySQLConnection(user='admin', password='wWusLXWEsxNqaviwGPsP',
                                     host='cryptologic-test-mysql-db.cyage1xxew24.us-east-1.rds.amazonaws.com',
                                     database='cryptologic_BE_Dev')
    c = conn.cursor()
    conn.commit()

    query = "SELECT * FROM wallets_monitor"
    df_aux = pd.read_sql(query, con=conn)


    for row in df_aux.index:
            if df_aux.at[row, "balance"] is None or math.isnan(df_aux.at[row, "balance"]):
                balance=(int(eth.get_acc_balance_by_token_and_contract_address(address=df_aux.at[row, "wallet"], contract_address=Token)))/div
                df = pd.concat([df, pd.DataFrame({'wallet': df_aux.at[row, "wallet"], 'balance':balance}, index=[Wallet])])
            else:
                 df = df.append({'wallet': df_aux.at[row, "wallet"], 'balance': df_aux.at[row, "balance"], 'mv_time_frame': df_aux.at[row, "mv_time_frame"], 'last_trx_time': df_aux.at[row, "last_trx_time"]}, ignore_index=True) 
    df.set_index('wallet', inplace=True)

    import datetime
    import time
    import logging
    from abc import ABC, abstractmethod
    from typing import Tuple, Optional, Callable, List, Iterable, Dict, Any

    from web3 import Web3
    from web3.contract import Contract
    from web3.datastructures import AttributeDict
    from web3.exceptions import BlockNotFound
    from eth_abi.codec import ABICodec

    from web3._utils.filters import construct_event_filter_params
    from web3._utils.events import get_event_data

    logger = logging.getLogger(__name__)

    os.remove("transfers.json")

    class EventScannerState(ABC):
        """Application state that remembers what blocks we have scanned in the case of crash.
        """

        @abstractmethod
        def get_last_scanned_block(self) -> int:
            """Number of the last block we have scanned on the previous cycle.

            :return: 0 if no blocks scanned yet
            """

        @abstractmethod
        def start_chunk(self, block_number: int):
            """Scanner is about to ask data of multiple blocks over JSON-RPC.

            Start a database session if needed.
            """

        @abstractmethod
        def end_chunk(self, block_number: int):
            """Scanner finished a number of blocks.

            Persistent any data in your state now.
            """

        @abstractmethod
        def process_event(self, block_when: datetime.datetime, event: AttributeDict) -> object:
            """Process incoming events.

            This function takes raw events from Web3, transforms them to your application internal
            format, then saves them in a database or some other state.

            :param block_when: When this block was mined

            :param event: Symbolic dictionary of the event data

            :return: Internal state structure that is the result of event tranformation.
            """

        @abstractmethod
        def delete_data(self, since_block: int) -> int:
            """Delete any data since this block was scanned.

            Purges any potential minor reorg data.
            """

    class EventScanner:
        """Scan blockchain for events and try not to abuse JSON-RPC API too much.

        Can be used for real-time scans, as it detects minor chain reorganisation and rescans.
        Unlike the easy web3.contract.Contract, this scanner can scan events from multiple contracts at once.
        For example, you can get all transfers from all tokens in the same scan.

        You *should* disable the default `http_retry_request_middleware` on your provider for Web3,
        because it cannot correctly throttle and decrease the `eth_getLogs` block number range.
        """

        def __init__(self, w3: Web3, contract: Contract, state: EventScannerState, events: List, filters: Dict[str, Any],
                     max_chunk_scan_size: int = 10000, max_request_retries: int = 30, request_retry_seconds: float = 3.0):
            """
            :param contract: Contract
            :param events: List of web3 Event we scan
            :param filters: Filters passed to getLogs
            :param max_chunk_scan_size: JSON-RPC API limit in the number of blocks we query. (Recommendation: 10,000 for mainnet, 500,000 for testnets)
            :param max_request_retries: How many times we try to reattempt a failed JSON-RPC call
            :param request_retry_seconds: Delay between failed requests to let JSON-RPC server to recover
            """

            self.logger = logger
            self.contract = contract
            self.w3 = w3
            self.state = state
            self.events = events
            self.filters = filters

            # Our JSON-RPC throttling parameters
            self.min_scan_chunk_size = 10  # 12 s/block = 120 seconds period
            self.max_scan_chunk_size = max_chunk_scan_size
            self.max_request_retries = max_request_retries
            self.request_retry_seconds = request_retry_seconds

            # Factor how fast we increase the chunk size if results are found
            # # (slow down scan after starting to get hits)
            self.chunk_size_decrease = 0.5

            # Factor how was we increase chunk size if no results found
            self.chunk_size_increase = 2.0

        @property
        def address(self):
            return self.token_address

        def get_block_timestamp(self, block_num) -> datetime.datetime:
            """Get Ethereum block timestamp"""
            try:
                block_info = self.w3.eth.getBlock(block_num)
            except BlockNotFound:
                # Block was not mined yet,
                # minor chain reorganisation?
                return None
            last_time = block_info["timestamp"]
            return datetime.datetime.utcfromtimestamp(last_time)

        def get_suggested_scan_start_block(self):
            """Get where we should start to scan for new token events.

            If there are no prior scans, start from block 1.
            Otherwise, start from the last end block minus ten blocks.
            We rescan the last ten scanned blocks in the case there were forks to avoid
            misaccounting due to minor single block works (happens once in a hour in Ethereum).
            These heurestics could be made more robust, but this is for the sake of simple reference implementation.
            """

            end_block = self.get_last_scanned_block()
            if end_block:
                return max(1, end_block - self.NUM_BLOCKS_RESCAN_FOR_FORKS)
            return 1

        def get_suggested_scan_end_block(self):
            """Get the last mined block on Ethereum chain we are following."""

            # Do not scan all the way to the final block, as this
            # block might not be mined yet
            return self.w3.eth.blockNumber - 1

        def get_last_scanned_block(self) -> int:
            return self.state.get_last_scanned_block()

        def delete_potentially_forked_block_data(self, after_block: int):
            """Purge old data in the case of blockchain reorganisation."""
            self.state.delete_data(after_block)

        def scan_chunk(self, start_block, end_block) -> Tuple[int, datetime.datetime, list]:
            """Read and process events between to block numbers.

            Dynamically decrease the size of the chunk if the case JSON-RPC server pukes out.

            :return: tuple(actual end block number, when this block was mined, processed events)
            """

            block_timestamps = {}
            get_block_timestamp = self.get_block_timestamp

            # Cache block timestamps to reduce some RPC overhead
            # Real solution might include smarter models around block
            def get_block_when(block_num):
                if block_num not in block_timestamps:
                    block_timestamps[block_num] = get_block_timestamp(block_num)
                return block_timestamps[block_num]

            all_processed = []

            for event_type in self.events:

                # Callable that takes care of the underlying web3 call
                def _fetch_events(_start_block, _end_block):
                    return _fetch_events_for_all_contracts(self.w3,
                                                           event_type,
                                                           self.filters,
                                                           from_block=_start_block,
                                                           to_block=_end_block)

                # Do `n` retries on `eth_getLogs`,
                # throttle down block range if needed
                end_block, events = _retry_web3_call(
                    _fetch_events,
                    start_block=start_block,
                    end_block=end_block,
                    retries=self.max_request_retries,
                    delay=self.request_retry_seconds)

                for evt in events:
                    idx = evt["logIndex"]  # Integer of the log index position in the block, null when its pending

                    # We cannot avoid minor chain reorganisations, but
                    # at least we must avoid blocks that are not mined yet
                    assert idx is not None, "Somehow tried to scan a pending block"

                    block_number = evt["blockNumber"]

                    # Get UTC time when this event happened (block mined timestamp)
                    # from our in-memory cache
                    block_when = get_block_when(block_number)

                    logger.debug(f"Processing event {evt['event']}, block: {evt['blockNumber']} count: {evt['blockNumber']}")
                    processed = self.state.process_event(block_when, evt)
                    all_processed.append(processed)

            end_block_timestamp = get_block_when(end_block)
            return end_block, end_block_timestamp, all_processed

        def estimate_next_chunk_size(self, current_chuck_size: int, event_found_count: int):
            """Try to figure out optimal chunk size

            Our scanner might need to scan the whole blockchain for all events

            * We want to minimize API calls over empty blocks

            * We want to make sure that one scan chunk does not try to process too many entries once, as we try to control commit buffer size and potentially asynchronous busy loop

            * Do not overload node serving JSON-RPC API by asking data for too many events at a time

            Currently Ethereum JSON-API does not have an API to tell when a first event occurred in a blockchain
            and our heuristics try to accelerate block fetching (chunk size) until we see the first event.

            These heurestics exponentially increase the scan chunk size depending on if we are seeing events or not.
            When any transfers are encountered, we are back to scanning only a few blocks at a time.
            It does not make sense to do a full chain scan starting from block 1, doing one JSON-RPC call per 20 blocks.
            """

            if event_found_count > 0:
                # When we encounter first events, reset the chunk size window
                current_chuck_size = self.min_scan_chunk_size
            else:
                current_chuck_size *= self.chunk_size_increase

            current_chuck_size = max(self.min_scan_chunk_size, current_chuck_size)
            current_chuck_size = min(self.max_scan_chunk_size, current_chuck_size)
            return int(current_chuck_size)

        def scan(self, start_block, end_block, start_chunk_size=20, progress_callback=Optional[Callable]) -> Tuple[
            list, int]:
            """Perform a token balances scan.

            Assumes all balances in the database are valid before start_block (no forks sneaked in).

            :param start_block: The first block included in the scan

            :param end_block: The last block included in the scan

            :param start_chunk_size: How many blocks we try to fetch over JSON-RPC on the first attempt

            :param progress_callback: If this is an UI application, update the progress of the scan

            :return: [All processed events, number of chunks used]
            """

            assert start_block <= end_block

            current_block = start_block

            # Scan in chunks, commit between
            chunk_size = start_chunk_size
            last_scan_duration = last_logs_found = 0
            total_chunks_scanned = 0

            # All processed entries we got on this scan cycle
            all_processed = []

            while current_block <= end_block:

                self.state.start_chunk(current_block, chunk_size)

                # Print some diagnostics to logs to try to fiddle with real world JSON-RPC API performance
                estimated_end_block = current_block + chunk_size
                logger.debug(
                    f"Scanning token transfers for blocks: {current_block} - {estimated_end_block}, chunk size {chunk_size}, last chunk scan took {last_scan_duration}, last logs found {last_logs_found}"
                )

                start = time.time()
                actual_end_block, end_block_timestamp, new_entries = self.scan_chunk(current_block, estimated_end_block)

                # Where does our current chunk scan ends - are we out of chain yet?
                current_end = actual_end_block

                last_scan_duration = time.time() - start
                all_processed += new_entries

                # Print progress bar
                if progress_callback:
                    progress_callback(start_block, end_block, current_block, end_block_timestamp, chunk_size, len(new_entries))

                # Try to guess how many blocks to fetch over `eth_getLogs` API next time
                chunk_size = self.estimate_next_chunk_size(chunk_size, len(new_entries))

                # Set where the next chunk starts
                current_block = current_end + 1
                total_chunks_scanned += 1
                self.state.end_chunk(current_end)
                time.sleep(1)

            return all_processed, total_chunks_scanned

    def _retry_web3_call(func, start_block, end_block, retries, delay) -> Tuple[int, list]:
        """A custom retry loop to throttle down block range.

        If our JSON-RPC server cannot serve all incoming `eth_getLogs` in a single request,
        we retry and throttle down block range for every retry.

        For example, Go Ethereum does not indicate what is an acceptable response size.
        It just fails on the server-side with a "context was cancelled" warning.

        :param func: A callable that triggers Ethereum JSON-RPC, as func(start_block, end_block)
        :param start_block: The initial start block of the block range
        :param end_block: The initial start block of the block range
        :param retries: How many times we retry
        :param delay: Time to sleep between retries
        """
        for i in range(retries):
            try:
                return end_block, func(start_block, end_block)
            except Exception as e:
                # Assume this is HTTPConnectionPool(host='localhost', port=8545): Read timed out. (read timeout=10)
                # from Go Ethereum. This translates to the error "context was cancelled" on the server side:
                # https://github.com/ethereum/go-ethereum/issues/20426
                if i < retries - 1:
                    # Give some more verbose info than the default middleware
                    logger.warning(
                        f"Retrying events for block range {start_block} - {end_block} ({end_block-start_block}) failed with {e} , retrying in {delay} seconds")
                    # Decrease the `eth_getBlocks` range
                    end_block = start_block + ((end_block - start_block) // 2)
                    # Let the JSON-RPC to recover e.g. from restart
                    time.sleep(delay)
                    continue
                else:
                    logger.warning("Out of retries")
                    raise


    def _fetch_events_for_all_contracts(
            w3,
            event,
            argument_filters: Dict[str, Any],
            from_block: int,
            to_block: int) -> Iterable:
        """Get events using eth_getLogs API.

        This method is detached from any contract instance.

        This is a stateless method, as opposed to createFilter.
        It can be safely called against nodes which do not provide `eth_newFilter` API, like Infura.
        """

        if from_block is None:
            raise TypeError("Missing mandatory keyword argument to getLogs: fromBlock")

        # Currently no way to poke this using a public Web3.py API.
        # This will return raw underlying ABI JSON object for the event
        abi = event._get_event_abi()

        # Depending on the Solidity version used to compile
        # the contract that uses the ABI,
        # it might have Solidity ABI encoding v1 or v2.
        # We just assume the default that you set on Web3 object here.
        # More information here https://eth-abi.readthedocs.io/en/latest/index.html
        codec: ABICodec = w3.codec

        # Here we need to poke a bit into Web3 internals, as this
        # functionality is not exposed by default.
        # Construct JSON-RPC raw filter presentation based on human readable Python descriptions
        # Namely, convert event names to their keccak signatures
        # More information here:
        # https://github.com/ethereum/web3.py/blob/e176ce0793dafdd0573acc8d4b76425b6eb604ca/web3/_utils/filters.py#L71
        data_filter_set, event_filter_params = construct_event_filter_params(
            abi,
            codec,
            address=argument_filters.get("address"),
            argument_filters=argument_filters,
            fromBlock=from_block,
            toBlock=to_block
        )

        logger.debug(f"Querying eth_getLogs with the following parameters: {event_filter_params}")

        # Call JSON-RPC API on your Ethereum node.
        # get_logs() returns raw AttributedDict entries
        logs = w3.eth.get_logs(event_filter_params)

        # Convert raw binary data to Python proxy objects as described by ABI
        all_events = []
        for log in logs:
            # Convert raw JSON-RPC log result to human readable event by using ABI data
            # More information how processLog works here
            # https://github.com/ethereum/web3.py/blob/fbaf1ad11b0c7fac09ba34baff2c256cffe0a148/web3/_utils/events.py#L200
            evt = get_event_data(codec, abi, log)
            # Note: This was originally yield,
            # but deferring the timeout exception caused the throttle logic not to work
            all_events.append(evt)
        return all_events

    if __name__ == "__main__":
        # Simple demo that scans all the token transfers of RCC token (11k).
        # The demo supports persistant state by using a JSON file.
        # You will need an Ethereum node for this.
        # Running this script will consume around 20k JSON-RPC calls.
        # With locally running Geth, the script takes 10 minutes.
        # The resulting JSON state file is 2.9 MB.
        import sys
        import json
        from web3.providers.rpc import HTTPProvider

        # We use tqdm library to render a nice progress bar in the console
        # https://pypi.org/project/tqdm/
        from tqdm import tqdm

        # RCC has around 11k Transfer events
        # https://etherscan.io/token/0x9b6443b0fb9c241a7fdac375595cea13e6b7807a
        RCC_ADDRESS = "0xCF3C8Be2e2C42331Da80EF210e9B1b307C03d36A"

        # Reduced ERC-20 ABI, only Transfer event
        ABI = """[
            {
                "anonymous": false,
                "inputs": [
                    {
                        "indexed": true,
                        "name": "from",
                        "type": "address"
                    },
                    {
                        "indexed": true,
                        "name": "to",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "name": "value",
                        "type": "uint256"
                    }
                ],
                "name": "Transfer",
                "type": "event"
            }
        ]
        """

        class JSONifiedState(EventScannerState):
            """Store the state of scanned blocks and all events.

            All state is an in-memory dict.
            Simple load/store massive JSON on start up.
            """

            def __init__(self):
                self.state = None
                self.fname = "transfers.json"
                # How many second ago we saved the JSON file
                self.last_save = 0

            def reset(self):
                """Create initial state of nothing scanned."""
                self.state = {
                    "last_scanned_block": 0,
                    "blocks": {},
                }

            def restore(self):
                """Restore the last scan state from a file."""
                try:
                    self.state = json.load(open(self.fname, "rt"))
                    print(f"Restored the state, previously {self.state['last_scanned_block']} blocks have been scanned")
                except (IOError, json.decoder.JSONDecodeError):
                    print("State starting from scratch")
                    self.reset()

            def save(self):
                """Save everything we have scanned so far in a file."""
                with open(self.fname, "wt") as f:
                    json.dump(self.state, f)
                self.last_save = time.time()

            #
            # EventScannerState methods implemented below
            #

            def get_last_scanned_block(self):
                """The number of the last block we have stored."""
                return self.state["last_scanned_block"]

            def delete_data(self, since_block):
                """Remove potentially reorganised blocks from the scan data."""
                for block_num in range(since_block, self.get_last_scanned_block()):
                    if block_num in self.state["blocks"]:
                        del self.state["blocks"][block_num]

            def start_chunk(self, block_number, chunk_size):
                pass

            def end_chunk(self, block_number):
                """Save at the end of each block, so we can resume in the case of a crash or CTRL+C"""
                # Next time the scanner is started we will resume from this block
                self.state["last_scanned_block"] = block_number

                # Save the database file for every minute
                if time.time() - self.last_save > 60:
                    self.save()

            def process_event(self, block_when: datetime.datetime, event: AttributeDict) -> str:
                """Record a ERC-20 transfer in our database."""
                # Events are keyed by their transaction hash and log index
                # One transaction may contain multiple events
                # and each one of those gets their own log index

                # event_name = event.event # "Transfer"
                log_index = event.logIndex  # Log index within the block
                # transaction_index = event.transactionIndex  # Transaction index within the block
                txhash = event.transactionHash.hex()  # Transaction hash
                block_number = event.blockNumber

                # Convert ERC-20 Transfer event to our internal format
                args = event["args"]
                transfer = {
                    "from": args["from"],
                    "to": args.to,
                    "value": args.value,
                    "timestamp": block_when.isoformat(),
                }

                # Create empty dict as the block that contains all transactions by txhash
                if block_number not in self.state["blocks"]:
                    self.state["blocks"][block_number] = {}

                block = self.state["blocks"][block_number]
                if txhash not in block:
                    # We have not yet recorded any transfers in this transaction
                    # (One transaction may contain multiple events if executed by a smart contract).
                    # Create a tx entry that contains all events by a log index
                    self.state["blocks"][block_number][txhash] = {}

                # Record ERC-20 transfer in our database
                self.state["blocks"][block_number][txhash][log_index] = transfer

                # Return a pointer that allows us to look up this event later if needed
                return f"{block_number}-{txhash}-{log_index}"

        def run():

            if len(sys.argv) < 2:
                print("Usage: eventscanner.py http://your-node-url")
                sys.exit(1)

            api_url = "https://summer-necessary-wish.discover.quiknode.pro/0f8072ec3941a0b0996c64aeb42bf46125b91126"

            # Enable logs to the stdout.
            # DEBUG is very verbose level
            logging.basicConfig(level=logging.INFO)

            provider = HTTPProvider(api_url)

            # Remove the default JSON-RPC retry middleware
            # as it correctly cannot handle eth_getLogs block range
            # throttle down.
            provider.middlewares.clear()

            w3 = Web3(provider)

            # Prepare stub ERC-20 contract object
            abi = json.loads(ABI)
            ERC20 = w3.eth.contract(abi=abi)

            # Restore/create our persistent state
            state = JSONifiedState()
            state.restore()

            # chain_id: int, w3: Web3, abi: Dict, state: EventScannerState, events: List, filters: Dict, max_chunk_scan_size: int=10000
            scanner = EventScanner(
                w3=w3,
                contract=ERC20,
                state=state,
                events=[ERC20.events.Transfer],
                filters={"address": RCC_ADDRESS},
                # How many maximum blocks at the time we request from JSON-RPC
                # and we are unlikely to exceed the response size limit of the JSON-RPC server
                max_chunk_scan_size=10000
            )

            # Assume we might have scanned the blocks all the way to the last Ethereum block
            # that mined a few seconds before the previous scan run ended.
            # Because there might have been a minor Etherueum chain reorganisations
            # since the last scan ended, we need to discard
            # the last few blocks from the previous scan results.
            chain_reorg_safety_blocks = 10
            scanner.delete_potentially_forked_block_data(state.get_last_scanned_block() - chain_reorg_safety_blocks)

            # Scan from [last block scanned] - [latest ethereum block]
            # Note that our chain reorg safety blocks cannot go negative
            start_block =15428155# scanner.get_suggested_scan_end_block()
            end_block =15428155# scanner.get_suggested_scan_end_block()
            blocks_to_scan = end_block - start_block

            print(f"Scanning events from blocks {start_block} - {end_block}")

            # Render a progress bar in the console
            start = time.time()
            with tqdm(total=blocks_to_scan) as progress_bar:
                def _update_progress(start, end, current, current_block_timestamp, chunk_size, events_count):
                    if current_block_timestamp:
                        formatted_time = current_block_timestamp.strftime("%d-%m-%Y")
                    else:
                        formatted_time = "no block time available"
                    progress_bar.set_description(f"Current block: {current} ({formatted_time}), blocks in a scan batch: {chunk_size}, events processed in a batch {events_count}")
                    progress_bar.update(chunk_size)

                # Run the scan
                result, total_chunks_scanned = scanner.scan(start_block, end_block, progress_callback=_update_progress)

            state.save()
            duration = time.time() - start
            print(f"Scanned total {len(result)} Transfer events, in {duration} seconds, total {total_chunks_scanned} chunk scans performed")

            return result

        run()


    def check_trx_value(df, max_trx_value, curr_value, curr_from, curr_to):
        if curr_value > max_trx_value and curr_from in df.index:
            return curr_from
        if curr_value > max_trx_value and curr_to in df.index:
            return curr_to

    def check_perc_var(df, max_perc_var, curr_from, curr_to, curr_value):
        if curr_from in df.index and ((curr_value)/(df.at[curr_from, "balance"]*100+1))>max_perc_var:
            return curr_from
        if curr_to in df.index and ((curr_value)/(df.at[curr_to, "balance"]*100+1))>max_perc_var:
            return curr_to
        else:    
            pass

    def check_amount_mv_time(df, mv_time_frame, curr_from, curr_to):
        if curr_from in df.index:
            mv_from=df.at[curr_from, "mv_time_frame"]
            if mv_from>mv_time_frame:
                return curr_from
        if curr_to in df.index:   
            mv_to=df.at[curr_to, "mv_time_frame"]
            if mv_to>mv_time_frame:   
                return curr_to
        else:
            pass

    def check_amount_address(df, mv_time_frame, curr_from, curr_to, max_balance):

        if curr_from in df.index:
            bal_from=df.at[curr_from, "balance"]
            if bal_from>max_balance:
                return curr_from
        if curr_to in df.index:
            bal_to=df.at[curr_to, "balance"]
            if bal_to>max_balance:
                return curr_to
        else:
            pass


    def check_mv_time_frame(df, curr_add, curr_time, curr_time_last, mv_time_frame):
        if pd.Timedelta(pd.to_datetime(curr_time)-pd.to_datetime(curr_time_last))<=time_frame:
            return True
        else:
            return False




    def update_tokendb(df, df_alert, curr_value, curr_from, curr_to, curr_time, max_perc_var, key, mv_time_frame, max_balance):
        check_perc = check_perc_var(df, max_perc_var, curr_from, curr_to, curr_value)
        if check_perc is not None:
            df_alert = df_alert.append({'type': max_perc_name, 'trx_hash': key, 'wallet_involved':check, 'timestamp': curr_time}, ignore_index=True)

    #in case all add is at db 

        if curr_from in df.index and curr_to in df.index:
            df.at[curr_from, "balance"]-=curr_value
            df.at[curr_to, "balance"]+=curr_value
            if check_mv_time_frame(df, curr_from, curr_time, df.at[curr_from, "last_trx_time"], time_frame):
                df.at[curr_from, "mv_time_frame"]+=curr_value
            else:
                df.at[curr_from, "last_trx_time"]=curr_time
                df.at[curr_from, "mv_time_frame"]=curr_value
                if check_mv_time_frame(df, curr_to, curr_time, df.at[curr_to, "last_trx_time"], time_frame):
                    df.at[curr_to, "mv_time_frame"]+=curr_value
                else:
                    df.at[curr_to, "last_trx_time"]=curr_time
                    df.at[curr_to, "mv_time_frame"]=curr_value

            check_time=check_amount_mv_time(df, mv_time_frame, curr_from, curr_to)
            if check_time is not None:
                df_alert = df_alert.append({'type': mv_time_name, 'trx_hash': key, 'wallet_involved':check_time, 'timestamp': curr_time}, ignore_index=True) 

            check_bal=check_amount_address(df, mv_time_frame, curr_from, curr_to, max_balance)
            if check_bal is not None and check_bal in df.index:
                df_alert = df_alert.append({'type': max_bal_name, 'trx_hash': key, 'wallet_involved':check_bal, 'timestamp': curr_time}, ignore_index=True) 



    #in case from address is but to address isn't in db

        if (curr_from in df.index) and (curr_to not in df.index):
            print("pixa")
            df.at[curr_from, "balance"]-=curr_value
            if check_mv_time_frame(df, curr_from, curr_time, df.at[curr_from, "last_trx_time"], time_frame):
                df.at[curr_from, "mv_time_frame"]+=curr_value
            else:
                df.at[curr_from, "last_trx_time"]=curr_time
                df.at[curr_from, "mv_time_frame"]=curr_value

            check_time=check_amount_mv_time(df, mv_time_frame, curr_from, curr_to)
            if check_time is not None:
                df_alert = df_alert.append({'type': mv_time_name, 'trx_hash': key, 'wallet_involved':check_time, 'timestamp': curr_time}, ignore_index=True) 

            check_bal=check_amount_address(df, mv_time_frame, curr_from, curr_to, max_balance)
            if check_bal is not None:
                df_alert = df_alert.append({'type': max_bal_name, 'trx_hash': key, 'wallet_involved':check_bal, 'timestamp': curr_time}, ignore_index=True) 


    #in case to add is but from isn't in db

        if (curr_from not in df.index) and (curr_to in df.index):
            df.at[curr_to, "balance"]+=curr_value
            if check_mv_time_frame(df, curr_to, curr_time, df.at[curr_to, "last_trx_time"], time_frame):
                df.at[curr_to, "mv_time_frame"]+=curr_value

            else:
                df.at[curr_to, "last_trx_time"]=curr_time
                df.at[curr_to, "mv_time_frame"]=curr_value

            mv_to=df.at[curr_to, "mv_time_frame"]
            if mv_to>mv_time_frame:
                df_alert = df_alert.append({'type': mv_time_name, 'trx_hash': key, 'wallet_involved':curr_to, 'timestamp': curr_time}, ignore_index=True)     
            mv_to=df.at[curr_to, "balance"]
            if mv_to>max_balance:
                df_alert = df_alert.append({'type': max_bal_name, 'trx_hash': key, 'wallet_involved':curr_to, 'timestamp': curr_time}, ignore_index=True) 

        return df, df_alert


    import json

    trx_contract_file=open('transfers.json','r')
    trx_by_contract=trx_contract_file.read()

    obj=json.loads(trx_by_contract)
    obj_list = list(obj['blocks'].items())


    conn = mysql.connector.MySQLConnection(user='admin', password='wWusLXWEsxNqaviwGPsP',
                                     host='cryptologic-test-mysql-db.cyage1xxew24.us-east-1.rds.amazonaws.com',
                                     database='cryptologic_BE_Dev')
    c = conn.cursor()
    conn.commit()

    query = "SELECT * FROM rules"
    df_rules = pd.read_sql(query, con=conn)


    def set_params(df_rules):
        max_trx_value=100000000000
        max_perc_var=100000
        max_balance=10000000000000
        mv_time_frame=100000000000
        max_trx_name='None'
        max_perc_name='None'
        max_bal_name='None'
        mv_time_name='None'
        time_frame=pd.Timedelta(0, "d")
        for ind in df_rules.index:
            if df_rules.at[ind, "criteria"] == 'Transaction Value':
                max_trx_value=df_rules.at[ind, "value"]
                max_trx_name=df_rules.at[ind, "rule_name"]
            if df_rules.at[ind, "criteria"] == 'Percentage Variation':
                max_perc_var=df_rules.at[ind, "value"]
                max_perc_name=df_rules.at[ind, "rule_name"]
            if df_rules.at[ind, "criteria"] == 'Balance':
                max_balance=df_rules.at[ind, "value"]
                max_bal_name=df_rules.at[ind, "rule_name"]
            if df_rules.at[ind, "criteria"] == 'Amount Moved in Interval of':
                mv_time_frame=df_rules.at[ind, "value"]
                mv_time_name=df_rules.at[ind, "rule_name"]
                if df_rules.at[ind, "frame"] == 'hours':
                    time_frame=pd.Timedelta(df_rules.at[ind, "time"], "h")
                elif df_rules.at[ind, "frame"] == 'minutes':
                    time_frame=pd.Timedelta(df_rules.at[ind, "time"], "m")
                elif df_rules.at[ind, "frame"] == 'seconds':
                    time_frame=pd.Timedelta(df_rules.at[ind, "time"], "sec")
                elif df_rules.at[ind, "frame"] == 'days':
                    time_frame=pd.Timedelta(df_rules.at[ind, "time"], "d")


        return max_trx_value, max_perc_var, max_balance, mv_time_frame, time_frame, max_trx_name, max_perc_name, max_bal_name, mv_time_name
    max_trx_value, max_perc_var, max_balance, mv_time_frame, time_frame, max_trx_name, max_perc_name, max_bal_name, mv_time_name = set_params(df_rules)

    df_alert = pd.DataFrame(columns = ['type', 'trx_hash', 'wallet_involved', 'timestamp'])
    for i in range(len(obj_list)):
        curr=obj_list[i][1] 
        key=list(obj_list[i][1]) 

        for j in range(len(curr)):
            curr_in=curr[key[0]]
            key_in=list(curr_in)
            fields=curr_in[key_in[0]]
            curr_value=int(fields['value'])/(10**18)
            curr_from=fields['from']
            curr_to=fields['to']
            curr_time=fields['timestamp']
            check = check_trx_value(df, max_trx_value, curr_value, curr_from, curr_to)
            if check is not None:
                df_alert = df_alert.append({'type': max_trx_name, 'trx_hash': key[0], 'wallet_involved':check, 'timestamp': curr_time}, ignore_index=True)

            df, df_alert=update_tokendb(df, df_alert, curr_value, curr_from, curr_to, curr_time, max_perc_var, key[0], mv_time_frame, max_balance)

    import sqlalchemy

    url = 'mysql+mysqlconnector://admin:wWusLXWEsxNqaviwGPsP@cryptologic-test-mysql-db.cyage1xxew24.us-east-1.rds.amazonaws.com:3306/cryptologic_BE_Dev'
    engine = sqlalchemy.create_engine(url)
    df_alert.to_sql("alerts", engine, if_exists="append", index= False)
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'wallet'}, inplace=True)

    c.execute('DROP TABLE wallets_monitor')
    url = 'mysql+mysqlconnector://admin:wWusLXWEsxNqaviwGPsP@cryptologic-test-mysql-db.cyage1xxew24.us-east-1.rds.amazonaws.com:3306/cryptologic_BE_Dev'
    engine = sqlalchemy.create_engine(url)
    df.to_sql("wallets_monitor", engine, index=False)
