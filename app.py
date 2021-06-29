import time
import json
import pandas as pd
from stellar_sdk import (
    Asset,
    Keypair,
    Server,
    ManageBuyOffer,
    ManageSellOffer,
    Network,
    TransactionBuilder
)
from config import (
    URL,
    SECRET,
    COUNTER_ASSET,
    BASE_ASSETS,
)


class MarketMaker:

    def __init__(self):
        self.server = Server(horizon_url=URL)
        self.keypair = Keypair.from_secret(SECRET)
        self.account = None
        self.get_account()
        self.transaction = TransactionBuilder(
            source_account=self.account,
            network_passphrase=Network.PUBLIC_NETWORK_PASSPHRASE,
            base_fee=100
        )
        self.counter_asset = Asset(COUNTER_ASSET[0], issuer=COUNTER_ASSET[1])
        self.base_balances = {}
        self.counter_balance = 0
        self.records = {}
        self.candles = {}
        self.orderbook = {}
        self.buy_offers = []
        self.sell_offers = []
        self.fills = []
        self.open_records()
        self.get_latest_candles()
        self.get_balances()

    def open_records(self):
        with open('mmrecords.json', 'r') as f:
            self.records = json.load(f)
            f.close()

    def save_records(self):
        with open('mmrecords.json', 'w') as f:
            self.records = json.dump(self.records, f)
            f.close()

    def get_latest_candles(self):
        for asset in BASE_ASSETS:
            base_asset = Asset(asset, issuer=BASE_ASSETS[asset])
            r = self.server.trade_aggregations(
                base_asset,
                self.counter_asset,
                3600000,
                start_time=int((time.time() - 3600) * 1000)
            ).limit(1).call()
            self.candles[
                f'{asset}-{COUNTER_ASSET[0]}'
            ] = r['_embedded']['records']

    def get_orderbook(self, asset: str):
        base_asset = Asset(asset, issuer=BASE_ASSETS[asset])
        self.orderbook = self.server.orderbook(
            selling=base_asset,
            buying=self.counter_asset
        ).call()

    def get_account(self):
        self.account = self.server.load_account(
            account_id=self.keypair.public_key
        )

    def get_balances(self):
        r = self.server.accounts()\
            .for_signer(signer=self.keypair.public_key).call()
        balances = r['_embedded']['records'][0]['balances']
        for asset in BASE_ASSETS:
            self.base_balances[asset] = float(
                [
                    x for x in balances
                    if asset == 'XLM' and x['asset_type'] == 'native'
                    or x.get('asset_code') == asset
                ][0]['balance']
            )
        self.counter_balance = float(
            [
                x for x in balances
                if x.get('asset_code') == COUNTER_ASSET[0]
                and x.get('asset_issuer') == COUNTER_ASSET[1]
            ][0]['balance']
        )

    def get_buy_offers(self, asset: str):
        r = self.server.offers().for_seller(self.keypair.public_key).call()
        offers = r['_embedded']['records']
        self.buy_offers = [
            x for x in offers
            if (
                x['buying'].get('asset_code')
                and x['buying']['asset_code'] == BASE_ASSETS[asset]
                or asset == 'XLM'
                and x['buying']['asset_type'] == 'native'
            )
            and x['selling'].get('asset_code')
            and x['selling']['asset_code'] == COUNTER_ASSET[0]
        ]

    def get_sell_offers(self, asset: str):
        r = self.server.offers().for_seller(self.keypair.public_key).call()
        offers = r['_embedded']['records']
        self.sell_offers = [
            x for x in offers
            if (
                x['selling'].get('asset_code')
                and x['selling']['asset_code'] == BASE_ASSETS[asset]
                or asset == 'XLM'
                and x['selling']['asset_type'] == 'native'
            )
            and x['buying'].get('asset_code')
            and x['buying']['asset_code'] == COUNTER_ASSET[0]
        ]

    def trade(self):
        for market in self.candles:
            asset = market.split('-')[0]
            base_asset = Asset(asset, issuer=BASE_ASSETS[asset])
            record = self.records[market]
            candle = self.candles[market][0]
            self.get_orderbook(asset)
            self.get_buy_offers(asset)
            self.get_sell_offers(asset)

            base_balance_quote = \
                self.base_balances[asset] \
                    * float(candle['close'])

            if base_balance_quote < self.counter_balance:
                orderbook_price = float(self.orderbook['asks'][0]['price'])
                size = min(self.counter_balance, 10000)
                size = round(size / orderbook_price, 7)
                price = min(
                    float(candle['close']),
                    float(self.orderbook['bids'][0]['price'])
                )
                price = round(price * .995, 7)

                op = ManageBuyOffer(
                    selling=self.counter_asset,
                    buying=base_asset,
                    amount=str(size),
                    price=str(price),
                    offer_id=0
                )

                if not self.buy_offers:
                    transaction = self.transaction.append_operation(op) \
                        .set_timeout(30).build()
                    transaction.sign(self.keypair)
                    self.server.submit_transaction(transaction)
                    record['buySize'] = size
                    record['buyPrice'] = price
                elif record['open'] != candle['open']:
                    op.offer_id = [int(x['id']) for x in self.buy_offers][0]
                    transaction = self.transaction.append_operation(op) \
                        .set_timeout(30).build()
                    transaction.sign(self.keypair)
                    self.server.submit_transaction(transaction)
                    record['buySize'] = size
                    record['buyPrice'] = price
            else:
                price = max(
                    record['buyPrice'] * 1.001 ** 2,
                    float(self.orderbook['asks'][0]['price'])
                )
                price = round(price, 7)

                op = ManageSellOffer(
                    selling=base_asset,
                    buying=self.counter_asset,
                    amount=str(record['buySize']),
                    price=str(price),
                    offer_id=0
                )

                if not self.sell_offers:
                    transaction = self.transaction.append_operation(op) \
                        .set_timeout(30).build()
                    transaction.sign(self.keypair)
                    self.server.submit_transaction(transaction)
                elif (
                    float(self.orderbook['bids'][0]['price']) <
                    record['buyPrice'] * .99
                    ):
                    op.price = self.orderbook['bids'][0]['price']
                    op.offer_id = [int(x['id']) for x in self.sell_offers][0]
                    transaction = self.transaction.append_operation(op) \
                        .set_timeout(30).build()
                    transaction.sign(self.keypair)
                    self.server.submit_transaction(transaction)

            record['open'] = candle['open']
        self.save_records()


def run_bot():

    server = Server(horizon_url=URL)
    keypair = Keypair.from_secret(SECRET)
    account = server.load_account(account_id=keypair.public_key)

    transaction = TransactionBuilder(
        source_account=account,
        network_passphrase=Network.PUBLIC_NETWORK_PASSPHRASE,
        base_fee=100
    )

    counter_asset = Asset(COUNTER_ASSET[0], issuer=COUNTER_ASSET[1])

    for code in BASE_ASSETS:
        base_asset = Asset(code, issuer=BASE_ASSETS[code])

        # This prevents any further offers being made while a selling offer is still pending
        r = server.offers().for_seller(keypair.public_key).call()
        offers = r['_embedded']['records']
        buy_offers = [x for x in offers if x['selling'].get('asset_code') and x['selling']['asset_code'] == COUNTER_ASSET[0]]

        r = server.accounts().for_signer(signer=keypair.public_key).call()
        balances = r['_embedded']['records'][0]['balances']
        base_balance = float([x for x in balances if x['asset_type'] == 'native'][0]['balance'])

        counter_balance = float(
            [
                x for x in balances
                if x.get('asset_code') == COUNTER_ASSET[0] and x.get('asset_issuer') == COUNTER_ASSET[1]
            ][0]['balance']
        )

        def get_candles(start_time, data=[]):
            r = server.trade_aggregations(base_asset, counter_asset, 3600000, start_time=start_time).limit(20).call()
            data = r['_embedded']['records'] + data
            if len(data) >= 20:
                return data
            else:
                start_time = int(data[0]['timestamp']) - 3600000 * 20
                return get_candles(start_time, data=data)

        start_time = int((time.time() - 36000) * 1000)
        data = get_candles(start_time)
        
        df = pd.DataFrame(data, dtype=float)
        df.drop(columns=['high_r', 'low_r', 'open_r', 'close_r'], inplace=True)
        df.drop_duplicates(inplace=True)
        df.index = pd.to_datetime(df.timestamp, unit='ms')
        df.sort_index(inplace=True)

        # INDICATORS

        # Volume

        df['volume_ma'] = df.base_volume.rolling(20, min_periods=1).mean()
        df['volume_stdev'] = df.base_volume.rolling(20, min_periods=1).std()
        df['volume_thres'] = df.volume_ma + df.volume_stdev * 2

        # Relative Strength Indicator

        delta = df.close.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=6, adjust=False).mean()
        ema_down = down.ewm(com=6, adjust=False).mean()
        rs = ema_up / ema_down
        df['RSI'] = 100 - (100 / (1 + rs))

        # LATEST FRAME
        bar = df.iloc[-1]

        # ORDERBOOK

        r = server.orderbook(selling=base_asset, buying=counter_asset).call()
        orderbook = {
            'buy': {
                'price': 0,
                'amount': 0
            },
            'sell': {
                'price': 0,
                'amount': 0
            },
            'ask_price': float(r['asks'][0]['price']),
            'bid_price': float(r['bids'][0]['price'])
        }
        for n in range(10):
            if orderbook['buy']['amount'] < counter_balance / bar.close * 1.01:
                orderbook['buy']['price'] = float(r['asks'][n]['price'])
                orderbook['buy']['amount'] += float(r['asks'][n]['amount'])
            if orderbook['sell']['amount'] < base_balance * 1.01:
                orderbook['sell']['price'] = float(r['bids'][n]['price'])
                orderbook['sell']['amount'] += float(r['bids'][n]['amount'])

        # ADVICE

        buy = (
            bar.base_volume > bar.volume_thres
            and bar.RSI < 40
        )

        sell = (
            bar.base_volume > bar.volume_thres
            and bar.RSI > 60
        )

        # OPERATIONS

        if buy:

            amount = round(
                min(
                    counter_balance / bar.close * .95,
                    orderbook['buy']['amount'] * .95
                ),
                7
            )

            op = ManageBuyOffer(
                selling=counter_asset,
                buying=base_asset,
                amount=str(amount),
                price=str(orderbook['buy']['price']),
                offer_id=0
            )

            transaction = transaction.append_operation(op).set_timeout(30).build()
            transaction.sign(keypair)
            r = server.submit_transaction(transaction)

        elif sell:

            for offer_id in [int(x['id']) for x in buy_offers]:
                # Delete existing buy offer
                op = ManageBuyOffer(
                    selling=counter_asset,
                    buying=base_asset,
                    amount='0',
                    price=str(orderbook['sell']['price']),
                    offer_id=offer_id
                )
                transaction.append_operation(op)

            if base_balance:
                amount = round(
                    min(
                        base_balance * .95,
                        orderbook['sell']['amount'] * .95
                    ),
                    7
                )

                op = ManageSellOffer(
                    selling=base_asset,
                    buying=counter_asset,
                    amount=str(amount),
                    price=str(orderbook['sell']['price']),
                    offer_id=0
                )
                transaction.append_operation(op)

            if transaction.operations:
                transaction = transaction.set_timeout(30).build()
                transaction.sign(keypair)
                r = server.submit_transaction(transaction)
