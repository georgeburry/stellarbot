import time
import json
import statistics
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


class Bot:

    def __init__(self, num_samples=20, num_std=2, records_fname='records'):
        self.num_samples = num_samples
        self.num_std = num_std
        self.records_fname = records_fname
        self.server = Server(horizon_url=URL)
        self.keypair = Keypair.from_secret(SECRET)
        self.account = None
        self.get_account()
        self.transaction = TransactionBuilder(
            source_account=self.account,
            network_passphrase=Network.PUBLIC_NETWORK_PASSPHRASE,
            base_fee=100
        )
        self.market = None
        self.counter_asset = Asset(COUNTER_ASSET[0], issuer=COUNTER_ASSET[1])
        self.base_asset = None
        self.latest_low = None
        self.price_history = []
        self.mean_price = None
        self.mean_std = None
        self.counter_balance = None
        self.base_balances = {}
        self.base_balance = None
        self.base_balance_quote = None
        self.orderbook = {}
        self.buy_offers = []
        self.sell_offers = []

    def load_all_records(self):
        with open(self.records_fname + '.json', 'r') as f:
            records = json.load(f)
        return records

    def load_market_record(self):
        records = self.load_all_records()
        return records[self.market]

    def save_market_record(self, data):
        records = self.load_all_records()
        records[self.market] = data
        with open(self.records_fname + '.json', 'w') as f:
            json.dump(records, f)

    def get_price_history(self):
        r = self.server.trade_aggregations(
            self.base_asset,
            self.counter_asset,
            60000,
            start_time=int((time.time() - 1200) * 1000)
        ).limit(20).call()
        data = r['_embedded']['records'][-self.num_samples:]
        self.latest_low = float(data[-1]['low'])
        self.price_history = [float(x['close']) for x in data]

    def calculate_price_stats(self):
        self.mean_price = statistics.mean(self.price_history)
        self.mean_std = statistics.stdev(self.price_history)

    def check_price_anomaly(self):
        return self.latest_low < self.mean_price - self.num_std * self.mean_std

    def get_orderbook(self):
        self.orderbook = self.server.orderbook(
            selling=self.base_asset,
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

    def get_buy_offers(self):
        r = self.server.offers().for_seller(self.keypair.public_key).call()
        offers = r['_embedded']['records']
        self.buy_offers = [
            x for x in offers
            if (
                x['buying'].get('asset_code')
                and x['buying']['asset_code'] == BASE_ASSETS[self.base_asset.code]
                or self.base_asset.code == 'XLM'
                and x['buying']['asset_type'] == 'native'
            )
            and x['selling'].get('asset_code')
            and x['selling']['asset_code'] == COUNTER_ASSET[0]
        ]

    def get_sell_offers(self):
        r = self.server.offers().for_seller(self.keypair.public_key).call()
        offers = r['_embedded']['records']
        self.sell_offers = [
            x for x in offers
            if (
                x['selling'].get('asset_code')
                and x['selling']['asset_code'] ==
                    BASE_ASSETS[self.base_asset.code]
                or self.base_asset.code == 'XLM'
                and x['selling']['asset_type'] == 'native'
            )
            and x['buying'].get('asset_code')
            and x['buying']['asset_code'] == COUNTER_ASSET[0]
        ]

    def run_meanreversion_strategy(self):
        for market in [b + '-' + COUNTER_ASSET[0] for b in BASE_ASSETS]:
            self.market = market
            self.base_asset = Asset(
                self.market.split('-')[0],
                issuer=BASE_ASSETS[self.market.split('-')[0]]
            )
            self.get_price_history()
            self.calculate_price_stats()
            self.get_orderbook()
            self.get_balances()
            self.get_buy_offers()
            self.get_sell_offers()

            self.base_balance = self.base_balances[self.base_asset.code]
            self.base_balance_quote =\
                self.base_balance * self.price_history[-1]

            if self.base_balance_quote < self.counter_balance:
                if self.check_price_anomaly():
                    size = min(
                        self.counter_balance / self.price_history[-1] * .99,
                        10000
                    )
                    size = str(round(size, 7))
                    price = min(
                        self.latest_low,
                        float(self.orderbook['bids'][0]['price']),
                    )
                    price = str(round(price, 7))

                    op = ManageBuyOffer(
                        selling=self.counter_asset,
                        buying=self.base_asset,
                        amount=size,
                        price=price,
                        offer_id=0
                    )

                    if self.buy_offers:
                        op.offer_id = int(self.buy_offers[0]['id'])
                    elif (
                        not self.buy_offers
                        or float(price) != float(self.buy_offers[0]['price'])
                        ):
                        transaction = self.transaction\
                            .append_operation(op)\
                            .set_timeout(30)\
                            .build()
                        transaction.sign(self.keypair)
                        self.server.submit_transaction(transaction)
                        self.save_market_record(
                            {
                                'entryPrice': float(price),
                                'targetSigma': self.mean_std
                            }
                        )
            else:
                record = self.load_market_record()
                entry_price = record['entryPrice']
                target_sigma = record['targetSigma']
                ask_price = float(self.orderbook['asks'][0]['price'])
                # Take profit price
                price = max(
                    entry_price + target_sigma,
                    entry_price * 1.0075,
                )
                # Stop loss price
                if ask_price < entry_price - (price - entry_price) * .67:
                    price = ask_price
                price = str(round(price, 7))
                size = str(round((self.base_balance - 2) * .99, 7))

                op = ManageSellOffer(
                    selling=self.base_asset,
                    buying=self.counter_asset,
                    amount=size,
                    price=price,
                    offer_id=0
                )

                if self.sell_offers:
                    op.offer_id = int(self.sell_offers[0]['id'])
                if (
                    not self.sell_offers
                    or float(price) != float(self.sell_offers[0]['price'])
                    ):
                    transaction = self.transaction\
                        .append_operation(op)\
                        .set_timeout(30)\
                        .build()
                    transaction.sign(self.keypair)
                    self.server.submit_transaction(transaction)


# def run_bot():

#     server = Server(horizon_url=URL)
#     keypair = Keypair.from_secret(SECRET)
#     account = server.load_account(account_id=keypair.public_key)

#     transaction = TransactionBuilder(
#         source_account=account,
#         network_passphrase=Network.PUBLIC_NETWORK_PASSPHRASE,
#         base_fee=100
#     )

#     counter_asset = Asset(COUNTER_ASSET[0], issuer=COUNTER_ASSET[1])

#     for code in BASE_ASSETS:
#         base_asset = Asset(code, issuer=BASE_ASSETS[code])

#         # This prevents any further offers being made while a selling offer is still pending
#         r = server.offers().for_seller(keypair.public_key).call()
#         offers = r['_embedded']['records']
#         buy_offers = [x for x in offers if x['selling'].get('asset_code') and x['selling']['asset_code'] == COUNTER_ASSET[0]]

#         r = server.accounts().for_signer(signer=keypair.public_key).call()
#         balances = r['_embedded']['records'][0]['balances']
#         base_balance = float([x for x in balances if x['asset_type'] == 'native'][0]['balance'])

#         counter_balance = float(
#             [
#                 x for x in balances
#                 if x.get('asset_code') == COUNTER_ASSET[0] and x.get('asset_issuer') == COUNTER_ASSET[1]
#             ][0]['balance']
#         )

#         def get_candles(start_time, data=[]):
#             r = server.trade_aggregations(base_asset, counter_asset, 3600000, start_time=start_time).limit(20).call()
#             data = r['_embedded']['records'] + data
#             if len(data) >= 20:
#                 return data
#             else:
#                 start_time = int(data[0]['timestamp']) - 3600000 * 20
#                 return get_candles(start_time, data=data)

#         start_time = int((time.time() - 36000) * 1000)
#         data = get_candles(start_time)
        
#         df = pd.DataFrame(data, dtype=float)
#         df.drop(columns=['high_r', 'low_r', 'open_r', 'close_r'], inplace=True)
#         df.drop_duplicates(inplace=True)
#         df.index = pd.to_datetime(df.timestamp, unit='ms')
#         df.sort_index(inplace=True)

#         # INDICATORS

#         # Volume

#         df['volume_ma'] = df.base_volume.rolling(20, min_periods=1).mean()
#         df['volume_stdev'] = df.base_volume.rolling(20, min_periods=1).std()
#         df['volume_thres'] = df.volume_ma + df.volume_stdev * 2

#         # Relative Strength Indicator

#         delta = df.close.diff()
#         up = delta.clip(lower=0)
#         down = -1 * delta.clip(upper=0)
#         ema_up = up.ewm(com=6, adjust=False).mean()
#         ema_down = down.ewm(com=6, adjust=False).mean()
#         rs = ema_up / ema_down
#         df['RSI'] = 100 - (100 / (1 + rs))

#         # LATEST FRAME
#         bar = df.iloc[-1]

#         # ORDERBOOK

#         r = server.orderbook(selling=base_asset, buying=counter_asset).call()
#         orderbook = {
#             'buy': {
#                 'price': 0,
#                 'amount': 0
#             },
#             'sell': {
#                 'price': 0,
#                 'amount': 0
#             },
#             'ask_price': float(r['asks'][0]['price']),
#             'bid_price': float(r['bids'][0]['price'])
#         }
#         for n in range(10):
#             if orderbook['buy']['amount'] < counter_balance / bar.close * 1.01:
#                 orderbook['buy']['price'] = float(r['asks'][n]['price'])
#                 orderbook['buy']['amount'] += float(r['asks'][n]['amount'])
#             if orderbook['sell']['amount'] < base_balance * 1.01:
#                 orderbook['sell']['price'] = float(r['bids'][n]['price'])
#                 orderbook['sell']['amount'] += float(r['bids'][n]['amount'])

#         # ADVICE

#         buy = (
#             bar.base_volume > bar.volume_thres
#             and bar.RSI < 40
#         )

#         sell = (
#             bar.base_volume > bar.volume_thres
#             and bar.RSI > 60
#         )

#         # OPERATIONS

#         if buy:

#             amount = round(
#                 min(
#                     counter_balance / bar.close * .95,
#                     orderbook['buy']['amount'] * .95
#                 ),
#                 7
#             )

#             op = ManageBuyOffer(
#                 selling=counter_asset,
#                 buying=base_asset,
#                 amount=str(amount),
#                 price=str(orderbook['buy']['price']),
#                 offer_id=0
#             )

#             transaction = transaction.append_operation(op).set_timeout(30).build()
#             transaction.sign(keypair)
#             r = server.submit_transaction(transaction)

#         elif sell:

#             for offer_id in [int(x['id']) for x in buy_offers]:
#                 # Delete existing buy offer
#                 op = ManageBuyOffer(
#                     selling=counter_asset,
#                     buying=base_asset,
#                     amount='0',
#                     price=str(orderbook['sell']['price']),
#                     offer_id=offer_id
#                 )
#                 transaction.append_operation(op)

#             if base_balance:
#                 amount = round(
#                     min(
#                         base_balance * .95,
#                         orderbook['sell']['amount'] * .95
#                     ),
#                     7
#                 )

#                 op = ManageSellOffer(
#                     selling=base_asset,
#                     buying=counter_asset,
#                     amount=str(amount),
#                     price=str(orderbook['sell']['price']),
#                     offer_id=0
#                 )
#                 transaction.append_operation(op)

#             if transaction.operations:
#                 transaction = transaction.set_timeout(30).build()
#                 transaction.sign(keypair)
#                 r = server.submit_transaction(transaction)
