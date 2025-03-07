"""
@version: v2.0
@author: Kandy.Ye
@contact: Kandy.Ye@outlook.com
@file: triangle_arbitrage.py
@time: 星期一 2025/2/17 8:58
"""

import asyncio
import os
import logging
from typing import Dict, Any

if os.name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import ccxt.pro

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TriangleArbitrage:
    def __init__(self, exchange: str, apikey: str, secret: str, principal: int = 10000, profit_margin: float = 0.01):
        """
        三角套利
        :param exchange: 交易所名称 小写
        :param apikey: apikey
        :param secret: secret
        :param principal: 本金  用于计算，默认10000，不需要实际金额
        :param profit_margin: 预期利润 小数
        """
        self.exchange = exchange
        self.apikey = apikey
        self.secret = secret
        self.profit_info = []
        self.principal = principal
        self.profit = profit_margin * principal
        self.logger = logger

    async def _execute_forward_arbitrage(self, client: Any, symbol: str, usdt_balance: float) -> Dict[str, Any]:
        """执行正向套利：USDT -> 代币 -> BTC -> USDT"""
        try:
            # 1. 购买代币
            base_order = await client.create_market_buy_order_with_cost(f"{symbol}/USDT", usdt_balance)
            fee = sum(item['cost'] for item in base_order['fees'])
            base_amount = base_order['amount'] - fee

            # 2. 卖代币成BTC
            btc_order = await client.create_market_sell_order(f"{symbol}/BTC", base_amount)

            # 3. 卖出BTC
            usdt_order = await client.create_market_sell_order("BTC/USDT", btc_order['cost'])

            profit = usdt_order['cost'] - float(usdt_balance)
            return {
                'symbol': symbol,
                'profit': profit,
                'trace': f"USDT => {symbol} => BTC => USDT"
            }
        except Exception as e:
            self.logger.error(f"执行正向套利失败: {str(e)}")
            raise

    async def _execute_reverse_arbitrage(self, client: Any, symbol: str, usdt_balance: float) -> Dict[str, Any]:
        """执行反向套利：USDT -> BTC -> 代币 -> USDT"""
        try:
            # 1. 购买BTC
            btc_order = await client.create_market_buy_order_with_cost("BTC/USDT", usdt_balance)
            btc_amount = btc_order['amount'] - btc_order['fee']['cost']

            # 2. 购买代币
            base_order = await client.create_market_buy_order_with_cost(f"{symbol}/BTC", btc_amount)
            base_amount = base_order['amount'] - base_order['fee']['cost']

            # 3. 卖出代币
            usdt_order = await client.create_market_sell_order(f"{symbol}/USDT", base_amount)

            profit = usdt_order['cost'] - float(usdt_balance)
            return {
                'symbol': symbol,
                'profit': profit,
                'trace': f"USDT => BTC => {symbol} => USDT"
            }
        except Exception as e:
            self.logger.error(f"执行反向套利失败: {str(e)}")
            raise

    async def main(self) -> Dict[str, Any] | None:
        exchange_class = getattr(ccxt.pro, self.exchange)
        client = exchange_class({
            'apiKey': self.apikey,
            'secret': self.secret,
            'timeout': 30000,
            'enableRateLimit': True
        })

        try:
            markets = await client.load_markets()
            symbols = [
                market['base'] for market in markets.values()
                if market['active'] and market['spot'] and market['quote'] == 'BTC'
                   and f"{market['base']}/USDT" in markets
            ]

            self.logger.info(f"找到{len(symbols)}个符合条件的交易对")

            for i, symbol in enumerate(symbols):
                try:
                    # 获取市场价格
                    usdt_ticker = await client.fetch_ticker(f"{symbol}/USDT")
                    btc_ticker = await client.fetch_ticker("BTC/USDT")
                    symbol_ticker = await client.fetch_ticker(f"{symbol}/BTC")

                    # 计算套利收益
                    forward_profit = (btc_ticker['last'] * symbol_ticker['last'] *
                                      self.principal / usdt_ticker['last']) - self.principal
                    reverse_profit = (usdt_ticker['last'] * self.principal /
                                      (btc_ticker['last'] * symbol_ticker['last'])) - self.principal

                    # 执行套利
                    if forward_profit > self.profit:
                        balance = await client.fetch_balance()
                        usdt_balance = balance['USDT']['free']
                        result = await self._execute_forward_arbitrage(client, symbol, usdt_balance)
                        self.profit_info.append(result)

                    if reverse_profit > self.profit:
                        balance = await client.fetch_balance()
                        usdt_balance = balance['USDT']['free']
                        result = await self._execute_reverse_arbitrage(client, symbol, usdt_balance)
                        self.profit_info.append(result)

                    if i % 5 == 0:
                        await asyncio.sleep(2)

                except Exception as e:
                    self.logger.error(f"处理{symbol}时发生错误: {str(e)}")
                    await asyncio.sleep(10)
                    continue

            return {
                'symbols': self.profit_info,
                'result': "success"
            }

        finally:
            await client.close()

if __name__ == '__main__':

    af = TriangleArbitrage(
        exchange="binance",
        apikey=os.environ.get("APIKEY"),
        secret=os.environ.get("SECRET"),
        principal=100,
        profit_margin=0.04
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(af.main())