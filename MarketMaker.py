import asyncio 
import logging 
from typing import Optional
from core import DataClient, OrderManager, Client
from core import ORDER_TYPE_IOC, ORDER_TYPE_DAY, SIDE_BUY, SIDE_SELL

MIDPRICE = "midprice"
WEIGHTEDPRICE = "weightedprice"
LASTTRADEPRICE = "lasttradeprice"
BIDASKPRICE = "bidaskprice"

class MarketMaker:
    def __init__(self, dataclient: DataClient, 
                 ordermanager: OrderManager, 
                 symbol: Optional[str] = None, 
                 margins: float = None, 
                 max_position: int = None,
                 trader_loop_sleep_time : int = 30,
                 tp_loop_sleep_time : int = 10,
                 price_type : str = MIDPRICE,
                 order_type : str = ORDER_TYPE_IOC):
        self._dataclient: DataClient = dataclient
        self._ordermanager: OrderManager = ordermanager
        self._symbol: Optional[str] = symbol
        self._max_position = max_position
        self._margins = margins
        self._buy_price = None
        self._sell_price = None
        self._trader_loop_sleep_time = trader_loop_sleep_time
        self._tp_loop_sleep_time = tp_loop_sleep_time
        self._price_type = price_type
        self._order_type = order_type

    
    def _get_weighted_price(self): 
        quote = self._dataclient.get_last_quote(self._symbol) 
        if quote is None:
            logging.error(f"No quote data available for {self._symbol}.")
            return None
        if  (quote.bid_price > 0) & (quote.ask_price > 0):
            weighted_price = (quote.ask_price * quote.ask_size + quote.bid_price * quote.bid_size) / (quote.ask_size + quote.bid_size)
            return weighted_price
        else:
            return None
        
    def _get_bid_ask_price(self):
        quote = self._dataclient.get_last_quote(self._symbol)
        if quote is None:
            logging.error(f"No quote data available for {self._symbol}.")
            return None
        if (quote.bid_price > 0) & (quote.ask_price > 0):
            bid_price = quote.bid_price
            ask_price = quote.ask_price
            return bid_price, ask_price
        else:
            logging.error(f"Invalid bid/ask prices for {self._symbol}. Bid: {quote.bid_price}, Ask: {quote.ask_price}")
            return None

        
    async def _get_fill_price(self):
        position_object = await self._dataclient.get_position_object_by_symbol(self._symbol)
        if position_object is not None:
            # Convert avg_entry_price from string to float
            return float(position_object['avg_entry_price'])
        else:
            logging.warning(f"No position found for {self._symbol}")
            return None


    async def _trader(self):
        while True:
            Order_ID_trader = []
            pos_qty = self._dataclient.get_position_by_symbol(self._symbol)

            if self._price_type == MIDPRICE:
                price = self._dataclient.get_last_mid_price(self._symbol)  
                logging.info(f"midprice of {self._symbol} is {price}")
            elif self._price_type == WEIGHTEDPRICE:
                price = self._get_weighted_price()
                logging.info(f"weighted price of {self._symbol} is {price}")
            elif self._price_type == LASTTRADEPRICE:
                price = self._dataclient.get_last_trade_price(self._symbol)
                logging.info(f"last trade price of {self._symbol} is {price}")
            elif self._price_type == BIDASKPRICE:
                bid_ask_prices = self._get_bid_ask_price()
                logging.info(f"bid and prices of {self._symbol} are {bid_ask_prices}")

            if self._price_type != BIDASKPRICE: 
                if price is None:
                    logging.error(f"No price info available for symbol {self._symbol}. Skipping this cycle.")
                    await asyncio.sleep(20)  # Sleep before retrying
                    continue
            else:
                if (bid_ask_prices is None):
                    logging.error(f"No price info available for symbol {self._symbol}. Skipping this cycle.")
                    await asyncio.sleep(20)  # Sleep before retrying
                    continue
            
            if self._price_type == BIDASKPRICE:
                bid_price, ask_price = bid_ask_prices
                self._buy_price = round(bid_price - bid_price * self._margins, 2)
                self._sell_price = round(ask_price + ask_price * self._margins, 2)
            else:
                self._buy_price = round(price - price * self._margins, 2)
                self._sell_price = round(price + price * self._margins, 2)

            try:
                if pos_qty == 0:
                    logging.info(f"{self._symbol} has {pos_qty} Net Position, insert buy limit order at {self._buy_price} and sell limit order at {self._sell_price}")
                    long_order = await self._ordermanager.insert_order(symbol=self._symbol, 
                                                                       price=self._buy_price, 
                                                                       quantity=self._max_position, 
                                                                       side=SIDE_BUY, 
                                                                       order_type=self._order_type)
                    await asyncio.sleep(1)  
                    short_order = await self._ordermanager.insert_order(symbol=self._symbol, 
                                                                        price=self._sell_price, 
                                                                        quantity=self._max_position, 
                                                                        side=SIDE_SELL, 
                                                                        order_type=self._order_type)
                    if long_order and long_order.success:
                        Order_ID_trader.append(long_order.order_id)
                    if short_order and short_order.success:
                        Order_ID_trader.append(short_order.order_id)

                # elif pos_qty > 0:
                #     logging.info(f"{self._symbol} has + {pos_qty} Net Position, insert sell limit order at {self._sell_price}")
                #     short_order = await self._ordermanager.insert_order(symbol=self._symbol, 
                #                                                         price=self._sell_price, 
                #                                                         quantity=pos_qty, 
                #                                                         side=SIDE_SELL, 
                #                                                         order_type=self._order_type)
                #     if short_order and short_order.success:
                #         Order_ID_trader.append(short_order.order_id)

                # elif pos_qty < 0:
                #     logging.info(f"{self._symbol} has {pos_qty} Net Position, insert buy limit order at {self._buy_price}")
                #     long_order = await self._ordermanager.insert_order(symbol=self._symbol, 
                #                                                        price=self._buy_price, 
                #                                                        quantity=abs(pos_qty), 
                #                                                        side=SIDE_BUY, 
                #                                                        order_type=self._order_type)
                #     if long_order and long_order.success:
                #         Order_ID_trader.append(long_order.order_id)

            except Exception as e:
                logging.error(f"Error in placing orders: {e}")

            await asyncio.sleep(self._trader_loop_sleep_time)  

            if self._order_type != ORDER_TYPE_IOC:
                for order in Order_ID_trader:
                    try:
                        await self._ordermanager.cancel_order(order)
                    except Exception as e:
                        logging.error(f"Error cancelling order {order}: {e}")


    async def _take_profit(self):
        while True:
            pos_qty = self._dataclient.get_position_by_symbol(self._symbol)          
            Order_ID_TP = []
            try:
                if pos_qty > 0:
                    fill_price = await self._get_fill_price()    
                    take_profit_price = fill_price * (1 + self._margins)
                    take_profit_price = round(take_profit_price, 2)
                    logging.info(f"{self._symbol} has + {pos_qty} Net Position, insert sell take profit order at {take_profit_price} for fill price {fill_price}")
                    short_order = await self._ordermanager.insert_order(symbol=self._symbol, 
                                                                        price=take_profit_price, 
                                                                        quantity=pos_qty, 
                                                                        side=SIDE_SELL, 
                                                                        order_type=self._order_type)
                    if short_order and short_order.success:
                        Order_ID_TP.append(short_order.order_id)

                elif pos_qty < 0:
                    fill_price = await self._get_fill_price()    
                    take_profit_price = fill_price * (1 - self._margins)
                    take_profit_price = round(take_profit_price, 2)
                    logging.info(f"{self._symbol} has {pos_qty} Net Position, insert buy take profit order at {take_profit_price} for fill price {fill_price}")
                    long_order = await self._ordermanager.insert_order(symbol=self._symbol, 
                                                                       price=take_profit_price, 
                                                                       quantity=abs(pos_qty), 
                                                                       side=SIDE_BUY, 
                                                                       order_type=self._order_type)
                    if long_order and long_order.success:
                        Order_ID_TP.append(long_order.order_id)

            except Exception as e:
                logging.error(f"Error in placing take-profit orders: {e}")

            await asyncio.sleep(self._tp_loop_sleep_time)  

            if self._order_type != ORDER_TYPE_IOC:
                for order in Order_ID_TP:
                    try:
                        await self._ordermanager.cancel_order(order)
                    except Exception as e:
                        logging.error(f"Error cancelling take-profit order {order}: {e}")


    async def main(self) -> None:
        
        await asyncio.gather(self._trader(), self._take_profit())


async def MarketMakerBasic():
    i = DataClient(symbols={"AAPL","AMZN","TSLA","NVDA","META", "GOOGL","QCOM","MSFT","NFLX"})
    o = OrderManager()
    await asyncio.sleep(5)  

    AAPL = MarketMaker(dataclient=i, ordermanager=o, symbol="AAPL", margins=0.002, max_position=5, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)
    AMZN = MarketMaker(dataclient=i, ordermanager=o, symbol="AMZN", margins=0.002, max_position=6 , trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)
    TSLA = MarketMaker(dataclient=i, ordermanager=o, symbol="TSLA", margins=0.002, max_position=5, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)
    NVDA = MarketMaker(dataclient=i, ordermanager=o, symbol="NVDA", margins=0.002, max_position=9, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)
    META = MarketMaker(dataclient=i, ordermanager=o, symbol="META", margins=0.002, max_position=2, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)
    GOOGL = MarketMaker(dataclient=i, ordermanager=o, symbol="GOOGL", margins=0.002, max_position=7, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)
    QCOM = MarketMaker(dataclient=i, ordermanager=o, symbol="QCOM", margins=0.002, max_position=5, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)
    MSFT = MarketMaker(dataclient=i, ordermanager=o, symbol="MSFT", margins=0.002, max_position=2, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)
    NFLX = MarketMaker(dataclient=i, ordermanager=o, symbol="NFLX", margins=0.002, max_position=1, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5, price_type = MIDPRICE, order_type= ORDER_TYPE_DAY)

    asyncio.create_task(i.start())
    await o.start()  

    await asyncio.sleep(2)  
    await o.cancel_all_orders()
    await o.close_all_positions()
    await asyncio.sleep(2) 
    await asyncio.gather(AAPL.main(), AMZN.main(),TSLA.main(), NVDA.main(), META.main(), GOOGL.main(), QCOM.main(), MSFT.main(), NFLX.main())

loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(MarketMakerBasic())
except KeyboardInterrupt:
    logging.info('Stopped (KeyboardInterrupt)')
finally:
    loop.run_until_complete(Client.close_session()) 