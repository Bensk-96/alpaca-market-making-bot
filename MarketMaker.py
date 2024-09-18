import asyncio
import logging 
from typing import Optional 
from core import DataClient, OrderManager, Client
from core import ORDER_TYPE_DAY, SIDE_BUY, SIDE_SELL

class MarketMaker:
    def __init__(self, dataclient: DataClient, 
                 ordermanager: OrderManager, 
                 symbol: Optional[str] = None, 
                 margins: Optional[float] = 0.0, 
                 max_position: Optional[int] = None,
                 trader_loop_sleep_time: int = 30, 
                 tp_loop_sleep_time: int = 10):
        self._dataclient: DataClient = dataclient
        self._ordermanager: OrderManager = ordermanager
        self._symbol: Optional[str] = symbol
        self._max_position: Optional[int] = max_position
        self._margins: Optional[float] = margins
        self._buy_price: Optional[float] = None
        self._sell_price: Optional[float] = None
        self._trader_loop_sleep_time: int = trader_loop_sleep_time
        self._tp_loop_sleep_time: int = tp_loop_sleep_time

    async def _get_fill_price(self):
        position_object = await self._dataclient.get_position_object_by_symbol(self._symbol)
        if position_object is not None:
            return float(position_object['avg_entry_price'])
        else:
            logging.warning(f"No position found for {self._symbol}")
            return None # Return None if no position is found
        
    async def _trader(self):
        while True:
            Order_ID_trader = []
            pos_qty = self._dataclient.get_position_by_symbol(self._symbol)

            price = self._dataclient.get_last_mid_price(self._symbol)  

            if price is None:
                logging.error(f"No price info available for symbol {self._symbol}. Skipping this cycle.")
                await asyncio.sleep(20)  # Sleep before retrying
                continue

            logging.info(f"midprice of {self._symbol} is {price}")
            self._buy_price = round(price - price * self._margins, 2)
            self._sell_price = round(price + price * self._margins, 2)

            try:
                if pos_qty == 0:
                    logging.info(f"{self._symbol} has {pos_qty} Net Position, insert buy limit order at {self._buy_price} and sell limit order at {self._sell_price}")
                    long_order = await self._ordermanager.insert_order(symbol=self._symbol, 
                                                                       price=self._buy_price, 
                                                                       quantity=self._max_position, 
                                                                       side=SIDE_BUY, 
                                                                       order_type=ORDER_TYPE_DAY)
                    await asyncio.sleep(1)  
                    short_order = await self._ordermanager.insert_order(symbol=self._symbol, 
                                                                        price=self._sell_price, 
                                                                        quantity=self._max_position, 
                                                                        side=SIDE_SELL, 
                                                                        order_type=ORDER_TYPE_DAY)
                    if long_order and long_order.success:
                        Order_ID_trader.append(long_order.order_id)
                    if short_order and short_order.success:
                        Order_ID_trader.append(short_order.order_id)

            except Exception as e:
                logging.error(f"Error in placing orders: {e}")

            await asyncio.sleep(self._trader_loop_sleep_time)  


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
                if pos_qty != 0:  # Only execute logic if there is a position
                    fill_price = await self._get_fill_price()
                    last_trade_price = self._dataclient.get_last_trade_price(self._symbol)
                    
                    # Ensure valid PnL calculation
                    if fill_price is None or last_trade_price is None:
                        logging.warning(f"No valid fill price or PnL for {self._symbol}. Skipping take-profit for this cycle.")
                        await asyncio.sleep(self._tp_loop_sleep_time)
                        continue

                    # Calculate PnL
                    pnl = (last_trade_price / fill_price - 1) if pos_qty > 0 else (1 - last_trade_price / fill_price)
                    logging.info(f"PnL of {self._symbol} is {pnl}. Ltp is {last_trade_price} and fill price of {'long' if pos_qty > 0 else 'short'} position is {fill_price}")


                        

                    # Calculate take-profit price
                    take_profit_price = fill_price * (1 + self._margins) if pos_qty > 0 else fill_price * (1 - self._margins)
                    take_profit_price = round(take_profit_price, 2)
                    side = SIDE_SELL if pos_qty > 0 else SIDE_BUY
                    logging.info(f"Inserting {'sell' if pos_qty > 0 else 'buy'} take-profit order at {take_profit_price} for {self._symbol}. Fill Price was {fill_price}")

                    # Place take-profit order
                    tp_order = await self._ordermanager.insert_order(
                        symbol=self._symbol, 
                        price=take_profit_price, 
                        quantity=abs(pos_qty), 
                        side=side, 
                        order_type=ORDER_TYPE_DAY
                    )
                    if tp_order and tp_order.success:
                        Order_ID_TP.append(tp_order.order_id)
            
            except Exception as e:
                logging.error(f"Error in placing take-profit orders: {e}")

            await asyncio.sleep(self._tp_loop_sleep_time)

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

    AAPL = MarketMaker(dataclient=i, ordermanager=o, symbol="AAPL", margins=0.002, max_position=5, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)
    AMZN = MarketMaker(dataclient=i, ordermanager=o, symbol="AMZN", margins=0.002, max_position=6 , trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)
    TSLA = MarketMaker(dataclient=i, ordermanager=o, symbol="TSLA", margins=0.002, max_position=5, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)
    NVDA = MarketMaker(dataclient=i, ordermanager=o, symbol="NVDA", margins=0.002, max_position=9, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)
    META = MarketMaker(dataclient=i, ordermanager=o, symbol="META", margins=0.002, max_position=2, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)
    GOOGL = MarketMaker(dataclient=i, ordermanager=o, symbol="GOOGL", margins=0.002, max_position=7, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)
    QCOM = MarketMaker(dataclient=i, ordermanager=o, symbol="QCOM", margins=0.002, max_position=5, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)
    MSFT = MarketMaker(dataclient=i, ordermanager=o, symbol="MSFT", margins=0.002, max_position=2, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)
    NFLX = MarketMaker(dataclient=i, ordermanager=o, symbol="NFLX", margins=0.002, max_position=1, trader_loop_sleep_time = 15, tp_loop_sleep_time= 5)

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

