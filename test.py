import asyncio
import logging 
import math
import numpy as np
from numpy.typing import NDArray
import pickle
from typing import Optional
import time

from core import DataClient, OrderManager, Client
from core import ORDER_CYLE_END_EVENT, ORDER_TYPE_IOC
logging.basicConfig(level=logging.INFO , format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    o = OrderManager()
    await o.start()  
    
    logging.info("Insert order for AMZN")
    order1 = await o.insert_order(symbol= "AMZN", price = 200, quantity= 1, side = "buy", order_type=ORDER_TYPE_IOC)
    logging.info("Insert order for AAPL")
    order2 = await o.insert_order(symbol= "AAPL", price = 200, quantity= 1, side = "buy", order_type=ORDER_TYPE_IOC)
    logging.info("Insert order for TSLA")
    order3 = await o.insert_order(symbol= "TSLA", price = 200, quantity= 1, side = "buy", order_type=ORDER_TYPE_IOC)

    print(f"Order 1 Id : {order1.order_id}")
    print(f"Order 2 Id : {order2.order_id}")
    print(f"Order 3 Id : {order3.order_id}")

    print("sleep for 10 seconds")
    await asyncio.sleep(10) 

    await o.cancel_all_orders()

loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    logging.info('Stopped (KeyboardInterrupt)')
finally:
    loop.run_until_complete(Client.close_session()) 