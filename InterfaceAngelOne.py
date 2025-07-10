from CredentialTo.CredentialToBrokerAPI import CredentialAngelOne
from SmartApi.smartConnect import SmartConnect
import pyotp
from logzero import logger
import datetime
import time
import pandas as pd
from SmartApi.smartWebSocketV2 import SmartWebSocketV2


class InterfaceAngelOne:
    def __init__(self):
        print("*************** Connecting to AngelOne Broker **************")
        # self._shoonyAPi = ShoonyaApiPy()
        self.sws = None
        self._isConnected = False
        self._isWebSocketConnected = False
        self.__set_up_feed()
        self.authToken = None
        self.feedToken = None
        self.action = 1
        self.mode = 1
        self.tokenList = []
        
        
    def __set_up_feed(self):
        feed_col = ['Token', 'TradingSymbol', 'Open', 'High', 'Low', 'Close', 'Ltp', 'Vol', 'Oi']
        self.df_feed = pd.DataFrame(columns=feed_col)
        
        
    def login_panel(self):
        
        totp = self._get_totp_factor()
        
        if totp == -1:
            print("Please restart the application again.")
            return
        
        # convert totp to string if it is not already   
        totp = str(totp)
        api_key = CredentialAngelOne.APIKEY
        username = CredentialAngelOne.USERNAME
        pwd = CredentialAngelOne.PASSWORD
        
        smartApi = SmartConnect(api_key)
        
        correlation_id = CredentialAngelOne.CORRELATION_ID
        
        data = smartApi.generateSession(username, pwd, totp)
        
        print(F"Broker Replied:", data)
        print(F"Connection Established to Broker: ", data['status']) # ok or not ok
        
        
        if data['status'] == False:
            print("Error connecting to AngelOne Broker:")
            
        else:
            print("Successfully connected to Angel One Broker.")
            
            # login api call
            # logger.info(f"You Credentials: {data}")
            self.authToken = data['data']['jwtToken']
            refreshToken = data['data']['refreshToken']
            # fetch the feedtoken
            self.feedToken = smartApi.getfeedToken()
            # fetch User Profile
            res = smartApi.getProfile(refreshToken)
            smartApi.generateToken(refreshToken)
            res=res['data']['exchanges']
        
            self._successfully_connected()
            
        
    def _get_totp_factor(self):
    # This method should return the TOTP factor for two-factor authentication.
    # For now, we will return a placeholder value.
        try:
            print("Please enter the TOTP (6 Digit) Numeric Character:")
            result = input()
            
            check_length_of_input = len(result)
            if check_length_of_input != 6:
                print("TOTP must be 6 digits long. Please try again.")
                return self._get_totp_factor()
            
            result = int(result)
            return result
    

        except Exception as e:
            print(f"Error in _get_totp_factor: {e}")
            return -1
            
    
    def _successfully_connected(self):
        try:
            self._isConnected = True
        except Exception as e:
            print(f"Error in _successfully_connected: {e}")
            

    # confirm client about connection status { Ok, Not_Ok}
    
    def IsConnect(self):
        return self._isConnected
    
    
    # Requesting data from Broker server through API
    # def RequestToBroker(self):
    #     print("Get historical data!")
        
    #     current_date = datetime.datetime.today()
    #     previous_date = current_date - datetime.timedelta(days=1)
    #     uxEntryTime = int(previous_date.timestamp())
        
    #     print(f"Previous Date: {previous_date}, Unix Entry Time: {uxEntryTime}")
        
    #     return_ohlc = self._shoonyAPi.get_time_price_series(
    #         exchange="NSE",
    #         token="RELIANCE",
    #         starttime= str(uxEntryTime),
    #         endtime=None
    #         )
        
    #     print("Historical Data:", return_ohlc)
        
    # connection close forcefully - Logout
    
    def CloseAPI(self): 
        try:
            if self.IsConnect() == False:
                print("Connection already closed.")
                return
            
            result = self._shoonyAPi.logout()
            if result['stat'] == 'Ok':
                print("Successfully disconnected from Finvasia Broker.")
            else:
                print("Failed to disconnect from Finvasia Broker:", result['stat'])
            
        except Exception as e:
            print(f"Error in CloseAPI: {e}")
            
            
    def TransmitOrderToBrokerOMS(self,buy_or_sell,product_type, 
                                 exchange,tradingsymbol,quantity,
                                 discloseqty,price_type, price,
                                 trigger_price, retention, amo,
                                 remarks, bookloss_price, bookprofit_price,
                                 trial_price):
        try:
            print("Sending trade signals to broker OMS...")
            
            
            order_message = self._shoonyAPi.place_order(buy_or_sell,product_type,exchange,
                                                        tradingsymbol,quantity,
                                                        discloseqty,price_type, price,
                                                        trigger_price, retention, amo,
                                                        remarks, bookloss_price, bookprofit_price,
                                                        trial_price)
            
            print("Order Transmitted to broker:", order_message)
            
            if order_message["stat"] == "Ok":
                print("Order transmitted successfully.")
                return int(order_message["norenordno"])
            else:
                print("Failed to transmit order")
                return -1
            
        except Exception as e:
            print(f"Error occured while transmitting trade to OMS: {e}")
            
                
    #application callbacks
    # These functions are called by Broker ( ANGEL BROKER)
    # Run on thread Pool
    
    
    def on_data(self, wsapp, message):
        logger.info("Ticks: {}".format(message))
        # close_connection()

    def on_open(self, wsapp):
        print("on open new")
        
        # token_list = [
        #     {
        #         "exchangeType": 1,
        #         "tokens": ["26009"]
        #     }
        # ]
        
        token_list = self.tokenList
        
        self.sws.subscribe(CredentialAngelOne.CORRELATION_ID , self.mode, token_list)
        # sws.unsubscribe(correlation_id, mode, token_list1)


    def on_error(self, wsapp, error):
        logger.error(error)


    def on_close(wsapp):
        logger.info("Close")



    def close_connection():
        self.sws.close_connection()


    # def _event_handler_order_update(self, message):
    #     print("order event: " + str(message))
        
    
    # def _event_handler_quote_update(self, message):
    #     try:
    #         print("Received quote update from broker------------------------------------------------------")
    #         print("quote event: " + str(message))
    #         # OHLC Data
            
    #         ltp = 0
    #         open = 0
    #         high = 0
    #         low = 0
    #         close = 0
    #         volume = 0
    #         Oi = 0
            
    #         token = message['tk']
            
            
    #         if 'lp' in message:
    #             ltp = float(message['lp'])
                
    #         if 'v' in message:
    #             volume = float(message['v'])
                
    #             print(f"Volume: {volume}")
            
    #         if 'O' in  message:
    #             open = float(message['O'])
                
    #         if 'h' in message:
    #             high = float(message['h'])
                
    #         if 'l' in message:  
    #             low = float(message['l'])
            
    #         if 'c' in message:
    #             close = float(message['c'])
                
    #         if 'oi' in message:
    #             Oi = float(message['oi'])
                
    #         volume = 1
            
    #         if str(token) == str(token):
    #             print(message)
            
    #         #insert - If key ( Token ) Absent 
    #         # we will get Token key always and trading symbol we will get only on 1st tick
            
    #         if token not in self.df_feed['Token'].values:
    #             new_record = { 'Token': token, 
    #                            'TradingSymbol': 'NA',
    #                            'Open': open, 
    #                            'High': high, 
    #                            'Low': low, 
    #                            'Close': close, 
    #                            'Ltp': ltp,  
    #                            'Vol': volume,
    #                            'Oi': Oi,
    #                            }
            
    #             #add new record to df
                            
    #             if ltp > 0: # Sometimes we get invalid token data from broker. This check will prevent adding 0 value to ltp, open etc
    #                 self.df_feed.loc[len(self.df_feed)] = new_record
                
    #         else:
    #             # Update existing record
                
    #             idx = self.df_feed.index[self.df_feed['Token'] == token]
                
    #             if not idx.empty:
    #                 if ltp > 0:
    #                     self.df_feed.at[idx[0], 'Ltp'] = ltp
    #                     print(self.df_feed.tail(3))  # Print the last 3 row to check if it is updated
    
    #                 if open > 0:
    #                     self.df_feed.at[idx[0], 'Open'] = ltp

    #                 if high > 0:
    #                     self.df_feed.at[idx[0], 'High'] = open
                        
    #                 if low > 0:
    #                     self.df_feed.at[idx[0], 'Low'] = high
                        
    #                 if close > 0:
    #                     self.df_feed.at[idx[0], 'Close'] = close
                
    #                 if volume > 0:
    #                     self.df_feed.at[idx[0], 'Vol'] = volume
            
    #     except Exception as e: 
    #         print(f"Error in _event_handler_quote_update: {e}")


    # def _open_callback(self):   
    #     self._ManageWebSocketConnectionOpen() # we have to call this function earlier to know web socket is open or not


    # #end of callbacks

            
    def StartStreamingUsingWebSocket(self, tokenList) -> None:
        try:
            print("Task streaming data <Price, Order> from Trading venue...")
            # Connection Failed 
            self.tokenList = tokenList
            
            if self.IsConnect() == False:
                print("Connection Failure. Please connect To Broker than using streaming function")
                return None
            
            AUTH_TOKEN = self.authToken
            API_KEY =  CredentialAngelOne.APIKEY
            CLIENT_CODE = CredentialAngelOne.USERNAME
            FEED_TOKEN = self.feedToken         
            
            self.sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN)
            
            # Assign the callbacks.
            self.sws.on_open = self.on_open
            self.sws.on_data = self.on_data
            self.sws.on_error = self.on_error
            self.sws.on_close = self.on_close

            self.sws.connect()

            time.sleep(5)  # Allow some time for the WebSocket to establish connection
            print("Streaming request completed")

        except Exception as e:
            print(f"Error starting WebSocket: {e}")
            
       # For Dynamic requirement     
    # def SubscribeTokenToBroker(self, tokenList):
    #     try:
    #         if self.IsWebSocketConnectionOpened() == False:
    #             print("Error !! WebSocket connection failure")
    #             return #Early return
            
    #         if isinstance(tokenList, list):
    #             #handle this function to List Collection
    #             print("Bulk subscribe to Broker...")
    #             self.on_open(tokenList)
                
    #     except Exception as e:
    #         print(f"Error subscribing to token: {e}")
            
    # Is web socket connection is open or not Price()
    
    def _ManageWebSocketConnectionOpen(self):
        try:
            self._isWebSocketConnected = True
        except Exception as e:
            print(f"Error checking WebSocket connection: {e}")
            
    # Allow client(Trading Engine) to know web socket Connected or NOT

    def IsWebSocketConnectionOpened(self):
        return self._isWebSocketConnected
    
    # Get complete order book from trading venue (Broker)
    
    # def GetCompleteOrderBookFromTradingVenue(self):
    #     try:
    #         print("Requesting complete order book from trading venue...")
    #         if self.IsConnect() == False:
    #             print("Connection Failure. Please connect To Broker than use Order Book Request")
    #             return None
            
    #         # Succesfully execute the block
            
    #         getOrderBook = self._shoonyAPi.get_order_book()
            
    #         if getOrderBook == None:
    #             print("No packet received from broker panel")
    #             return
            
    #         #actual data
    #         for trade in getOrderBook:
    #             print(f"Order Book : {trade}")
            
    #         print("Recieved data from Order Book is:", getOrderBook)
            
    #     except Exception as e:
    #         print(f"Error getting complete book from trading venue: {e}")
    #         return None
        
    # def GetExecutedTradeBookFromTradingVenue(self):
    #     try:
    #         print("Requesting executed Trade book from trading venue...")
    #         if self.IsConnect() == False:
    #             print("Connection Failure. Please connect To Broker than use Order Book Request")
    #             return None
            
    #         # Succesfully execute the block
            
    #         getTradeBook = self._shoonyAPi.get_trade_book()
            
    #         if getTradeBook == None:
    #             print("No packet received from broker panel")
    #             return
            
    #         #actual data
    #         for trade in getTradeBook:
    #             print(f"Order Book : {trade}")
            
    #         # count trade available in trade book
            
    #         totalTradeCount = len(getTradeBook)
    #         print("Total executed Trade is:", totalTradeCount)
            
    #     except Exception as e:
    #         print(f"Error getting trade book from trading venue: {e}")
    #         return None
            
    # # Get Net Positions(LIVE) from Trading Venue (Broker)        
            
    # def GetNetPositionsLiveFromTradingVenue(self):
    #     try:
    #         print("Requesting Net Positions from trading venue...")
    #         if self.IsConnect() == False:
    #             print("Connection Failure. Please connect To Broker than use Net Position Request")
    #             return None
            
    #         # Succesfully execute the block
            
    #         getNetPosition = self._shoonyAPi.get_positions()
            
    #         if getNetPosition == None:
    #             print("No packet received from broker panel")
    #             return
            
    #         #actual data
    #         for position in getNetPosition:
    #             print(f"Net Position : {position}")
            
    #         totalTradeCount = len(getNetPosition)
    #         print("Total Rows in Net Position Trade is:", totalTradeCount)
            
    #     except Exception as e:
    #         print(f"Error Occured while getting LIVE net positions from trading venue: {e}")
    #         return None
        
        
    
    # def TransmitExitOrderToBrokerOMS(self, order_id):
    #     try:
    #         print(f"Exiting order with ID: {order_id}")
    #         if self.IsConnect() == False:
    #             print("Connection Failure. Please connect To Broker than use Exit Order Request")
    #             return None
            
    #         # Succesfully execute the block
            
    #         exit_order = self._shoonyAPi.exit_order(order_id)
            
    #         if exit_order == None:
    #             print("No packet received from broker panel")
    #             return
            
    #         print("Exit Order Response:", exit_order)
            
    #     except Exception as e:
    #         print(f"Error Occured while exiting order: {e}")
    #         return None