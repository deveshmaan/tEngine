import InterfaceAngelOne
# from Master.MasterSymbolFinvasia import MasterSymbolFinvasia, MasterTypeVar
import Utility.RealtivePath
import Utility.SystemCol as cl
import settings
import pandas as pd
import StrategyEngine.TradingStrategy as ts


class TradingEngine():
    
    def __init__(self):
        print("Welcome to Trading Engine...")  
        self._angelOneInstance = InterfaceAngelOne.InterfaceAngelOne()
        # self.df_cash = pd.DataFrame()
        self.df_futureOptions = pd.DataFrame()
        self.setup_system_trades()
        self.__settings_strategy()
        
        
    def ConnectToBroker(self):
        print("Connecting to Broker...")
        try:
            self._angelOneInstance.login_panel()
        
        except Exception as e:
            print(f"Failed to Connect To Broker: {e}")
            return -1
    
        
    def StartEngine(self):
        if self._angelOneInstance.IsConnect() == True:
            print("Starting Trading Engine...")
            
            # self.RequestOrderBook()
            # self.RequestExecutedTradeBook()
            # self.RequestNetPositionTradeBook()
            self.ActivateMarketFeed()
            
        else:
            print("Request mechanism failed due to connection Establishment failure.")
            
        self._angelOneInstance.CloseAPI()
        
            
    # Get Market data through X ( any source)
    
    def ActivateMarketFeed(self):
        try:
            print("Activating Market Feed...")

            self._angelOneInstance.StartStreamingUsingWebSocket()
            
            self.subscribe_live_feedFNO()
                
        except Exception as e:
            print(f"Failed to activate market feed: {e}")
            
    # request order book from Trading Venue
            
    def RequestOrderBook(self):
        print("Requesting Order Book from Trading Venue...")
        self._angelOneInstance.GetCompleteOrderBookFromTradingVenue()

    # Request Executed Trade Book from Trading Venue
    
    def RequestExecutedTradeBook(self):
        print("Requesting Trade Book from Trading Venue...")
        self._angelOneInstance.GetExecutedTradeBookFromTradingVenue() 
        
        
    def RequestNetPositionTradeBook(self):
        print("Requesting Net Position from Trading Venue...")
        self._angelOneInstance.GetNetPositionsLiveFromTradingVenue()
        
        
    # Download Master File
    # def ActivateMasterSymbolDownloader(self):
    #     print("Processing Master...")
    #     try:      
    #         getFullPath =  Utility.RealtivePath.Path.GetCurrentDirectory()
    #         print(f"Current Directory: {getFullPath}")
    #         __master = MasterSymbolFinvasia(getFullPath)
    #         __master.DownloadMaster(MasterTypeVar.WITH_FNO)
    #         __master.ReadAllMastersTextFile(MasterTypeVar.WITH_FNO)
                        
    #         # Here we are storing stocks data to df_futureOptions dataframe ( table)
    #         self.df_futureOptions = __master.GetFutureOptionsMasterData()
            
    #         # self.apply_instruments_filter()
    #         self.apply_instruments_filterFNO()
            
    #     except Exception as e:
    #         print(f"Failed to process master symbol: {e}")
            
            
            
    def apply_instruments_filterFNO(self):
        
        """ Filter other series data and include FNO series cluster and index cluster"""
        
        try:
            if self.df_futureOptions is None:
                raise ValueError("DataFrame is not initialized.")               
            
            if (len(self.df_futureOptions) <= 0):
                raise ValueError("DataFrame is empty.")
            
            self.df_futureOptions = self.df_futureOptions[ (self.df_futureOptions['Symbol'].isin(settings.futureOption_list) 
                                     & self.df_futureOptions['OptionType'].isin(settings.option_list)
                                     & self.df_futureOptions['Instrument'].isin(settings.instrument_list)
                                     )]
            
            #reset index again from 0

            self.df_futureOptions = self.df_futureOptions.reset_index(drop=True)

            # includes those expiry date which are mentioned in settings
            self.df_futureOptions = self.df_futureOptions[(self.df_futureOptions['Expiry'].isin(settings.expiry_list)) ]
            self.df_futureOptions = self.df_futureOptions[(self.df_futureOptions['StrikePrice'] < 27000) ]

           
            print("Final Option Dataframe after Reset Index:", self.df_futureOptions)

            # self.df_futureOptions = self.df_futureOptions[ (self.df_futureOptions['Symbol'].str.match('^[^0-9]')) ]
            
            filteredexpiry = self.df_futureOptions['Expiry'].unique()
            
            print("filteredexpiry: ", filteredexpiry)

            
        except ValueError as e:
            print(f"Pattern matching failed, {e}")
            
        except KeyError as e:
            print(f"coloumn : missing from dataframe")
            
        except Exception as e:
            print(f"Failed to apply instruments filter: {e}")
            
    # Subscribe to live feed from cluster dataframe
            
    def subscribe_live_feedFNO(self) -> None:
        """ Subscribe to live feed from cluster dataframe (FNO) """
        try:
            if not isinstance(self._angelOneInstance, InterfaceAngelOne.InterfaceAngelOne):
                raise AttributeError("This method should be accesed through an instance of class.")
                
            if len(self.df_futureOptions) <= 0:
                raise ValueError(" future Options is empty.")
            
            token_list = list(self.df_futureOptions['Token'])
            formatted_token_list = [ "{}|{}".format('NFO', token) for token in token_list ]
            
            print("Subscribing to live feed for FNO...", formatted_token_list)
            
            test_formatted_token_list = [
                {
                    "action": 0,
                    "exchangeType": 1,
                    "tokens": ["26009"]
                }
            ]
            
            #for bulk subscription
            # print("Subscribing to live feed for FNO...", self._angelOneInstance.SubscribeTokenToBroker(formatted_token_list))
            
            self._angelOneInstance.SubscribeTokenToBroker(test_formatted_token_list) 
            
        except Exception as e:
            print(f"Failed to subscribe to live feed for: {e}")
            
            
    def start(self):
        print("Starting function is called...")
        #waiting block
        count = 1
        while (5 > 0):
            if count > 500000:
                count = 0
            count += 1
            
            self.start_trading()
                        
        
    # Conditional to filter out stocks, Index, etc
    
    def conditional_strategy(self):
        try:
            
            if self._angelOneInstance.IsConnect() == True:
            
            # Df_feed - Record + Validate
                for idx, row in self._angelOneInstance.df_feed.iterrows():
                    
                    # LTP > High -> Conditional Strategy 1
                    _ltp = row['Ltp']
                    
                    if _ltp <= 0:
                        continue
                    
                    _high = row['High']
                    _token = row['Token']
                    stockname = row['TradingSymbol']
                    
                    if _ltp > _high:
                        print(f"Stocks under SC1:  {_token} : {stockname} {_ltp} > {_high}")
                        # Implement your strategy logic here
            
        except Exception as e:
            print(f"Failed to apply conditional strategy: {e}")
            
            
        # Doji standard candle strategy
        
    def doji_strategy(self):
        try:
            
            if self._angelOneInstance.IsConnect() == True:
            
            # Df_feed - Record + Validate
            # Token , TradingSymbol, Ltp, High, Low, close, Volume, Open
            
                for idx, row in self._angelOneInstance.df_feed.iterrows():
                    
                    # LTP > High -> Conditional Strategy 1
                    _ltp = row['Ltp']
                    
                    if _ltp <= 0.0:
                        continue
                    
                    _high = row['High']
                    _low = row['Low']
                    _token = row['Token']
                    stockname = row['TradingSymbol']
                    
                    openInterest = row['TradingSymbol']
                    
                    if (_high - _low) <= 0.0:
                        continue
                    
                    _open = row['Open']
                    _close = row['Close']
                    
                    body_length = abs(_open - _close)
                    threshold = 5
                    
                    has_upper_wick = _high > max(_open, _close)
                    has_lower_wick = _low < min(_open, _close)
                    
                    if (body_length <= threshold) and ( (has_upper_wick == True) and ( has_lower_wick == True) ):
                        print(f"Doji Candle Detected: {_token} : {stockname} {_ltp} > {_high}")
                        # Implement your strategy logic here
                        
                        buy_or_sell='B'
                        product_type='C'
                        exchange='NSE'
                        tradingsymbol=stockname # Unique id of contract on which order to be placed. (use url encoding to avoid special char error for symbols like M&M
                        quantity=75
                        discloseqty=0
                        # price_type='MKT', LMT, SL-MKT, SL-LMT
                        # for stop loss = price_type='SL-LMT'
                        price_type='LMT'  
                        price= 0.05  # Price in paise, 100.00 is sent as 10000
                        trigger_price= None
                        retention= 'DAY'
                        amo= None  #Flag for After Market Order, YES/NO
                        remarks= None # client order id or free text
                        bookloss_price= 0.0
                        bookprofit_price= 0.0
                        trial_price= 0.0
                        
                        self.take_new_entry( buy_or_sell, product_type, exchange, tradingsymbol, quantity, discloseqty, 
                                            price_type, price, trigger_price, retention, amo, remarks,
                                            bookloss_price, bookprofit_price, trial_price)
                
            
        except Exception as e:
            print(f"Failed to apply conditional strategy: {e}")
            
     
    # Hammer Bullish standard candle pattern strategy      
      
    def conditional_strategy3(self):
        
        """ Hammer candle Pattern """
        try:
            
            if self._angelOneInstance.IsConnect() == True:
            
            # Df_feed - Record + Validate
            # Token , TradingSymbol, Ltp, High, Low, close, Volume, Open
            
                for idx, row in self._angelOneInstance.df_feed.iterrows():
                    
                    print(f"Classify hammer ( Bullish ) candle pattern...")
                    
                    # LTP > High -> Conditional Strategy 1
                    _ltp = row['Ltp']
                    
                    if _ltp <= 0.0:
                        continue
                    
                    _high = row['High']
                    _low = row['Low']
                    _token = row['Token']
                    stockname = row['TradingSymbol']
                    
                    if (_high - _low) <= 0.0:
                        continue
                    
                    _open = row['Open']
                    _close = row['Close']
                    stockname = row['TradingSymbol']
                    
                    body_length = abs(_open - _close)
                    threshold = 5
                    
                    lower_wick = min(_open, _close) - _low
                    has_large_lower_wick = lower_wick > (2 * body_length)
                    
                    upper_wick = abs(_high - max(_open, _close))
                    has_upper_wick_in_range = upper_wick < (0.5 * body_length)
                    
                    
                    if ( body_length <= threshold) and (has_upper_wick_in_range == True) and (has_large_lower_wick == True):
                        print(f"Stocks under Hammer Candle pattern: {_token} : {stockname} {_ltp} > {_high}")
                        # Implement your strategy logic here
                
            
        except Exception as e:
            print(f"Failed to apply conditional strategy: {e}")
            
            
    # Shooting star bearish reversal candle pattern strategy        
    def conditional_strategy4(self):
        """ Shooting star Bearish reversal candle Pattern """
        
        try:
            if self._angelOneInstance.IsConnect() == True:
            
            # Df_feed - Record + Validate
            # Token , TradingSymbol, Ltp, High, Low, close, Volume, Open
            
                for idx, row in self._angelOneInstance.df_feed.iterrows():
                    
                    print(f"Classify Shooting Star ( Bearish ) candle pattern...")
                    
                    # LTP > High -> Conditional Strategy 1
                    _ltp = row['Ltp']
                    
                    if _ltp <= 0.0:
                        continue
                    
                    _high = row['High']
                    _low = row['Low']
                    _token = row['Token']
                    stockname = row['TradingSymbol']
                    
                    if (_high - _low) <= 0.0:
                        continue
                    
                    _open = row['Open']
                    _close = row['Close']
                    stockname = row['TradingSymbol']
                    
                    body_length = abs(_open - _close)
                    threshold = 5
                    
                    upper_wick = _high - max(_open, _close)
                    has_upper_wick_in_range = upper_wick >= (2 * body_length)
                    
                    lower_wick = min(_open, _close) - _low
                    has_larger_lower_wick = lower_wick < (0.1 * body_length)
                    
                    
                    if ( body_length <= threshold) and (has_upper_wick_in_range == True) and (has_larger_lower_wick == True):
                        print(f"Stocks under Shooting Star Candle pattern: {_token} : {stockname} {_ltp} > {_high}")
                        # Take new entry
                        
        except Exception as e:
            print(f"Failed to apply conditional4 strategy: {e}")
            
    def start_trading(self):
        """ Start trading based on the strategies """
        try:
            print("Starting trading...")
            # Implement your trading logic here
            
            # self.conditional_strategy()
            self.doji_strategy()
            # self.conditional_strategy3()
            # self.conditional_strategy4()
            
        except Exception as e:
            print(f"Failed to start trading: {e}")
            
    def take_new_entry(self,buy_or_sell, product_type, exchange, tradingsymbol, quantity, discloseqty,
                       price_type, price, trigger_price, retention, amo, remarks,
                       bookloss_price, bookprofit_price, trial_price):
        
        """ Take new entry based on the strategies """
        try:
            
            print("Taking new entry...")
            # NSE , 2093, 1, TATATECH,  TATATECH-EQ, EQ, 0.05
            
            rec_orderid =  self._angelOneInstance.TransmitOrderToBrokerOMS(buy_or_sell,product_type,exchange,
                                                                          tradingsymbol,quantity,discloseqty,
                                                                          price_type, price, trigger_price, 
                                                                          retention, amo, remarks, 
                                                                          bookloss_price, bookprofit_price,
                                                                          trial_price)
            
   
           
            if rec_orderid != -1:
                print(f"New entry taken successfully with Order ID: {rec_orderid}")
                
            else:
                print("Failed to take new entry.")
        
            
        except Exception as e:
            print(f"Failed to take new entry: {e}")
            # Forcefully exit the program
            exit()
            
    def take_exist(self):
        """ Take exit based on the strategies """
        try:
            
            id = 1
            print("Taking exit...", {id})
            
            self._angelOneInstance.TransmitExitOrderToBrokerOMS(id)
            # for exit we need index number from system trades dataframe . To get this we need to store data to database
            # Implement your logic to take an exit
            
        except Exception as e:
            print(f"Failed to take exit: {e}")
            
    def update_pnl(self):
        """ Update PnL based on the trades """
        try:
            print("Updating PnL...")
            # Implement your logic to update PnL
            
        except Exception as e:
            print(f"Failed to update PnL: {e}")
            
    
    
    def setup_system_trades(self):
        """ Setup system trades based on the strategies """
        try:
            col_name = [cl.SystemCol.STRATEGY, cl.SystemCol.TRADSYMBOL, 
                        cl.SystemCol.ENTRYPRICE, cl.SystemCol.EXITPRICE, 
                        cl.SystemCol.PNL, cl.SystemCol.LTP]
            
            self.df_system = pd.DataFrame(columns=col_name)
            print(self.df_system)
            
        except Exception as e:
            print(f"Failed to setup system trades: {e}")
            
    # setting storage for trading strategy data
    
    def __settings_strategy(self):
        """ Initialize settings storage for trading strategy data """
        self.df_trading_strategy = pd.DataFrame()
        self.load_trading_strategy()
            
    def load_trading_strategy(self):
        """ Load trading strategy from StrategyEngine """
        try:
            print(f"Loading trading strategy")
            _strategy_engine = ts.StrategyEngine()
            self.df_trading_strategy = _strategy_engine.get_trading_strategy()
            
        except Exception as e:
            print(f"Failed to load trading strategy: {e}")
            
            
     # def apply_instruments_filter(self):
        
    #     """ Filter other series data and include EQ series cluster and index cluster"""
    #     print("Applying instruments filter for Cash Master...")
        
    #     try:
    #         if self.df_cash is None:
    #             raise ValueError("DataFrame is not initialized.")
                
            
    #         if (len(self.df_cash) <= 0):
    #             raise ValueError("DataFrame is empty.")
                
           
    #         self.df_cash = self.df_cash[ (self.df_cash['Instrument'].isin(settings.instrument_list))]
            
            
            
    #         self.df_cash = self.df_cash[ (self.df_cash['Symbol'].str.match('^[^0-9]')) ]
            
    #         self.df_cash = self.df_cash.reset_index(drop=True)
    #         print("Filtered Cash Master DataFrame:", self.df_cash)

            
        # except ValueError as e:
        #     print(f"Pattern matching failed")
            
        # except KeyError as e:
        #     print(f"coloumn : missing from dataframe")
            
        # except Exception as e:
        #     print(f"Failed to apply instruments filter: {e}")
            
    # Subscribe to live feed from cluster dataframe
            
    # def subscribe_live_feed(self) -> None:
        # """ Subscribe to live feed from cluster dataframe (FNO, Cash) """
        # try:
        #     if not isinstance(self._angelOneInstance, InterfaceFinvasia.InterfaceFinvasia):
        #         raise AttributeError("This method should be accesed through an instance of class.")
                
        #     if len(self.df_cash) <= 0:
        #         raise ValueError("Cash master is empty.")
            
        #     token_list = list(self.df_cash['Token'])
        #     formatted_token_list = [ "{}|{}".format('NSE', token) for token in token_list ]
            
            #for bulk subscription
            # self._angelOneInstance.SubscribeTokenToBroker(formatted_token_list)
            
            # task_collection =  list(map(self._angelOneInstance.SubscribeTokenToBroker, formatted_token_list))
            # print(task_collection)
            # print(list(task_collection))
        # except Exception as e:
        #     print(f"Failed to subscribe to live feed for: {e}")
            
            
   
    







