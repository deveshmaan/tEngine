from CredentialTo.CredentialToBrokerAPI import CredentialAngelOne
import InterfaceAngelOne
import Utility.SystemCol as cl
import settings
import pandas as pd
import StrategyEngine.TradingStrategy as ts
import requests
from datetime import date
from pandasai import SmartDataframe
import openai
from langchain_openai import ChatOpenAI
import os



class TradingEngine():
    
    def __init__(self, angelOneInstance=None):
        print("Welcome to Trading Engine...")  
        self._angelOneInstance = InterfaceAngelOne.InterfaceAngelOne()
        # self.df_cash = pd.DataFrame()
        self.df_futureOptions = pd.DataFrame()
    # The code is a Python script with comments. It appears to define a class or a module with methods
    # such as `setup_system_trades()` and `__settings_strategy()`. The `setup_system_trades()` method
    # is being called, and within that method, the `__settings_strategy()` method is being invoked.
    # The code is likely setting up some trading system and configuring strategy settings.
        # self.setup_system_trades()
        # self.__settings_strategy()
        
        
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
            
            if len(self.df_futureOptions) <= 0:
                raise ValueError("future Options is empty.")
            
            token_list = list(self.df_futureOptions['token'])
            
            print(f"Token List Activating Feed: {token_list}")       
              
            # exchangeType = 2 is for NFO
            
            formatted_token_list = [
                {
                    "exchangeType": 2, 
                    "tokens": token_list
                }
            ]
            
            greeksPayload = {
                "name":"NIFTY", 
                "expirydate":"25SEP2025"
                }

            self._angelOneInstance.StartStreamingUsingWebSocket(formatted_token_list)
            # self._angelOneInstance.GetGreeksValue(greeksPayload)
                            
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
    def ActivateMasterSymbolDownloader(self):
        print("Downloading Master File...")
        try:    
              
            url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            masterData = requests.get(url).json() 
            print("Master Data Downloaded Successfully.",masterData)
            self.df_futureOptions = pd.DataFrame.from_dict(masterData)
            print("Master DataFrame from_dict:", self.df_futureOptions)
            # self.df_futureOptions["expiry"] = pd.to_datetime(self.df_futureOptions["expiry"] , format = "mixed").apply(lambda x: x.date()).split("T")[0]
            

            self.df_futureOptions = self.df_futureOptions.astype({"strike": float})
            
            if self.df_futureOptions is None:
                raise ValueError("DataFrame is not initialized.")               
            
            if (len(self.df_futureOptions) <= 0):
                raise ValueError("DataFrame is empty.")
            
            self.df_futureOptions = self.df_futureOptions[ (self.df_futureOptions['name'].isin(settings.futureOption_list) 
                                                            & self.df_futureOptions['expiry'].isin(settings.expiry_list)
                                                            & self.df_futureOptions['instrumenttype'].isin(settings.instrument_list))]
            
            #reset index again from 0
            
            print("Master DataFrame before reset:", self.df_futureOptions)


            self.df_futureOptions = self.df_futureOptions.reset_index(drop=True)
            
            print("Final Option Dataframe after Reset Index:", self.df_futureOptions)


            # includes those expiry date which are mentioned in settings
            self.df_futureOptions = self.df_futureOptions[(self.df_futureOptions['expiry'].isin(settings.expiry_list)) ]
            
            print("Final Option Dataframe after Reset Index expiry:", self.df_futureOptions)
            
            # Assigning Stock Master DataFrame to class variable df_futureOptions

            self.df_futureOptions = self.df_futureOptions[(self.df_futureOptions['strike'] < 3000000) ]

            print("Final Option Dataframe after Reset Index strike:", self.df_futureOptions)
            
            filteredexpiry = self.df_futureOptions['expiry']
            # filteredexpiry = self.df_futureOptions['expiry'].unique()

            print("filteredexpiry: ", filteredexpiry)

            
        except ValueError as e:
            print(f"Pattern matching failed, {e}")
            
        except KeyError as e:
            print(f"coloumn : missing from dataframe", e)
            
        except Exception as e:
            print(f"Failed to apply instruments filter: {e}")
            
            
    def start(self):
        print("Starting function is called...")
        #waiting block
        count = 1
        while (5 > 0):
            if count > 500000:
                count = 0
            count += 1
            
            self.start_trading()
            pass
            
            
       # Conditional to filter out stocks, Index, etc
    
    def conditional_strategy(self):
        try:
            
            print("Applying conditional strategy...self._angelOneInstance.IsConnect()",self._angelOneInstance.IsConnect())
            
            if self._angelOneInstance.IsConnect() == True:
                
                print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^Applying conditional strategy...")
                
                os.environ["OPENAI_API_KEY"] = "your_api_key_here"
            
                # # Connect with OpenAI
                
                llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
                
                df = SmartDataframe(
                    self._angelOneInstance.df_feed,
                    config={
                        "llm": llm
                    }
                )
                            
                # df = SmartDataframe(self._angelOneInstance.df_feed, config={"llm": openai.OpenAI(model=MODEL_NAME, temperature=0, max_tokens=500)})
                print(df.chat("Analyze the dataframe and provide insights."))
            # Df_feed - Record + Validate
                for idx, row in self._angelOneInstance.df_feed.iterrows():
                    
                    # LTP > High -> Conditional Strategy 1
                    _ltp = row['Ltp']
                    
                    if _ltp <= 0:
                        continue
                    
                    _high = row['High']
                    _token = row['Token']
                    
                    if _ltp > _high:
                        print(f"Stocks under SC1:  {_token} :  {_ltp} > {_high}")
                        self.take_new_entry(_token,_ltp)
            
        except Exception as e:
            print(f"Failed to apply conditional strategy: {e}")
                        
          
    def start_trading(self):
        """ Start trading based on the strategies """
        try:
            print("Starting trading...")
            # Implement your trading logic here
    
            self.conditional_strategy()
     
            
        except Exception as e:
            print(f"Failed to start trading: {e}")
            
    def take_new_entry(self, token, price):
        
        """ Take new entry based on the strategies """
        try:
            
            TradingSymbol = ""
            Token = token   # token from df_feed data ( websocket streaming data)
            
            for idx, row in self.df_futureOptions.iterrows():
                masterDataToken = row['token']
                
                if masterDataToken == Token:
                    TradingSymbol = row['symbol']

            
            print("-------------------------------------------------- Taking new entry ------------------------- ", TradingSymbol)
            print("-------------------------------------------------- Taking new entry ------------------------- ", price)

            orderparams = {
                "variety":"NORMAL",
                "tradingsymbol":TradingSymbol,
                "symboltoken":token,
                "transactiontype":"BUY",
                "exchange":"NFO",
                "ordertype":"MARKET",
                "producttype":"INTRADAY",
                "duration":"DAY",
                "price":price,
                "squareoff":"0",
                "stoploss":"0",
                "quantity":"75"
                }
            
            print("-------------------------------------------------- ORDER PARAMS ------------------------- ", orderparams)

            # for STOP LOSS
            
            # orderparams = {
            #     "variety": "STOPLOSS",
            #     "tradingsymbol": tradingsymbol,
            #     "symboltoken": token,
            #     "transactiontype": "BUY",
            #     "exchange": "NFO",
            #     "ordertype": "STOPLOSS_LIMIT",
            #     "producttype": "INTRADAY",
            #     "duration": "DAY",
            #     "price": price,
            #     "squareoff": "0",
            #     "stoploss": "0",
            #     "quantity": "1",
            #     "triggerprice": price + 0.05  # Example trigger price for stop loss
            #     }
            
            
            
            rec_orderid =  self._angelOneInstance.TransmitOrderToBrokerOMS(orderparams)
            
   
           
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
            
            
  