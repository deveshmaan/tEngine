
import pandas as pd

class StrategyHeader:
    STRATEGYNAME: "Strategy"
    SPOT = "Spot"
    EXPIRYDATE = "ExpiryDate"
    STRIKEPRICE = "StrikePrice"
    OPTIONTYPE = "OptionType"
    QUANTITY = "Quantity"
    ATMITMOTM = "AtmItmOtm"

class StrategyEngine:
    def __init__(self):
        
        try:
            print(f"Reading Trading strategy")
            
            col = [StrategyHeader.STRATEGYNAME, StrategyHeader.SPOT, StrategyHeader.EXPIRYDATE, 
                   StrategyHeader.STRIKEPRICE, StrategyHeader.OPTIONTYPE, StrategyHeader.QUANTITY, 
                   StrategyHeader.ATMITMOTM]
            
            self.__df = pd.DataFrame(columns=col)
            self.read_input()
            
        except Exception as e:
            print(f"Error initializing Trading Engine: {e}")


    def read_input(self):
        print(f"Reading input for strategy:")
        
    def get_trading_strategy(self):
        return self.__df
        