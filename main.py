import TradingEngine as te


if __name__ == "__main__": # main - first method
    handler = te.TradingEngine()
    # handler.ActivateMasterSymbolDownloader()
    handler.ConnectToBroker()
    # handler.StartEngine()
    handler.start()
    
    print("This is the main module.")
    # You can add more functionality here if needed.