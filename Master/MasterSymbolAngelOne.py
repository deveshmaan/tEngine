import urllib.request
import zipfile
import pandas as pd

class MasterTypeVar:
    WITH_CASH = "1"
    WITH_FNO = "2"
    WITH_BOTH_CASH_FUTURE = "3"

class MasterSymbolAngelOne:
    def __init__(self, pathcwd):
        print("Activating Master Trading Symbol...")
        
        self.__pathcwd = pathcwd
        self.__INIT()
        self.__PrepareUrl()
        
    # prepare URL destination address with request parameter    
    def __PrepareUrl(self):
        self.__NFO = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        
        
    def __DownloadMasterFileUsingURL(self, urladdress):
        
        path = None
        try:
            
            print(f"Endpoint: {urladdress}")
            folder_extension = self.GetFileExtension(urladdress)
            
            if folder_extension is None:
                print("Error: Url Input is incorrect or short as expected.")
                return
            
            print(f"Folder Extension: {folder_extension}")
            
            outputpath = self.__actualPath + folder_extension
            
            
            with urllib.request.urlopen(urladdress) as response:
                with open(outputpath, "wb") as out_file:
                    out_file.write(response.read())
                    
            ## call unzip function to unzip the file
            path = self.__AutoUnzipMaster(outputpath)
            
        
        except Exception as e:
            print(f"Error occured while downloading master file : {e}")
            
        return path  # Return the path of the downloaded file
            
    # Knowledge about writing Directory
    
    def __INIT(self):
        self.__INITFolder = "INIT"
        self.__DestinationFolder = "MasterFinvasia"
        
        self.__actualPath = self.__pathcwd + "/" + self.__INITFolder + "/" + self.__DestinationFolder + "/"
        
        print(f"Actual Path: {self.__actualPath}")
        
        self.__pathCash = None
        self.__pathFNO = None
        
        self.__df_cash = pd.DataFrame()
        self.__df_fno = pd.DataFrame()
        
    # Get extension of file from URl
    
    def GetFileExtension(self, urladdress:str):
        
        file_extension = None
        
        try:
            datalist = urladdress.split('/')
            
            if len(datalist) == 4:
                file_extension = datalist[3]  # Get the last part of the URL
            print(f"Data List: {datalist}")
            
        except Exception as e:
            print(f"Error occurred while generating file extension: {e}")
        
        return file_extension
    
    
    # Internally Handle the different master path
    
    def DownloadMaster(self, signal: str) -> None: 
        try:
            
            if signal == MasterTypeVar.WITH_CASH:
                self.__pathCash = self.__DownloadMasterFileUsingURL(self.__NSE)
            elif signal == MasterTypeVar.WITH_FNO:
                self.__pathFNO = self.__DownloadMasterFileUsingURL(self.__NFO)
            elif signal == MasterTypeVar.WITH_BOTH_CASH_FUTURE:
                print("Downloading Both Cash and FNO Master")
                self.__pathCash = self.__DownloadMasterFileUsingURL(self.__NSE)
                self.__pathFNO = self.__DownloadMasterFileUsingURL(self.__NFO)
                print(f"Cash Master Path: {self.__pathCash}, FNO Master Path: {self.__pathFNO}")
            else:
                print("Please provide a valid signal: 'cash' or 'FNO'.")
                return
            
        except Exception as e:
            print(f"Error occurred while downloading master file: {e}")
    
    # Automatic unzip compressed file received from Trading venue
    
    def __AutoUnzipMaster(self, PathOfMaster: str) -> None:
        
        path = None
        
        try:
            # unzip file from given path
            
            print(f"Unzipping master: {PathOfMaster}")
            extract_path = PathOfMaster
            extract_path = extract_path.replace(".zip", "").replace(".txt", "")
            with zipfile.ZipFile(PathOfMaster, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            print(f"Unzipped file at: {extract_path}")  
            path = extract_path
        
        except Exception as e:
            print(f"Error occurred while downloading master file: {e}")
            
        return path
            
     # Read Master text file from local to RAM (internal memory)        
    def ReadAllMastersTextFile(self, signal: str):
        try:
            
            if signal == MasterTypeVar.WITH_CASH:
                if self.__pathCash != None:
                    print("Reading Cash Master")
                    self.__CashMasterOnly()
                
            elif signal == MasterTypeVar.WITH_FNO:
                if self.__pathFNO != None:
                    print("Reading FNO Master")
                    self.__FNOMasterOnly()
                    
            elif signal == MasterTypeVar.WITH_BOTH_CASH_FUTURE:
                print("inside ReadAllMastersTextFile Both Cash and FNO Master")

                if self.__pathCash != None:
                    self.__CashMasterOnly()

                
                if self.__pathFNO != None:
                    print("Reading Both Cash and FNO Master")
                    self.__FNOMasterOnly()
            else:
                print("Please provide a valid signal: 'cash' or 'FNO'.")
                return
            
            
        except Exception as e:
            print(f"Error occurred while reading file from local: {e}")
       
    # Only Read Future and Options Master Text File
    
    def __FNOMasterOnly(self):
        try:
            path = self.__pathFNO + "/" + "NFO_symbols.txt"
            print(f"Reading FNO Master from path: {path}")
            self.__df_fno = pd.read_csv(path)
            # print("Rows in Master File:" + len(self.__df_fno))
        except Exception as e:
            print(f"Error occurred while reading FNO master file: {e}")
            
    # Only Read Cash Master Text File
            
    def __CashMasterOnly(self):
        try:
            path = self.__pathCash + "/" + "NSE_symbols.txt"
            print(f"Reading Cash Master from path: {path}")
            self.__df_cash = pd.read_csv(path)
            # print("Rows in Master File:" + len(self.__df_cash))
        except Exception as e:
            print(f"Error occurred while reading Cash master file: {e}")
            
            
    def GetCashMasterData(self):
        """ Returns Cash Master DataFrame """
        return self.__df_cash
    
    def GetFutureOptionsMasterData(self):
        """ Returns Future and Options Master DataFrame """
        return self.__df_fno