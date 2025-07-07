import os

class Path:
 
    @staticmethod
    def GetCurrentDirectory():
        try:
            abs_path = os.getcwd()
            return abs_path
        except Exception as e:
            print(f"Error occurred while getting Relative Path: {e}")