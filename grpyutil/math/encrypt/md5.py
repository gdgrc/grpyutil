import hashlib

class Md5():
    def __init__(self,key):
        self.encryptKey = key
    
    def encode(self,data):
        str = data + self.encryptKey
        hl = hashlib.md5()
        hl.update(str.encode(encoding='utf-8'))
        return hl.hexdigest()



