import hashlib


class RC4:
    def __init__(self, public_key):
        self.public_key = public_key
        self.public_key = self.public_key.encode("utf-8")
        self.public_key = hashlib.md5(self.public_key).hexdigest()

    def encode(self, text):
        return self.__docrypt(text)

    def decode(self, text):
        return self.__docrypt(text)

    def __docrypt(self, text):

        result = ''
        box = list(range(256))
        randkey = []
        key_lenth = len(self.public_key)

        for i in range(255):
            randkey.append(ord(self.public_key[i % key_lenth]))

        for i in range(255):
            j = 0
            j = (j + box[i] + randkey[i]) % 256
            tmp = box[i]
            box[i] = box[j]
            box[j] = tmp
        for i in range(len(text)):
            a = j = 0
            a = (a + 1) % 256
            j = (j + box[a]) % 256
            tmp = box[a]
            box[a] = box[j]
            box[j] = tmp
            result += chr(ord(text[i]) ^ (box[(box[a] + box[j]) % 256]))
        return result
