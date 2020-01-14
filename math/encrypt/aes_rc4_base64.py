import hashlib
from .rc4 import RC4
from .aes import Aes
from binascii import b2a_hex, a2b_hex
import base64


class AesRC4Base64(object):
    """
     aes+rc4+base64
    """

    def __init__(self, key='key for aes333', rc4_key='key for rc4'):
        self.key = key

        # if we can't get special key for rc4, then we use key
        self.rc4 = RC4(rc4_key if rc4_key else self.key)

        # we use key utf8 format as the key for aes
        self.aes_total_key = hashlib.md5(self.key.encode("utf-8")).hexdigest()
        self.aes_iv = self.aes_total_key[16:32].encode('utf-8')
        self.aes_key = self.aes_total_key[:16].encode('utf-8')

        self.length = 16

        self.aes = Aes(self.aes_key, self.aes_iv)

    def encode(self, text):

        aes_text = b2a_hex(self.aes.encode(text))
        ciphertext = self.rc4.encode(aes_text.decode('utf8')).encode('utf8')
        return base64.b64encode(ciphertext).decode('utf8')

    def decode(self, text):
        text = base64.b64decode(text.encode('utf8')).decode('utf8')

        text = self.rc4.decode(text)
        plain_text = self.aes.decode(a2b_hex(text))
        return plain_text


"""
import hashlib
from io import BytesIO
from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex
import base64
import json
import sys
import argparse

class RC4:
  def __init__(self,public_key = None):
    self.public_key = public_key or 'key for rc4'
    self.public_key = self.public_key.encode("utf-8")
    self.public_key = hashlib.md5(self.public_key).hexdigest()

  def encode(self,text):
    return self.__docrypt(text)

  def decode(self,text):
    return self.__docrypt(text)

  def __docrypt(self,text):

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

class ItemCryptor(object):　# aes+rc4+base64
    def __init__(self, public_key=None):
        self.public_key = public_key or 'key for aes333'
        self.rc4 = RC4()
        self.aes_key = hashlib.md5(self.public_key.encode("utf-8")).hexdigest()
        self.iv = self.aes_key[16:32].encode('utf-8')
        self.aes_key = self.aes_key[:16].encode('utf-8')
        self.mode = AES.MODE_CBC
        self.length = 16

    def encode(self, text):
        cryptor = AES.new(self.aes_key, self.mode, self.iv)
        count = len(text)
        if (count % self.length != 0):
            add = self.length - (count % self.length)
        else:
            add = 0
        text = text + ('\0' * add)
        text = str.encode(text)
        ciphertext = b2a_hex(cryptor.encrypt(text))
        ciphertext = self.rc4.encode(ciphertext.decode('utf8')).encode('utf8')
        return base64.b64encode(ciphertext).decode('utf8')

    def decode(self, text):
        text = base64.b64decode(text.encode('utf8')).decode('utf8')
        cryptor = AES.new(self.aes_key, self.mode, self.iv)
        text = self.rc4.decode(text)
        plain_text = cryptor.decrypt(a2b_hex(text)).decode('utf8')
        return plain_text.rstrip('\0')
"""
