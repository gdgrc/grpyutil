from Cryptodome.Cipher import AES
from binascii import b2a_hex, a2b_hex


class Aes():

    def __init__(self, key, iv="", padding_char='\0', mode=AES.MODE_CBC):
        self.aes_key = key
        if not iv:
            iv = key

        self.aes_iv = iv
        self.padding_char = padding_char
        self.mode = mode

    @property
    def CBC_MODE(self):
        return AES.MODE_CBC

    def encode(self, data):
        data_len = len(data)

        # serverinterface_logger.info("type: %s,data: %s, data_len: %d" % (type(data), data, data_len))
        align_length = 16
        padding_len = 0

        padding_len = (align_length - (data_len % align_length))
        tmp_data = data + (self.padding_char * padding_len)

        aes_cryptor = AES.new(self.aes_key, self.mode, self.aes_iv)

        tmp_data = str.encode(tmp_data)
        encrypt_data = aes_cryptor.encrypt(tmp_data)

        # return b2a_hex(encrypt_data)
        return encrypt_data

    def decode(self, data):

        aes_cryptor = AES.new(self.aes_key, self.mode, self.aes_iv)

        tmp_data = data
        # tmp_data = a2b_hex(data)
        tmp_plain_text = aes_cryptor.decrypt(tmp_data).decode('utf8')
        plain_text = tmp_plain_text.rstrip(self.padding_char)

        return plain_text
