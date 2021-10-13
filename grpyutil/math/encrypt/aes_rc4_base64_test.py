import unittest


from grpyutil.math import encrypt


if __name__ == "__main__":

    arb = encrypt.AesRC4Base64()
    test_str = "good"
    encode_str = arb.encode(test_str)
    decode_str = arb.decode(encode_str)

    assert decode_str == test_str, "no equal decode: %s, origin: %s" % (decode_str, test_str)

    msg = "[Pass] AesRC4Base64 Test!"
    print(encode_str, msg)

    