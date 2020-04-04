from .identity import *
from .identity_code import *
from .cell_phone import *
from .name import *
from .email import *
from .ip import *
import sys


"""
print(sys.path[0])


with open("./identity_code.txt", "r") as fp:
    for code in fp:
        code = code.strip()
        if len(code) == 6 and code not in IDENTITY_CODE:
            IDENTITY_CODE.append(code)
"""
