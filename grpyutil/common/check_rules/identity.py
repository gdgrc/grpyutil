
import datetime
from .identity_code import IDENTITY_CODE
"""
identity check
"""
IDENTITY_WI = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
IDENTITY_AI = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']


class Identity(object):
    def __init__(self, identity):

        self._identity = identity.upper()

        assert len(self._identity) == 18, "lenght is not 18"

        # assert len(self._identity) == 18, "identity is not 18 length"

        self._identity17 = str(identity[:17])

        # assert self._identity17.isdigit(), "identity17 is not digital"

        self._check = gen_identity_check(self._identity17)

        # assert self._check == self._identity[17], "identity check is not correct, origin: %s, gen: %s" % (self._identity[17], self._check)

        self._identity_code = self._identity[0:6]

        assert self._identity_code in IDENTITY_CODE, "identity code is not normal! %s" % (self._identity_code)

        self._identity_birth = self._identity[6:14]

        try:

            self._birth_datetime = datetime.datetime.strptime(self._identity_birth, "%Y%m%d")
        except Exception as e:
            raise Exception("identity code birth is not good! %s, err: %s" % (self._identity_birth, e))

        self._identity_birth_year = self._identity_birth[0:4]

    def get_identity(self):
        return self._identity

    def get_province_code(self):
        return self._identity[0:2]

    def get_city_code(self):
        return self._identity[2:4]

    def get_district_code(self):
        return self._identity[4:6]

    def get_birth_code(self):
        return self._identity_birth

    def get_birth_datetime(self):
        return self._birth_datetime

    def get_sex_code(self):
        sex_code = int(self._identity[16])
        sex_code = 2 if sex_code % 2 == 0 else 1
        return sex_code


"""
def check_identity(identity):

    if len(identity) != 18:
        return False

    last_char = gen_identity_check(identity[:17])
    if last_char != identity[17]:
        return False

    return True
"""


def gen_identity_check(identity17):
    assert len(identity17) == 17, "uid part is not 17 len?"

    sum_num = 0

    i = 0
    while(i <= 16):
        sum_num += (int(identity17[i]) * IDENTITY_WI[i])
        i += 1

    res = sum_num % 11
    last_char = IDENTITY_AI[res]

    return last_char
