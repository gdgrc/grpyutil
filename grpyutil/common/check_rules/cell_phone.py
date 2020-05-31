cell_code_list = ["13", "14", "15", "16", "17", "18", "19"]


class CellPhone(object):
    def __init__(self, cell):
        self._cell_phone = int(cell)
        self._cell_phone_string = str(cell)

        assert len(self._cell_phone_string) == 11, "cell is not length 11, %s" % self._cell_phone_string

        self._cell_phone_code = self._cell_phone_string[0:2]
        assert self._cell_phone_code in cell_code_list, "cell phone code: %s not in code_list" % self._cell_phone_code

    def get_cell(self):
        return self._cell_phone_string

    def get_area_code(self):
        return self._cell_phone_string[3:7]

    def get_net_code(self):
        return self._cell_phone_string[0:3]


if __name__ == "__main__":
    cell = "13513512243"
    CellPhone(cell)
