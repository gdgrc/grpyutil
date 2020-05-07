from datetime import datetime


class DateTime(object):
    def __init__(self, instr):
        self.datetime = None
        self.datetime_type = 0

        try:
            self.datetime = datetime.strptime(instr, "%Y-%m-%d %H:%M:%S")
            self.datetime_type = 1
        except Exception as e:

            try:
                self.datetime = datetime.strptime(instr, "%Y-%m-%d %H:%M")
                self.datetime_type = 2
            except Exception as e:
                try:
                    self.datetime = datetime.strptime(instr, "%Y-%m-%d")
                    self.datetime_type = 3
                except Exception as e:
                    print(e)
                    self.datetime = datetime.strptime(instr, "%Y-%m")
                    self.datetime_type = 4

        return

    def to_string(self):
        return self.datetime.strftime("%Y-%m-%d %H:%M:%S")
