from datetime import datetime, timedelta


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
                    try:
                        self.datetime = datetime.strptime(instr, "%Y-%m")
                        self.datetime_type = 4
                    except Exception as e:
                        try:
                            self.datetime = datetime.strptime(instr, "%Y-%m-%d %H:%M:%S.%f")
                            self.datetime_type = 5
                        except Exception as e:

                            try:
                                self.datetime = datetime.fromtimestamp(int(instr))
                                self.datetime_type = 6
                            except Exception as e:
                                timestamp_ms = int(instr)
                                if timestamp_ms < 1000:
                                    raise Exception("timestamp is not timestamp ms. %d" % timestamp_ms)
                                self.datetime = datetime.fromtimestamp(timestamp_ms / 1000)
                                self.datetime_type = 7

        return

    def to_string(self):
        return self.datetime.strftime("%Y-%m-%d %H:%M:%S")

    def get_the_first_day_of_week(self):
        return DateTime((self.datetime - timedelta(days=self.datetime.isocalendar()[2] - 1)).strftime("%Y-%m-%d"))

    def equal(self, d):
        # delta = self.sub(d)
        if self.datetime == d.datetime:
            return True
        return False

    def greater(self, d):
        # delta = self.sub(d)
        if self.datetime > d.datetime:
            return True
        return False

    def sub(self, d):
        return self.datetime - d.datetime

    def fewer(self):
        pass
