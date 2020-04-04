import re
# https://blog.csdn.net/catkint/article/details/55260281


class EMail(object):
    def __init__(self, email):
        self._email = str(email)
        assert len(self._email) > 7, "mail is too short, %s" % self._email
        # assert re.match("^.+\\@(\\[?)[a-zA-Z0-9\\-\\.]+\\.([a-zA-Z]{2,3}|[0-9]{1,3})(\\]?)$",
        #                self._email) is not None, "mail format is not good: %s" % self._email

        assert re.match(r'^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+){0,4}@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+){0,4}$',
                        self._email) is not None, "mail format is not good: %s" % self._email

    def get_mail(self):
        return self._email


if __name__ == "__main__":
    email = "1-36416465@qq.com"

    EMail(email)

    email = "1216465@qq.com"

    EMail(email)

    try:

        email = "136416465          @qq.com"

        EMail(email)

        raise Exception("no good")
    except Exception:
        pass
