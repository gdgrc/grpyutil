import unittest


from grpyutil.common import check_rules


if __name__ == "__main__":
    print(1)
    uid = "622225199110080934"
    check_rules.Identity(uid)

    print(check_rules.gen_identity_check(uid[0:17]))
