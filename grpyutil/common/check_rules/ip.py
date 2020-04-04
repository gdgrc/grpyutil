class Ipv4(object):
    def __init__(self, ip):
        self._ip = ip

        self._ip_arr = ip.split('.', 4)

        for i in range(4):
            self._ip_arr[i] = int(self._ip_arr[i])

    def get_ip4(self):
        return self._ip_arr[3]
