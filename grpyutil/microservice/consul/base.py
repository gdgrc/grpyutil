import requests
import json
from grpyutil.math.encrypt import aes_rc4_base64


class Consul(object):
    def __init__(self, consul_address, consul_port, meta_encrypt=False):
        self.consul_address = consul_address
        self.consul_port = consul_port

        self.consul_service_url = "http://%s:%d" % (self.consul_address, self.consul_port)

        self.meta_encrypt = meta_encrypt

        if self.meta_encrypt:
            self.arb = aes_rc4_base64.AesRC4Base64()

    def get_health_services(self, service_name):
        result = requests.get("%s/v1/health/service/%s?passing=true" % (self.consul_service_url, service_name))

        result_dict = json.loads(result.text)

        return result_dict

    def get_one_health_service(self, service_name):
        result_dict = self.get_health_services(service_name)
        service = None
        ok = False

        if len(result_dict) > 0:
            service = result_dict[0]["Service"]
            if self.meta_encrypt:

                self.decrypt_meta(service)

            # service_url = "http://%s:%d" % (service["Address"], service["Port"])
            ok = True

        return service, ok

    def decrypt_meta(self, service):
        if "Meta" in service and service["Meta"]:
            meta_keys = service["Meta"].keys()
            if len(meta_keys) == 1 and "config" in meta_keys:
                service["Meta"] = json.loads(self.arb.decode(service["Meta"]["config"]))

    # def get_one_health
