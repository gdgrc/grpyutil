import logging as offical_logging


class Logging(object):

    def __init__(self):
        self.CRITICAL = offical_logging.CRITICAL
        self.FATAL = offical_logging.FATAL
        self.ERROR = offical_logging.ERROR
        self.WARNING = offical_logging.WARNING
        self.WARN = offical_logging.WARN
        self.INFO = offical_logging.INFO
        self.DEBUG = offical_logging.DEBUG
        self.NOTSET = offical_logging.NOTSET

    def debug(self, msg, *args, **kwargs):
        offical_logging.debug(msg, *args, **kwargs)
        # print(args)
        # print(kwargs)
        # print(msg % args)

    def info(self, msg, *args, **kwargs):
        offical_logging.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        offical_logging.error(msg, *args, **kwargs)

    def basicConfig(self, **kwargs):
        offical_logging.basicConfig(**kwargs)

    def shutdown(self):
        offical_logging.shutdown()

    def clear(self):
        offical_logging.shutdown()
        for handler in offical_logging.root.handlers[:]:
            offical_logging.root.removeHandler(handler)


logging = Logging()
