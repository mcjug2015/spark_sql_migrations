import logging

# A library must not configure logging when it is imported. A NullHandler on the
# package root keeps our records off logging.lastResort while leaving every
# decision about handlers, levels and formats to the host application, which
# configures logging from its own entry point.
logging.getLogger(__name__).addHandler(logging.NullHandler())
