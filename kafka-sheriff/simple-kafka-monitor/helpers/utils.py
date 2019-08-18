import re
import importlib
import os

class Utils:
    """
    This class handles all the general utility functions
    """

    @staticmethod
    def create_instance(kls, *args):
        """Creates an instance of class from a given class name
            Example usage: create_instance('module1.class1')
            create_instance('class1')
        """
        if '.' in kls:
            parts = kls.split('.')
            module_name = ".".join(parts[:-1])
            if module_name != '':
                # module = __import__(module_name)
                module = importlib.import_module(module_name)
                cls = parts[-1]
                class_ = getattr(module, cls) #(*args)
            else:
                module = importlib.import_module(kls)
                try:
                    class_ = getattr(module, '__init__')
                except AttributeError:
                    raise
            instance = class_(*args)
        else:
            instance = globals()[kls](*args)

        return instance

    @staticmethod
    def strip_char(char, string):
        string = re.sub(char, '', string)
        return string

    @staticmethod
    def get_env_var(key, default, logger):
        if key in os.environ:
            logger.info("Loading {} from ENV variables".format(key))
        else:
            logger.info("Setting {} from config file".format(key, default))
            #logger.info("{} does not exist in ENV. Setting {} from config file".format(key, default))

        val = os.getenv(key, default)
        return val

    @staticmethod
    def do_ssl_certs_exist(files):
        # even if one file doesn't exist return false
        for file_name in files:
            if not os.path.isfile(file_name):
                return False
        return True

    @staticmethod
    def do_files_exist(files, logger = None):
        # even if one file doesn't exist return false
        for file_name in files:
            if logger:
                logger.debug("Checking file {} exists or not...".format(file_name))
            if not os.path.isfile(file_name):
                if logger:
                    logger.error("{} does not exist.".format(file_name))
                return False
        return True

    @staticmethod
    def does_file_exist(file_name):
        return os.path.isfile(file_name)