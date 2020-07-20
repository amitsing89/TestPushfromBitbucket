import ConfigParser
import ast

config = ConfigParser.RawConfigParser()
config.read('parameters.cfg')

x = ast.literal_eval(config.get("USERS", "names"))
print(x,type(x))