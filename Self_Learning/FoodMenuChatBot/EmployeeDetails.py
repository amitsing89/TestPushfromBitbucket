import json

import redis

connection = redis.Redis(host='localhost', db=0)


class EmployeeDetail:
    def __init__(self, name, ein, ouc):
        self.name = name
        self.ein = ein
        self.ouc = ouc

    def insert(self):
        connection.hset("Employee", self.ein, json.dumps({"Name": self.name, "Ouc": self.ouc}))


a = EmployeeDetail("Amit", 611432986, "TASB")
a.insert()
