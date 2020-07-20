class LittleComplex:
    def __init__(self, name, id, mobno, address):
        self.name = name
        self.id = id
        self.mobno = mobno
        self.address = address

    def printing_Employee(self, name):
        if name == self.name:
            print("Details are as \nName -", self.name, "\nId -", self.id, "\nMobile -", self.mobno, "\nAddress -",
                  self.address)
        else:
            print(name)

    def justPrint(self):
        print("-" * 100)
        print("Details are as \nName -", self.name, "\nId -", self.id, "\nMobile -", self.mobno, "\nAddress -",
              self.address)


c = LittleComplex("amit", 509, 8527123639, "Chinnapanahalli bangalore")
d = LittleComplex("amit", 510, 9457820209, "Chinnapanahalli bangalore")

c.printing_Employee("Check")
c.printing_Employee("amit")
c.justPrint()
d.justPrint()
