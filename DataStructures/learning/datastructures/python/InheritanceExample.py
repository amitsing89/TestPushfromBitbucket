# class User:
#     name = ""
#
#     def __init__(self, name):
#         self.name = name
#
#     def printName(self):
#         print "Name  = " + self.name
#
#
# class Programmer(User):
#     def __init__(self, name):
#         self.name = name
#
#     def doPython(self):
#         print "Programming Python"
#
#
# brian = User("brian")
# brian.printName()
#
# diana = Programmer("Diana")
# diana.printName()
# diana.doPython()

class User:
    name = None
    job = None

    def __init__(self, name, job):
        self.name = name
        self.job = job

    def printName(self):
        print "Name = " + self.name

    def printJob(self):
        print "Job = " + self.job


class Programmer(User):
    language = ""

    def setLanguage(self, language):
        self.language = language

    def printLanguage(self):
        print "Language = " + self.language


guido = Programmer("Guido", "Developer")
guido.setLanguage("Python")
guido.printName()
guido.printLanguage()
guido.printJob()
