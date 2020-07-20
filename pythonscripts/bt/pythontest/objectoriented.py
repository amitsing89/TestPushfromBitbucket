class BankAccount:
    def __init__(self):
        self.balance = 0

    def withdraw(self, amount):
        self.balance -= amount
        print self.balance
        return self.balance

    def deposit(self, amount):
        self.balance += amount
        print self.balance
        return self.balance

a = BankAccount()
b = BankAccount()
# deposit(a, 100)
a.deposit(100)
# deposit(b, 50)
b.deposit(50)
# withdraw(b, 10)
b.withdraw(10)
# withdraw(a, 10)
a.withdraw(10)