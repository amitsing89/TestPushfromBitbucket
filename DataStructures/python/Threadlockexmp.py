from threading import Lock


class make_withdraw:
    def __init__(self, balance):
        self.balance = balance
        self.balance_lock = Lock()

    def withdraw(self, amount):
        self.balance_lock.acquire()  # once successful, enter the critical section
        if amount > self.balance:
            print ("Insufficient funds")
        else:
            self.balance = self.balance - amount
            print(self.balance)  # upon exiting the critical section, release the lock

        self.balance_lock.release()

    def add(self, balance_new):
        self.balance = self.balance + balance_new


w = make_withdraw(10)
w.withdraw(8)
w.withdraw(7)
w.add(20)
w.withdraw(8)
w.withdraw(7)
w.withdraw(8)
