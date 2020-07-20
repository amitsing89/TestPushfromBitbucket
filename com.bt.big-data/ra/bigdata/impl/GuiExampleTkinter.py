from tkinter import *
import redis

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
con = redis.Redis(connection_pool=pool)

# root = tk.Tk()

emp_key = "Employee"
fields = 'Last_Name', 'First_Name', 'Job', 'Country'


def fetch(entries):
    for entry in entries:
        field = entry[0]
        text = entry[1].get()
        entry_dict = {}
        print('%s: "%s"' % (field, text))
        entry_dict[field] = text
        con.hmset(emp_key, entry_dict)


def makeform(roots, fieldsparam):
    entries = []
    for field in fieldsparam:
        row = Frame(roots)
        lab = Label(row, width=15, text=field, anchor='w')
        ent = Entry(row)
        row.pack(side=TOP, fill=X, padx=5, pady=5)
        lab.pack(side=LEFT)
        ent.pack(side=RIGHT, expand=YES, fill=X)
        entries.append((field, ent))
    return entries


if __name__ == '__main__':
    root = Tk()
    ents = makeform(root, fields)
    root.bind('<Return>', (lambda event, e=ents: fetch(e)))
    b1 = Button(root, text='Show',
                command=(lambda e=ents: fetch(e)))
    b1.pack(side=LEFT, padx=5, pady=5)
    b2 = Button(root, text='Quit', command=root.quit)
    b2.pack(side=LEFT, padx=5, pady=5)
    root.mainloop()
