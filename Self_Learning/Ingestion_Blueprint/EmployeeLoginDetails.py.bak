import redis

red = redis.Redis(host='localhost')
x = raw_input("Enter Name\n")
n = red.set("Name", str(x))
y = raw_input("Enter Password\n")
p = red.set("Password", str(y))
z = raw_input("Enter Email\n")
e = red.set("Email", str(z))
a = raw_input("Enter Name and Password\n")
b = a.split(" ")
name = b[0]
password = b[1]
if name == red.get(str("Name")):
    if password == red.get(str("Password")):
        print "FOUND name-", n, "Password-", p, "Email-", e
        print "Warning", red.get("point")
    else:
        print "Password Error"
else:
    print "Username Error"
