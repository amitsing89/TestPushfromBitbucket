counter = 0
countersingleline = 0
countertotal = 0
counterstring = 0
with open("/home/cloudera/Documents/Source/Phase_1/TaasManager/src/com/bt/taas/httpProcessor/HttpProcessor.java",
          'r') as f:
    for line in f:
        if "/*" in line:
            for line in f:
                counter += 1
                if "*/" in line:
                    break
        elif "//" in line:
            countersingleline += 1
        elif " " " " in line:
            counterstring += 1
        else:
            countertotal += 1
print counter
print countersingleline
print countertotal
print counterstring
