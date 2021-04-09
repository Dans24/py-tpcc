def parseFile(filePath):
    f = open(filePath, "r")
    line = f.readline()
    result = {}
    while line != "":
        key, sep, value = line.partition("=")
        key = key.lower().strip()
        value = value.strip()
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                value = str(value)
        result[key] = value
        line = f.readline()
    return result