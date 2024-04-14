import hashlib

def Calculate_hash(file):
    return hashlib.md5(file.read()).hexdigest()

