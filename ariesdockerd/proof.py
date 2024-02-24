import hashlib


def solve(prototype: str, level: int):
    b = prototype.encode('ascii')
    target = '0' * level
    x = 0
    while True:
        h = hashlib.sha256(b + hex(x).encode('ascii')).hexdigest()
        if h[:level] == target:
            return hex(x)
        x += 1


def verify(prototype: str, x: str, level: int):
    return (
        hashlib.sha256(prototype.encode('ascii') + x.encode('ascii')).hexdigest()[:level] == '0' * level
    )
