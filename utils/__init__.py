import json
from types import SimpleNamespace

def loads_object(s: str | bytes | bytearray):
    return json.loads(s, object_hook=lambda d: SimpleNamespace(**d))
