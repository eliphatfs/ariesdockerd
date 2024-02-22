import os
import json
import functools


@functools.lru_cache(maxsize=None)
def get_config():
    if os.path.exists("config.json"):
        cfg_file = "config.json"
    elif os.path.exists(os.path.expanduser("~/.ariesdockerd/config.json")):
        cfg_file = os.path.expanduser("~/.ariesdockerd/config.json")
    elif os.path.exists("/etc/ariesdockerd/config.json"):
        cfg_file = "/etc/ariesdockerd/config.json"
    else:
        raise FileNotFoundError("Please place config in working directory, `~/.ariesdockerd/config.json` or `/etc/ariesdockerd/config.json`")
    with open(cfg_file) as fi:
        return json.load(fi)
