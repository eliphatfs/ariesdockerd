import os
import json
import functools
from typing import *
from dataclasses import dataclass


@dataclass
class Config:
    jwt_key: str
    central_host: str
    mount_paths: List[str]
    grafana_endpoint: str
    grafana_userid: int
    grafana_key: str
    policy_pod_time_limit: int
    policy_pod_gpu_limit: int


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
        return Config(**json.load(fi))
