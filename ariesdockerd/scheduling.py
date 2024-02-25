import random
from operator import itemgetter
from itertools import groupby
from typing import Any, List, Dict
from .error import AriesError


def schedule(available: Dict[Any, List[int]], njobs: int, ngpus: int):
    if ngpus not in [0, 1, 2, 4, 8, 16]:
        raise AriesError(11, "NGPUs should be in [0, 1, 2, 4, 8, 16]", ngpus)
    
    sched = []
    for i in range(njobs):
        min_seg = None
        sel_node = None
        avail_list = list(available.items())
        random.shuffle(avail_list)
        for node, avail in avail_list:
            segs = []
            sa = sorted(avail)
            for _, g in groupby(enumerate(sa), lambda x: x[0] - x[1]):
                segs.append(list(map(itemgetter(1), g)))
            for seg in segs:
                if len(seg) >= ngpus and (min_seg is None or len(seg) < len(min_seg)):
                    min_seg = seg
                    sel_node = node
        if sel_node is None:
            raise AriesError(12, 'avail: %s unschedulable: %s gpu: %s' % (available, njobs - i, ngpus))
        sched.append((sel_node, min_seg[:ngpus]))
        for gpu in min_seg[:ngpus]:
            available[sel_node].remove(gpu)
    return sched
