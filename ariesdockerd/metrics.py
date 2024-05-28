import time
import psutil
import GPUtil
import socket
import logging
import requests
import functools
import threading
from .config import get_config


def error_to_log(thmain):
    @functools.wraps(thmain)
    def wrapped():
        while not server_end:
            try:
                thmain()
            except Exception:
                logging.exception('Caught exception in metrics push service')
    return wrapped


def report(name: str, val: float, **extra_labels: dict):
    cfg = get_config()
    extra_labels = dict(extra_labels)
    extra_labels['source'] = 'ariesmond'
    extra_labels['host'] = host
    body = '%s,%s metric=%f' % (
        name,
        ','.join(['%s=%s' % (k, v) for k, v in extra_labels.items()]),
        val
    )
    response = requests.post(
        cfg.grafana_endpoint, 
        headers = {'Content-Type': 'text/plain'},
        data = str(body),
        auth = (cfg.grafana_userid, cfg.grafana_key)
    )
    response.raise_for_status()


host = socket.gethostname()
server_end = False


@error_to_log
def report_cpu_percent():
    cpu = psutil.cpu_percent(15)
    report('ariesmond.nodes.cpu.percent', cpu)


@error_to_log
def report_memory_usage():
    time.sleep(15)
    mi = psutil.virtual_memory()
    report('ariesmond.nodes.memory.total', mi.total)
    report('ariesmond.nodes.memory.available', mi.available)
    report('ariesmond.nodes.memory.free', mi.free)
    report('ariesmond.nodes.memory.used', mi.used)
    report('ariesmond.nodes.memory.percent', mi.percent)


@error_to_log
def report_network_usage():
    interval = 15
    net_io_s = psutil.net_io_counters(pernic=True, nowrap=True)
    sent_s = sum(nic.bytes_sent for name, nic in net_io_s.items() if name != 'lo')
    recv_s = sum(nic.bytes_recv for name, nic in net_io_s.items() if name != 'lo')
    time.sleep(interval)
    net_io_e = psutil.net_io_counters(pernic=True, nowrap=True)
    sent_e = sum(nic.bytes_sent for name, nic in net_io_e.items() if name != 'lo')
    recv_e = sum(nic.bytes_recv for name, nic in net_io_e.items() if name != 'lo')
    psutil.net_io_counters.cache_clear()
    report('ariesmond.nodes.net.up_bw', (sent_e - sent_s) / interval)
    report('ariesmond.nodes.net.down_bw', (recv_e - recv_s) / interval)


@error_to_log
def report_disk_usage():
    logging.debug("Entering Disk Report")
    interval = 15
    disk_io_s = psutil.disk_io_counters(nowrap=True)
    time.sleep(interval)
    disk_io_e = psutil.disk_io_counters(nowrap=True)
    psutil.disk_io_counters.cache_clear()
    logging.debug("Running Disk Report")
    report('ariesmond.nodes.disk.write_bw', (disk_io_e.write_bytes - disk_io_s.write_bytes) / interval)
    report('ariesmond.nodes.disk.read_bw', (disk_io_e.read_bytes - disk_io_s.read_bytes) / interval)
    report('ariesmond.nodes.disk.write_iops', (disk_io_e.write_count - disk_io_s.write_count) / interval)
    report('ariesmond.nodes.disk.read_iops', (disk_io_e.read_count - disk_io_s.read_count) / interval)


@error_to_log
def report_gpu_usage():
    logging.debug("Entering GPU Report")
    time.sleep(15)
    logging.debug("Running GPU Report")
    for gpu in GPUtil.getGPUs():
        report('ariesmond.nodes.gpu.memory.percent', gpu.memoryUsed / gpu.memoryTotal, gpu=gpu.id)
        report('ariesmond.nodes.gpu.load.percent', gpu.load, gpu=gpu.id)


def main():
    # logging.basicConfig(level=logging.DEBUG)
    global server_end
    subs = [
        threading.Thread(target=report_cpu_percent, daemon=True),
        threading.Thread(target=report_memory_usage, daemon=True),
        threading.Thread(target=report_network_usage, daemon=True),
        threading.Thread(target=report_disk_usage, daemon=True),
        threading.Thread(target=report_gpu_usage, daemon=True),
    ]
    for sub in subs:
        sub.start()
    try:
        for sub in subs:
            while sub.is_alive():
                sub.join(0.5)
    except KeyboardInterrupt:
        server_end = True
        raise
