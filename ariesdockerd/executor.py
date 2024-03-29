import os
import jwt
import time
import docker
import psutil
import logging
from typing import *
from dataclasses import dataclass
from docker.types import DeviceRequest, Ulimit
from docker.models.containers import Container
from .config import get_config


@dataclass
class ContainerEphemeral:
    logs: bytes
    name: str
    user: str
    entry_creation_time: float


class Executor(object):

    def __init__(self) -> None:
        self.client = docker.from_env()
        self.shared_devices: List[str] = []
        self.mount_paths = get_config().mount_paths
        self.exit_store: Dict[str, ContainerEphemeral] = dict()
        self.mark_removed = set()

    def set_up(self):
        if os.path.exists('/dev/infiniband'):
            self.shared_devices.append('/dev/infiniband:/dev/infiniband')

    def clean_up(self):
        logging.info("Performing clean-up...")
        self.client.containers.prune()
        self.client.networks.prune()
        self.client.images.prune(filters=dict(dangling=False))
        for k, v in list(self.exit_store.items()):
            if time.time() > v.entry_creation_time + 86400 * 7:
                self.exit_store.pop(k)

    def get_managed(self, container: str):
        cont: Container = self.client.containers.get(container)
        if 'ariesmanaged' not in cont.labels:
            raise ValueError("container not managed by executor", container)
        return cont

    def stop(self, container: str):
        return self.get_managed(container).stop()

    def run(self, name: str, image: str, cmd: Union[str, List[str]], gpu_ids: List[int], user: str, env: list):
        gpu_id_string = ','.join(map(str, gpu_ids))
        bookkeep_info = dict(gpu_ids=gpu_ids, user=user)
        token = jwt.encode(bookkeep_info, get_config().jwt_key, "HS256")
        return self.client.containers.run(
            image, cmd,
            name=name,
            hostname=name,
            detach=True,
            devices=self.shared_devices,
            device_requests=[DeviceRequest(device_ids=[gpu_id_string], capabilities=[['gpu']])],
            ulimits=[Ulimit(name='memlock', soft=1048576000, hard=1048576000)],
            shm_size='%dG' % (32 * len(gpu_ids) + 16),
            network_mode='host',
            volumes=self.mount_paths,
            labels={"ariesmanaged": token},
            environment=env
        ).short_id

    def logs(self, container: str):
        return self.get_managed(container).logs()

    def stat(self, container: str):
        return self.get_managed(container).status
    
    def kill(self, container: str):
        c = self.get_managed(container)
        errors = []
        for _ in range(2):
            try:
                top_results = c.top()
                pids = [x[top_results['Titles'].index('PID')] for x in top_results['Processes']]
            except Exception as exc:
                errors.append(repr(exc))
                time.sleep(1)
                continue
            for pid in pids:
                try:
                    psutil.Process(int(pid)).kill()
                except psutil.NoSuchProcess:
                    pass
                except Exception as exc:
                    errors.append(repr(exc))
            time.sleep(1)
        try:
            c.remove(force=True)
        except Exception as exc:
            errors.append(repr(exc))
        self.mark_removed.add(c.short_id)
        if len(errors):
            raise ValueError('\n'.join(errors))

    def scan(self):
        valid: List[Tuple[Container, dict]] = []
        for container in self.client.containers.list(all=True):
            container: Container
            if 'ariesmanaged' in container.labels:
                token = container.labels['ariesmanaged']
                try:
                    info = jwt.decode(token, get_config().jwt_key, algorithms=["HS256"])
                    if container.short_id in self.mark_removed:
                        info['removed'] = True
                    valid.append((container, info))
                except jwt.InvalidTokenError:
                    logging.warning("Invalid Token Found in `ariesmanaged`: %s", token)
        return valid

    def bookkeep(self):
        for container, info in self.scan():
            container: Container
            if container.status == 'exited':
                self.exit_store[container.short_id] = ContainerEphemeral(
                    container.logs(),
                    container.name,
                    info['user'],
                    time.time()
                )
                container.remove()
