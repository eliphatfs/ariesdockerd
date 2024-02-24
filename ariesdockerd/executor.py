import os
import jwt
import time
import docker
import logging
from typing import *
from dataclasses import dataclass
from docker.types import DeviceRequest, Ulimit
from docker.models.containers import Container
from .config import get_config


@dataclass
class ContainerEphemeral:
    logs: str
    name: str
    user: str
    entry_creation_time: float


class Executor(object):

    def __init__(self) -> None:
        self.client = docker.from_env()
        self.shared_devices: List[str] = []
        self.mount_paths = get_config().mount_paths
        self.exit_store: Dict[str, ContainerEphemeral] = dict()

    def set_up(self):
        if os.path.exists('/dev/infiniband'):
            self.shared_devices.append('/dev/infiniband:/dev/infiniband')

    def clean_up(self):
        self.client.containers.prune()
        self.client.networks.prune()
        self.client.images.prune(filters=dict(dangling=False))
        for k, v in list(self.exit_store.items()):
            if time.time() > v.entry_creation_time + 86400 * 7:
                self.exit_store.pop(k)

    def stop(self, container: str):
        return self.client.containers.get(container).stop()

    def run(self, name: str, image: str, cmd: str, gpu_ids: List[int], user: str):
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
            ulimits=[Ulimit(name='memlock', soft='1048576000', hard='1048576000')],
            shm_size='%dG' % (4 * len(gpu_ids)),
            network_mode='host',
            volumes=self.mount_paths,
            labels={"ariesmanaged": token}
        ).short_id

    def logs(self, container_id: str):
        return self.client.containers.get(container_id).logs()

    def stat(self, container_id: str):
        return self.client.containers.get(container_id).status

    def scan(self):
        valid: List[Tuple[Container, dict]] = []
        for container in self.client.containers.list(all=True):
            if 'ariesmanaged' in container.labels:
                token = container.labels['ariesmanaged']
                try:
                    info = jwt.decode(token, get_config().jwt_key)
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
