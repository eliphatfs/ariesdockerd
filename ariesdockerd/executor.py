import os
import docker
from typing import *
from docker.types import DeviceRequest, Ulimit
from .config import get_config


class Executor(object):

    def __init__(self) -> None:
        self.client = docker.from_env()
        self.shared_devices: List[str] = []
        self.mount_paths = get_config().mount_paths

    def set_up(self):
        if os.path.exists('/dev/infiniband'):
            self.shared_devices.append('/dev/infiniband:/dev/infiniband')

    def clean_up(self):
        self.client.containers.prune()
        self.client.networks.prune()
        self.client.images.prune(filters=dict(dangling=False))

    def run(self, name: str, image: str, cmd: str, gpu_ids: List[int]):
        gpu_id_string = ','.join(map(str, gpu_ids))
        return self.client.containers.run(
            image, cmd,
            name=name,
            hostname=name,
            detach=True, remove=True,
            devices=self.shared_devices,
            device_requests=[DeviceRequest(device_ids=[gpu_id_string], capabilities=[['gpu']])],
            ulimits=[Ulimit(name='memlock', soft='1048576000', hard='1048576000')],
            shm_size='%dG' % (4 * len(gpu_ids)),
            network_mode='host',
            volumes=self.mount_paths
        ).short_id

    def logs(self, container_id: str):
        return self.client.containers.get(container_id).logs()

    def stat(self, container_id: str):
        return self.client.containers.get(container_id).status
