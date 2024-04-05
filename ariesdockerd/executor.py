import os
import jwt
import time
import docker
import psutil
import logging
import subprocess
from typing import *
from dataclasses import dataclass
from dateutil.parser import isoparse
from docker.types import DeviceRequest, Ulimit
from docker.models.containers import Container
from docker.errors import NotFound
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

    def get_any(self, container: str):
        cont: Container = self.client.containers.get(container)
        return cont

    def stop(self, container: str):
        c = self.get_managed(container)
        try:
            c.stop()
        except Exception:
            self.get_any(c.name + '-ariesdv0').stop()
            for i in range(4):
                try:
                    if os.path.ismount("/run/ariesdockerd/" + c.name):
                        subprocess.check_call(["umount", "/run/ariesdockerd/" + c.name])
                    break
                except Exception:
                    logging.warning("umount failed, trying umount again in %d", 2 ** i)
                    time.sleep(2 ** i)
            else:
                logging.warning("umount failed, trying force umount")
                try:
                    subprocess.check_call(["umount", "-f", "/run/ariesdockerd/" + c.name])
                except Exception:
                    if os.path.ismount("/run/ariesdockerd/" + c.name):
                        logging.warning("force umount failed, failover")
                    else:
                        logging.warning("mountpoint does not exist?!")
            try:
                self.get_managed(container).stop()
            except NotFound:
                logging.info("container self-terminated after unmount")
            logging.info("successfully re-stopped %s", c.name)
        try:
            self.get_any(c.name + '-ariesdv0').stop()
        except NotFound:
            pass

    def run(self, name: str, image: str, cmd: Union[str, List[str]], gpu_ids: List[int], user: str, env: list, timeout: int):
        gpu_id_string = ','.join(map(str, gpu_ids))
        if timeout <= 0:
            timeout = 2147483647
        bookkeep_info = dict(gpu_ids=gpu_ids, user=user, timeout=timeout)
        token = jwt.encode(bookkeep_info, get_config().jwt_key, "HS256")
        based = "/run/ariesdockerd/" + name
        mountp = based + "/mountp"
        os.makedirs(mountp, exist_ok=True)
        self.client.containers.run(
            'python:3.10',
            [
                "/usr/local/bin/weed", "mount",
                "-replication=001", "-filer=10.8.150.13:8888", "-filer.path=/ariesdv0",
                "-dir=" + mountp
            ],
            name=name + '-ariesdv0',
            hostname=name + '-ariesdv0',
            detach=True,
            remove=True,
            devices=['/dev/fuse:/dev/fuse'],
            cap_add=['SYS_ADMIN'],
            shm_size='16G',
            network_mode='host',
            volumes=[
                "/run/ariesdockerd:/run/ariesdockerd",
                "/etc/seaweedfs:/etc/seaweedfs",
                "/usr/local/bin/weed:/usr/local/bin/weed"
            ]
        )
        for _ in range(200):
            time.sleep(0.1)
            if os.path.ismount(mountp):
                break
        if not os.path.ismount(mountp):
            raise ValueError("mount failed")
        return self.client.containers.run(
            image, cmd,
            name=name,
            hostname=name,
            detach=True,
            devices=self.shared_devices,
            device_requests=[DeviceRequest(device_ids=[gpu_id_string], capabilities=[['gpu']])],
            ulimits=[Ulimit(name='memlock', soft=1048576000, hard=1048576000)],
            shm_size='%dG' % (64 * len(gpu_ids) + 32),
            network_mode='host',
            volumes=[mountp + ":/ariesdv0"],
            labels={"ariesmanaged": token},
            environment=env + (['NCCL_P2P_DISABLE=1'] if len(gpu_ids) < 8 else [])
        ).short_id

    def logs(self, container: str):
        return self.get_managed(container).logs()

    def stat(self, container: str):
        return self.get_managed(container).status
    
    def kill(self, container: str):
        try:
            self.stop(container)
        except Exception:
            pass
        else:
            return
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
            created_str = container.attrs.get("Created")
            if created_str:
                created = isoparse(created_str)
                if time.time() - created.timestamp() > info.get("timeout", 2147483647):
                    try:
                        self.stop(container.short_id)
                    except Exception:
                        try:
                            self.kill(container.short_id)
                        except Exception:
                            logging.exception("book keeping kill failed")
