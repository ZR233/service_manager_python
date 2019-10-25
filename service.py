# @Time    : 2019-10-24 14:02
# @Author  : 周睿
# @Desc    :
import os

from kazoo.client import KazooClient
from kazoo.client import KazooState
import socket
import threading

import time
import json


class Service:
    def __init__(self, service_path, host, zk_hosts):
        self.prefix = "/service"
        self.service_path = service_path
        self.host = host
        self.zk_hosts = zk_hosts
        self.stop_flag = False
        self.__open_zk()

    def __open_zk(self):
        time.sleep(1)
        print("启动zk客户端")
        self.zk = KazooClient(self.zk_hosts)
        self.zk.add_listener(self.connection_listener)
        self.zk.start()

        self.t1 = threading.Thread(target=self.register_service, name="register_service")
        self.t1.setDaemon(True)
        self.t1.start()

    @staticmethod
    def connection_listener(state):
        print(state)

    def register_service(self):
        while True:
            if self.stop_flag:
                return
            root_path = "/".join([self.prefix, self.service_path])
            node_path = "/".join([root_path, self.host])
            if self.zk.state == KazooState.CONNECTED and not self.zk.exists(node_path):
                try:
                    zk_info = {'Pid': os.getpid(), 'Host': self.host, 'HostName': socket.gethostname()}
                    zk_info = json.dumps(zk_info)
                    self.zk.ensure_path(root_path)
                    print("注册服务：" + node_path)
                    self.zk.create(node_path, value=bytes(zk_info, encoding='utf-8'), ephemeral=True)
                    print("注册完成")
                except Exception as err:
                    print(err)
            time.sleep(1)

    def stop(self):
        self.stop_flag = True
        self.zk.stop()
        self.t1.join()
        print("服务注销")











