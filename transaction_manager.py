"""
   Copyright 2017 The Foundry Visionmongers Ltd

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import uuid
import os

from pyorbit import OrbitAPI, OrbitSyncSource, OrbitSyncTarget, OrbitSyncPayload, OrbitSyncDirection


class TransactionManager(object):
    def __init__(self, orbit, context, remote_root, local_path):
        super(TransactionManager, self).__init__()
        self.orbit = orbit
        self.context = context
        self.remote_root = remote_root
        self.local_path = os.path.realpath(local_path)
        self.active_transactions = {}

    def normalize(self, path):
        if len(path) > 1 and path.startswith(os.sep):
            path = path[1:]
        return path

    def uploadMulti(self, folders_and_files):
        # Not using iteritems as this changed in python3
        for folder in folders_and_files.keys():
            files = folders_and_files[folder]
            if files:  
                t = (self.normalize(folder), tuple(files))  
                h = hash(t)
                if h in self.active_transactions:
                    logging.debug("{} - Transaction already requested".format(self.active_transactions[h]))
                else:
                    self._do_upload(folder=folder, files=files)

    def _do_upload(self, folder, files):
        transaction_id = str(uuid.uuid4())

        logging.debug("{} - folder {}".format(transaction_id, folder))
        logging.debug("{} - files {}".format(transaction_id, files))
        folder = folder.replace(self.local_path, "")

        folder = self.normalize(folder)

        folders = folder.split(os.sep)

        
        sources = []
        for f in files:
            logging.debug("{} - file {}".format(transaction_id, f))
            filepath = os.path.join(self.local_path, folder, f)
            logging.debug("{} - Filepath: {}".format(transaction_id, filepath))
            sources.append(OrbitSyncSource(
                filepath=filepath,
                transaction_id=transaction_id
            ))

        remote_folder = os.path.join(self.remote_root, os.sep.join(folders))
        logging.debug("{} - Remote folder: {}".format(transaction_id, remote_folder))
        target = OrbitSyncTarget(remote_folder)

        payload = OrbitSyncPayload(
            direction=OrbitSyncDirection.UP,
            sources=sources,
            target=target
        )

        status, response = self.orbit.mounts_sync_post(
            group_id=self.context.group['id'],
            mount_id=self.context.mount['id'],
            payload=payload
        )

        t = (self.normalize(folder), tuple(files))
        h = hash(t)
                
        self.active_transactions[h] = transaction_id

        logging.info("{} - Upload requested. Status: {}".format(transaction_id, status))

    def downloadMulti(self, folders_and_files):
        # Not using iteritems as this changed in python3
        for folder in folders_and_files.keys():
            files = folders_and_files[folder]
            if files:    
                self._do_download(folder=folder, files=files)

    def _do_download(self, folder, files):
        transaction_id = str(uuid.uuid4())
        folder = self.normalize(folder)
        folders = folder.split(os.sep)

        print("{} - remote_root {}".format(transaction_id, self.remote_root))
        print("{} - folders {}".format(transaction_id, folders))
        print("{} - files {}".format(transaction_id, files))
        
        sources = []
        for f in files:
            print("{} - file {}".format(transaction_id, f))
            filepath = os.path.join(self.remote_root, folder, f)
            print("{} - Filepath: {}".format(transaction_id, filepath))
            sources.append(OrbitSyncSource(
                filepath=filepath,
                transaction_id=transaction_id
            ))

        local_folder = os.path.join(self.local_path, os.sep.join(folders))
        print("{} - Local folder: {}".format(transaction_id, local_folder))
        target = OrbitSyncTarget(local_folder)

        payload = OrbitSyncPayload(
            direction=OrbitSyncDirection.DOWN,
            sources=sources,
            target=target
        )

        status, response = self.orbit.mounts_sync_post(
            group_id=self.context.group['id'],
            mount_id=self.context.mount['id'],
            payload=payload
        )

        print("{} - Download requested. Status: {}".format(transaction_id, status))
