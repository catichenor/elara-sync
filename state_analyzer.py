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
import os
import requests
import logging

"""
Analyse differences between local and remote for initial syncing
"""

class StateAnalyzerException(RuntimeError):
    """ Something went wrong during the analysis """
    pass


class StateAnalyzer(object):
    """ Provides the ability to diff local and remote folders """
    def __init__(self, orbit, context, remote_root, local_path, depth):
        super(StateAnalyzer, self).__init__()
        self.orbit = orbit
        self.context = context
        self.remote_root = remote_root
        self.local_path = local_path
        self.depth = depth

    def _generate_local_model(self):
        local_model = {}
        
        # ('/workspace/pickles/bitbucket/elara-sync/target', ['subfolder'], ['.DS_Store', 'numbers.txt', 'simon.txt', 'stuff.txt'])
        # ('/workspace/pickles/bitbucket/elara-sync/target/subfolder', [], [])
        for i in os.walk(self.local_path):
            folder = i[0]
            subfolders = i[1]
            files = i[2]

            relative_folder = os.sep + folder.replace(self.local_path, "")

            #print folder, relative_folder

            for f in files:
                if f[0] == ".":
                    continue

                # Ignore temporary files
                if f.endswith(".tmp"):
                    continue

                if relative_folder in local_model:
                    local_model[relative_folder].append(f)
                else:
                    local_model[relative_folder] = [f,]

        return local_model

    def _generate_remote_model(self, relative_path = "/"):
        """ 
        Complexity here comes from folder recursion. We don't want to have to query every subfolder.
        For now, we support a limited depth.
        """
        remote_model = {}

        # Strip trailing slash
        if len(relative_path) > 1 and relative_path.endswith("/"):
            relative_path = relative_path[:-1]

        if relative_path:
            remote_path = "/".join([self.remote_root, relative_path])
        else:
            remote_path = self.remote_root
        
        # Cleanup double slashes
        remote_path = remote_path.replace("//", "/")

        #logging.debug("Getting remote file listing for {}".format(remote_path))

        depth = relative_path.count("/")
        if depth > self.depth:
            return remote_model

        try:
            status, response = self.orbit.mounts_files_get(
                self.context.group['id'], 
                self.context.mount['id'],
                remote_path
            )
        except requests.exceptions.ChunkedEncodingError as e:
            raise StateAnalyzerException("Error getting remote file listing for {}: {}".format(remote_path, e))

        if status / 100 != 2:
            raise StateAnalyzerException("Error getting remote file listing for {}: {}".format(remote_path, status))

        if 'files' not in response:
            raise StateAnalyzerException("Error empty remote file listing for {}: {}".format(remote_path, response))         

        files = response['files']
        for f in files:
            filename = f['name']

            # Ignore hidden files - could bite us later!
            if filename[0] == ".":
                continue

            # Ignore temporary files
            if filename.endswith(".tmp"):
                continue

            if f['type'] == "DIRECTORY":
                # recurse
                subfolder = "/".join([relative_path, filename])
                subfolder = subfolder.replace("//", "/")
                remote_model.update(self._generate_remote_model(relative_path=subfolder))
                continue

            if relative_path in remote_model:
                remote_model[relative_path].append(filename)
            else:
                remote_model[relative_path] = [filename,]

        return remote_model

    def _do_diff(self, modelA, modelB):
        """ Uni-directional comparison """
        unique = {}
        for kA in modelA.keys():
            # Look for a matching key in modelB
            if kA in modelB.keys():
                list_diff = self._list_diff(modelA[kA], modelB[kA])
                if len(list_diff):
                    unique[kA] = list_diff
            else:
                # Not found so its unique
                unique[kA] = modelA[kA]

        return unique

    def _list_diff(self, listA, listB):
        """ Go Python! """
        return [a for a in listA if a not in listB]

    def diff(self):
        """ Crunch the numbers """
        local_model = self._generate_local_model()
        remote_model = self._generate_remote_model()

        logging.debug("Local Model:  {}".format(local_model))
        logging.debug("Remote Model: {}".format(remote_model))
        return self._do_diff(local_model, remote_model), self._do_diff(remote_model, local_model)
