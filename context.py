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

import json
import logging

class Context(object):
    def __init__(self, group, mount):
        self.group = group
        self.mount = mount

class ContextEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Context):
            return {
                "groupid" : obj.group['id'],
                "mountid" : obj.mount['id']
            }
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

def ContextDecoder(obj):
    return (obj['groupid'], obj['mountid'])

def name_key(a):
    return a['name']

class ContextSelector(object):
    def __init__(self, orbit):
        self.orbit = orbit

    def ask_for_context(self):
        group = self.ask_for_org()
        if not group:
            return None

        mount = self.ask_for_mount(group)
        if not mount:
            return None

        return Context(group, mount)

    def ask_for_org(self):
        logging.debug("Fetching orgs....")
        err, orgs = self.orbit.orgs_get()
        #logging.debug("Orgs: {}".format(orgs))
        if err == 200:
            org = self.select_from_list(orgs['groups'], "name", "orgs")
            return org

    def ask_for_mount(self, group):
        logging.debug("Fetching mounts for group {}: {}".format(group['id'], group['name']))
        err, mounts = self.orbit.mounts_get(group['id'])
        #logging.debug("Mounts: {}".format(mounts))
        if err == 200:
            mount = self.select_from_list(mounts['mounts'], "mountLocation", "mounts")
            logging.debug("Setting mount to {}: {}".format(mount['id'], mount['mountLocation']))
            return mount

    def select_from_list(self, choices, dict_key, display_name):
        print("Please select one of the following {}:".format(display_name))
        # List is not ordered. Lets do that and retain a mapping
        choices_sorted = sorted(choices, key=name_key)

        # Start with one-indexed to be friendly
        i = 1   
        for c in choices_sorted:
            print("{}: {}".format(i, c[dict_key]))
            i += 1
        number_choice = None
        while not number_choice:
            try:
                choice = input("> ")
                number_choice = int(choice)
                if number_choice > 0 and number_choice <= len(choices_sorted):
                    # Convert back to zero-indexed
                    return choices_sorted[number_choice - 1]
            except ValueError:
                # Input is not a number
                pass
            except SyntaxError:
                # Input is empty
                pass

    def find_context(self, context_ids):
        group = self.find_org(context_ids[0])
        if not group:
            return None

        mount = self.find_mount(context_ids[0], context_ids[1])
        if not mount:
            return None

        return Context(group, mount)       

    def find_org(self, org_id):
        logging.debug("Finding Org: {}".format(org_id))
        err, orgs = self.orbit.orgs_get()
        if err == 200:
            for org in orgs['groups']:
                if org['id'] == org_id:
                    logging.info("Found Org: {}".format(org['name']))
                    return org

    def find_mount(self, group_id, mount_id):
        logging.debug("Finding Mount: {}".format(mount_id))
        err, mounts = self.orbit.mounts_get(group_id)
        if err == 200:
            for mount in mounts['mounts']:
                if mount['id'] == mount_id:
                    logging.info("Found Mount: {}".format(mount['name']))
                    return mount