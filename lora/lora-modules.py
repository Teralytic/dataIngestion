""" Utility to load device keys from manufacturing CSV files into Loraserver.
Every time we receive a Lora CSV file, we should run this script
to assign keys that were not assigned because a manufacturing jig was offline.
This script will be used even after we upgrade to sync code on the jigs.
If we ever need to recover using backup CSV's, this will be the script to use.
"""

import os.path, csv, sys
import json
import ssl
import certifi
import urllib3
import logging
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
cf = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(cf)
logger.addHandler(ch)

http = urllib3.PoolManager(cert_reqs=ssl.CERT_NONE)
host = "https://loraweb.teralytic.io:8080"
applicationID = 34

class Loraserver:
    def __init__(self):
        data = {'password': 't3ralyt1c', 'username': 'admin'}
        self.token = self.post_hdrs('/api/internal/login', data, headers={'Content-Type': 'application/json'})
        logger.debug('token = {}'.format(self.token))

    def post_hdrs(self, path, data, method='POST', headers=None):
        url = "{}{}".format(host, path)
        encoded_data = json.dumps(data).encode('utf-8')
        logger.debug('encoded_data = {}'.format(encoded_data))

        if headers is None:
            headers = {'Content-Type': 'application/json',
                        'Grpc-Metadata-Authorization': self.token['jwt']}
        r = http.request(method, url,
            body=encoded_data,
            headers=headers)
        logger.debug('r = {}'.format(r.data))
        return json.loads(r.data.decode('utf-8'))

    def post(self, path, data, method='POST'):
        return self.post_hdrs(path, data, method=method)

    def get(self, path):
        url = "{}{}".format(host, path)
        logger.debug("Calling {}".format(url))
        r = http.request('GET', url,
            headers={'Content-Type': 'application/json',
                        'Grpc-Metadata-Authorization': self.token['jwt']})
        logger.debug('r = {}'.format(r.data))
        return json.loads(r.data.decode('utf-8'))

    def get_device(self, devEUI):
        logger.debug('Getting device {}'.format(devEUI))
        node = self.get('/api/devices/{}'.format(devEUI))
        if 'error' in node:
            # logger.error(node['error'])
            if node['error']=='object does not exist':
                return None
            else:
                raise node['error']
        return node

    def get_key(self, devEUI):
        logger.debug('Getting key for device {}'.format(devEUI))
        node = self.get('/api/devices/{}/keys'.format(devEUI))
        if 'error' in node:
            # logger.error(node['error'])
            if node['error']=='object does not exist':
                return None
            else:
                raise node['error']
        return node['deviceKeys']['nwkKey']

    def write_key(self, devEUI, nwkKey, method='POST'):
        data = {
            'deviceKeys': {
                'devEUI': devEUI,
                'nwkKey': str(nwkKey)
            }
        }
        r = self.post('/api/devices/'+devEUI+'/keys', data, method=method)
        if 'error' in r:
            raise r['error']
        return r

    def update_key(self, devEUI, nwkKey):
        self.write_key(devEUI, nwkKey, method='PUT')

    def write_device(self, devEUI, nwkKey):
        data = {
            'device': {
                'applicationID': applicationID,
                'description': 'Agricultural sensor',
                'devEUI': devEUI,
                'deviceProfileID': 'd0468204-0436-4275-b2a1-7967a41f5fa8',
                'name': devEUI[9:16]
            }
        }
        r = self.post('/api/devices', data)
        if 'error' in r:
            raise r['error']

        return self.write_key(devEUI, nwkKey)

def read_csv(csv_name):
    """Reads and de-duplicates Loraserver device CSVs into usable datastructure.
    Reads CSV with devEUI and nwkKey columns.
    Rejects embedded header rows resulting from concatenated files.
    Expected to be in chronological order because the last key wins.
    Returned data structure is:
    {
      devEUI1: {
        key: the-last-key
        keys: set(all-keys-seen) - Using a set here because it automatically eliminates duplicates
      }
      devEUI2: {...}
      devEUI3: {...}
    }
    """
    devices = {}
    with open(csv_name, 'r') as csvfile:
        data = [x.replace('\0', '') for x in csvfile]
        reader = csv.DictReader(data, delimiter=',', lineterminator='\n')
        for row in reader:
            devEUI = row['devEUI']
            nwkKey = row['nwkKey']
            if devEUI == "devEUI":
                logger.debug("Ignoring header row") # Concatenated CSV's result in embedded header rows.
            elif devEUI in devices:
                devices[devEUI]['key'] = nwkKey
                devices[devEUI]['keys'].add(nwkKey)
            else:
                devices[devEUI] = {'key': nwkKey, 'keys': set([nwkKey])}
    return devices

def warn_multiple_keys(devices):
    for devEUI, props in devices.items():
        key_count = len(props['keys'])
        if key_count > 1:
            logger.warning("{} keys generated for {} ({})".format(key_count, devEUI, props['keys']))

def update_missing_keys(devices, do_writes = True):
    """Checks whether device exists and if so whether key is correct - fills in anything necessary and reports stats."""
    loraserver = Loraserver()
    missing_devices = 0
    missing_keys = 0
    mismatched = 0
    last_key_wrote = 0
    if not do_writes:
        logger.info(
            """******************************
            Not actually performing writes. Edit script to set do_writes = True.
            (Yes, this should be a command line arg.)")
            *********************************
            """)
    for devEUI in devices.keys():
        dev = loraserver.get_device(devEUI)
        stored = None
        if dev is not None:
            stored = loraserver.get_key(devEUI)
        devices[devEUI]['stored'] = stored
        key_count = len(devices[devEUI]['keys'])
        if dev is None:
            logger.warning("No device {}".format(devEUI))
            missing_devices = missing_devices + 1
            if do_writes:
                loraserver.write_device(devEUI, devices[devEUI]['key'])
        elif stored is None:
            logger.warning("No keys provisioned for device {}".format(devEUI))
            missing_keys = missing_keys + 1
            if do_writes:
                loraserver.write_key(devEUI, devices[devEUI]['key'])
        elif stored != devices[devEUI]['key']:
            logger.error("Keys do not match for device {} ({} generated)".format(devEUI, key_count))
            mismatched = mismatched + 1
            if do_writes:
                loraserver.update_key(devEUI, devices[devEUI]['key'])
        else:
            logger.info("Keys match for device {}".format(devEUI))
            if key_count > 1:
                logger.info("(despite {} generated keys)".format(key_count))
                last_key_wrote = last_key_wrote + 1
    logger.warning("{} devices not stored".format(missing_devices))
    logger.warning("{} devices missing keys".format(missing_keys))
    logger.warning("{} devices wrong key stored".format(mismatched))
    logger.info("{} devices were correct despite multiple keys generated".format(last_key_wrote))

if __name__ == "__main__":
    if len(sys.argv)!=2:
        print("Loads Lora keys from CSV files generated at manufacturing sites.\nUsage: {} loraXXXXX.csv".format(sys.argv[0]))
        sys.exit(1)

    csv_name = sys.argv[1]
    devices = read_csv(csv_name)

    warn_multiple_keys(devices)

    update_missing_keys(devices)