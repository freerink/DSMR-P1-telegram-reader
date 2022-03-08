#!/usr/bin/env python3
# Python script to retrieve and parse a DSMR telegram from a P1 port

import re
import sys
import serial
import crcmod.predefined
from datetime import datetime, timedelta
import json
import threading
import time
import requests
from collections import deque
import logging

class Token:
    clientId = ''
    clientSecret = ''
    scope = ''
    access_token = ''
    type = ''
    access_token_valid_until = datetime.now()

    def __init__(self, url, clientId, clientSecret, scope):
        self.url = url
        self.clientId = clientId
        self.clientSecret = clientSecret
        self.scope = scope


def getToken(token):
   payload = {'grant_type': 'client_credentials', 'client_id': token.clientId, 'client_secret': token.clientSecret, 'scope': token.scope} 
   if  datetime.now() >= token.access_token_valid_until :
       r = requests.post(token.url, data=payload, timeout=10)
       if r.status_code == 200:
           t = json.loads(r.text)
           logging.debug(t)
           token.access_token = t["access_token"]
           token.type = t["token_type"]
           token.access_token_valid_until = datetime.now() + timedelta(seconds=t["expires_in"])
       else:
           logging.error(f'Got HTTP status {r.status_code} requesting token')

def thread_send_data(name, messages, config):
    token = Token(config["token"]["url"],
                  config["token"]["clientId"], 
                  config["token"]["clientSecret"],
                  config["token"]["scope"])
    while True:
        logging.debug("In thread: " + name + ", length: " + str(len(messages)) )
        if len(messages) > 0:
            payload = dict()
            data = []
            while len(messages) > 0:
                msg = messages.popleft()
                data.append(msg)
            payload['data'] = data
            # TODO: make verbose level?
            logging.debug(json.dumps(payload, indent = 4))
            getToken(token)
            # send the data
            try:
                r = requests.post(config["send"]["url"], json=payload, headers={'Authorization': token.type + ' ' + token.access_token})
                if r.status_code != 200:
                    logging.error(f'Got HTTP status {r.status_code} sending data')
            except Exception as ex:
                template = "An exception of type {0} occured. Arguments:{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                logging.error(f'Error sending data. Error: {message}')
            
        time.sleep(config["send"]["sleepSec"])

if __name__ == "__main__" :
    # Read application configuration
    configFile = 'config.json'
    print(f'P1 reader starting. Reading configuration from {configFile}')
    config = json.load(open(configFile, 'r'))

    # Determine log level
    level = logging.INFO
    if config["logging"]["level"] == "DEBUG":
        level = logging.DEBUG

    # Logging settings
    format = "%(asctime)s: %(name)s-%(levelname)s-%(message)s"
    logging.basicConfig(format=format, level=level)

    # DSMR interesting codes
    gas_meter = '1' 
    list_of_interesting_codes = {
        '1-0:1.8.1': ['Meter Reading electricity delivered to client (Tariff 1) in kWh', 'totalEnergyDeliveredToClientTariff1', 'kWh'],
        '1-0:1.8.2': ['Meter Reading electricity delivered to client (Tariff 2) in kWh', 'totalEnergyDeliveredToClientTariff2', 'kWh'],
        '1-0:2.8.1': ['Meter Reading electricity delivered by client (Tariff 1) in kWh', ''],
        '1-0:2.8.2': ['Meter Reading electricity delivered by client (Tariff 2) in kWh', ''],
        '0-0:96.14.0': ['Tariff indicator electricity', 'tariffIndicator', ''],
        '1-0:1.7.0': ['Actual electricity power delivered (+P) in kW', 'actualPowerDelivered', 'kW'],
        '1-0:2.7.0': ['Actual electricity power received (-P) in kW', ''],
        '0-0:17.0.0': ['The actual threshold electricity in kW', ''],
        '0-0:96.3.10': ['Switch position electricity', 'switch', ''],
        '0-0:96.7.21': ['Number of power failures in any phase', 'failures', ''],
        '0-0:96.7.9': ['Number of long power failures in any phase', 'longFailures', ''],
        '1-0:32.32.0': ['Number of voltage sags in phase L1', ''],
        '1-0:52.32.0': ['Number of voltage sags in phase L2', ''],
        '1-0:72:32.0': ['Number of voltage sags in phase L3', ''],
        '1-0:32.36.0': ['Number of voltage swells in phase L1', ''],
        '1-0:52.36.0': ['Number of voltage swells in phase L2', ''],
        '1-0:72.36.0': ['Number of voltage swells in phase L3', ''],
        '1-0:31.7.0': ['Instantaneous current L1 in A', 'actualCurrentL1', 'A'],
        '1-0:32.7.0': ['Instantaneous voltage L1 in V', 'actualVoltageL1', 'V'],
        '1-0:51.7.0': ['Instantaneous current L2 in A', ''],
        '1-0:71.7.0': ['Instantaneous current L3 in A', ''],
        '1-0:21.7.0': ['Instantaneous active power L1 (+P) in kW', 'actualPowerL1', 'kW'],
        '1-0:41.7.0': ['Instantaneous active power L2 (+P) in kW', ''],
        '1-0:61.7.0': ['Instantaneous active power L3 (+P) in kW', ''],
        '1-0:22.7.0': ['Instantaneous active power L1 (-P) in kW', ''],
        '1-0:42.7.0': ['Instantaneous active power L2 (-P) in kW', ''],
        '1-0:62.7.0': ['Instantaneous active power L3 (-P) in kW', ''],
        '0-'+gas_meter+':24.2.1': ['gas delivered to client in m3', 'totalGasDeliveredToClient', 'm3']
    }
    # the list to pass messages to the thread
    messages = deque([])

    # Start helper threads
    jsonThread = threading.Thread(target=thread_send_data, args=("Send json", messages, config), daemon=True)
    jsonThread.start()

    max_len = 72
     
    # Program variables
    # Set the way the values are printed:
    print_format = 'json'
    #print_format = 'string'
    # The true telegram ends with an exclamation mark after a CR/LF
    pattern = re.compile('\r\n(?=!)')
    # According to the DSMR spec, we need to check a CRC16
    crc16 = crcmod.predefined.mkPredefinedCrcFun('crc16')
    # Create an empty telegram
    telegram = ''
    checksum_found = False
    ser_is_open = False
    ser_reopen = False
    good_checksum = False
    count = 0
    badChecksumCount = 0
    
    # Serial port configuration
    ser = serial.Serial()
    ser.baudrate = 115200
    ser.bytesize = serial.EIGHTBITS
    ser.parity = serial.PARITY_NONE
    ser.stopbits = serial.STOPBITS_ONE
    ser.xonxoff = 1
    ser.rtscts = 0
    ser.timeout = 12
    ser.port = config["serial"]["port"]
    
    while True:
        time.sleep(0.1)
        try:
            # Read in all the lines until we find the checksum (line starting with an exclamation mark)
            telegram = ''
            checksum_found = False
            if not ser_is_open :
                # Open serial port
                try:
                    logging.info(f"Opening serial line, count {count}")
                    count += 1
                    ser.open()
                    ser_is_open = True
                except Exception as ex:
                    template = "An exception of type {0} occured. Arguments:{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    logging.critical(f'Error opening serial device {ser.name}. Error: {message}')
                    sys.exit('Exiting')
            while not checksum_found:
                # Read in a line
                telegram_line = ser.readline().decode('ascii')
                logging.debug(telegram_line.strip())
                # Check if it matches the checksum line (! at start)
                if re.match('(?=!)', telegram_line):
                    telegram = telegram + telegram_line
                    logging.debug('Found checksum!')
                    checksum_found = True
                else:
                    telegram = telegram + telegram_line
    
        except Exception as ex:
            template = "An exception of type {0} occured. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            logging.error(message)
            logging.error("There was a problem %s, continuing..." % ex)
            ser_reopen = True
        # Close serial port on problems
        if ser_reopen and ser_is_open:
            ser_reopen = False
            try:
                logging.debug("Closing serial line")
                ser.close()
                ser_is_open = False
            except Exception as ex:
                sys.exit(f"Oops {ser.name}. Exiting. {ex}")
        # We have a complete telegram, now we can process it.
        # Look for the checksum in the telegram
        for m in pattern.finditer(telegram):
            # Remove the exclamation mark from the checksum,
            # and make an integer out of it.
            given_checksum = int('0x' + telegram[m.end() + 1:], 16)
            # The exclamation mark is also part of the text to be CRC16'd
            calculated_checksum = crc16(bytes(telegram[:m.end() + 1], 'ascii'))
            if given_checksum == calculated_checksum:
                good_checksum = True
        if good_checksum:
            logging.debug("Good checksum!")
            # Store the vaules in a dictionary
            telegram_values = dict()
            # Split the telegram into lines and iterate over them
            for telegram_line in telegram.split('\r\n'):
                # Split the OBIS code from the value
                # The lines with a OBIS code start with a number
                if re.match('\d', telegram_line):
                    logging.debug(telegram_line)
                    # The values are enclosed with parenthesis
                    # Find the location of the first opening parenthesis,
                    # and store all split lines
                    logging.debug(re.split('(\()', telegram_line))
                    # You can't put a list in a dict TODO better solution
                    code = ''.join(re.split('(\()', telegram_line)[:1])
                    value = ''.join(re.split('(\()', telegram_line)[1:])
                    telegram_values[code] = value
    
            json_values = dict()
            # Print the lines to screen
            for code, value in sorted(telegram_values.items()):
                # date-time of the telegram
                if code == '0-0:1.0.0' :
                    timestamp = value.lstrip('\(').rstrip('\)')
                    ts = datetime.strptime(timestamp, '%y%m%d%H%M%SW')
                    json_values['dateTime'] = '' + ts.strftime("%Y-%m-%dT%H:%M:%S")
                elif code in list_of_interesting_codes and len(list_of_interesting_codes[code][1]) > 0 :
                    # Cleanup value
                    # Gas needs another way to cleanup
                    if 'm3' in value:
                            (gasTime,value) = re.findall('\((.*?)\)',value)
                            value = float(value.lstrip('\(').rstrip('\)*m3'))
                    else:
                            value = float(value.lstrip('\(').rstrip('\)*kWhAV'))
                    # Print nicely formatted string
                    if print_format == 'string' :
                        print_string = '{0:<'+str(max_len)+'}{1:>12}'
                        logging.debug(print_string.format(list_of_interesting_codes[code][0], value))
                    elif print_format == 'json' :
                        if len(list_of_interesting_codes[code]) > 1 and len(list_of_interesting_codes[code][2]) > 0 :
                            valueWithUnit = dict()
                            valueWithUnit['unit'] = '\"' + list_of_interesting_codes[code][2] + '\"'
                            valueWithUnit['value'] = value
                            json_values[list_of_interesting_codes[code][1]] = valueWithUnit
                        else :
                            json_values[list_of_interesting_codes[code][1]] = value
                    else:
                        print_string = '{0:<10}{1:>12}'
                        logging.debug(print_string.format(code, value))
            if print_format == 'json' :
                messages.append(json_values)
        else:
            badChecksumCount += 1
            if badChecksumCount > 1:
                logging.warning("Bad checksum")
