# -*- coding: utf-8 -*-
"""
OpenPMU - Multicast Tools
Copyright (C) 2022  www.OpenPMU.org

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import signal, sys
import socket
import json, xmltodict
from datetime import datetime
import mqttSVpub

# Keyboard Interrupt event handler (CTRL+C to quit)
def signal_handler(signal, frame):
    global runLoop 
    runLoop = False
    print('You pressed Ctrl+C!')
    udpRxSock.close()
    sys.exit(0)
    
# Heartbeat 'tick' on console
def heartbeat(prev):
    now = datetime.now()
    if now.second != prev.second:
        print(".", end="", flush=True)
    return now

def UDPreceive(UDP_IP, UDP_PORT):
    try:
        udpRxSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)    # Internet, UDP
        udpRxSock.bind((UDP_IP, UDP_PORT))
        print("New port successfully opened.")
    except:
        print("Port already open in another tab")
        print("Exception has occurred: Only one usage of each socket address (protocol/network address/port) is normally permitted")
    
    udpRxSock.settimeout(1)                                         # Timeout 1 second
    return udpRxSock

# Load the config file (or fall back to hardcoded defaults)
def loadConfig(configFile="config.json"):
    try:
        with open(configFile, 'r') as jsonFile:
            return json.load(jsonFile)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Warning: {configFile} not found or invalid. Using fallback configuration.")
        return {    
            "UDP_IP": "127.0.0.1",
            "UDP_PORT": 48001,
            "mqttBroker": "127.0.0.1",
            "mqttPort": 1883,
            "mqttTopicSV": "OpenPMU/ADC_SV"
        }    

# ##############################
# ------------ MAIN ------------
# ##############################
               
if __name__ == '__main__':
    
    # Keyboard interrupt
    signal.signal(signal.SIGINT, signal_handler)
        
    print("OpenPMU - ADC to MQTT")
    
    config  = loadConfig("config.json")

    # Setup UDP Receive Socket
    udpRxSock = UDPreceive(config["UDP_IP"], config["UDP_PORT"])
    
    SVsender = mqttSVpub.Publisher(config)
       
    # Loop forever (CTRL+C to quit)
    prev = datetime.now()
    runLoop= True
    while runLoop:
        try:
            # Receive UDP, send to MC group
            data, addr = udpRxSock.recvfrom(10240)  # buffer size is 10240 bytes
            
            
            xml_parse = xmltodict.parse(data)
            
            data = json.dumps(xmltodict.parse(data))
            
            SVsender.send(data)
            # Heartbeat on console
            prev = heartbeat(prev)
        except Exception as e:
            print(e)
            pass

    udpRxSock.close()
    SVsender.close()
