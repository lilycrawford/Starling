"""
OpenPMU - MQTT Interface - SV Subscriber
Copyright (C) 2024  www.OpenPMU.org

Licensed under GPLv3.  SSee LICENSE file or <http://www.gnu.org/licenses/>.

MQTT subscriber used by applications to receive Sampled Value (SV) data from
an MQTT broker.  SV data may be required for an application such as a phasor
estimation algorithm.

The 'Subscriber' method connects to the MQTT broker and subscribes to the 
desired topic.  New data arriving on the topic is buffered in a queue.  Note 
that the queue is of limited length.  The 'receive()' method allows an 
application to obtain one "frame" of data at a time, and if the queue is empty
it has a 'timeout' analogous to the timeout when receiving UDP.

Use the 'config.json' file to set options.

The data is expected to be in the OpenPMU SV format.

--------------------
Changes 2024-06-10
* Changed from UDP to MQTT transport
* Improvements to use of MQTT terminology
"""

import base64
import numpy as np
from lxml import etree
import json

from paho.mqtt import client as mqtt_client
import random
from queue import Queue

import socket

__author__ = 'Xiaodong, OpenPMU.org'


class Subscriber:
    """
    A PMU Cape XML format receiver
    """

    CH_NUMBER = 8  # max ch number
    ADC_MAX_VALUE = 2 ** 15-1  # ADC max sampled value, for 16 bit ADC
    ADC_RANGE = 5.0  # ADC input voltage range, should be 5 or 10 V

    def __init__(self, config):
        
        self.config = config

        self.q = Queue(maxsize=200)                             # Create Queue which will buffer incoming data steam        
      
        self.client_id = ("SV_sub-{rand}").format(rand = random.randint(0,10000))     # Generate a Client ID prefix "SV_sub" and random suffix
        
        self.client = mqtt_client.Client(client_id=self.client_id)        # Setup MQTT CLient
        # client.username_pw_set(config["mqttUsername"], config["mqttPassword"])
        
        self.client.on_connect = self.on_connect                # Setup "on_connect" callback function
              
        try:
            self.client.connect(self.config["mqttBroker"], self.config["mqttPort"])         # Connect to MQTT Broker
        except:
            brokerIP = socket.gethostbyname(self.config["mqttBroker"])        # For an unknown reason, MQTT client having difficulty with some hostnames / DNS
            self.client.connect(brokerIP, self.config["mqttPort"])            # Seems to affect the second client connecting, not the first
        
        self.client.subscribe(self.config["mqttTopicSV"])                       # Subscribe to the SV topic
        self.client.on_message = self.on_message                # Setup "on_message" callback function (will buffer to Queue)
    
        self.client.loop_start()                                # Start client loop
                            
                
        # store parsed xml information
        self.xmlInfo = dict()
        # covert data in xml to different types based on tag name
        self.xmlTypeConvert = lambda tag: {  # 'Date': lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
            # 'Time': lambda x: datetime.strptime(x, "%H:%M:%S.%f").time(),
            'Frame': int,
            'Fs': int,
            'n': int,
            'bits': int,
            'Channels': int,
            'Payload': self.payloadConvert,
            'PayloadRAW': self.payloadConvertRAW 
        }.get(tag, lambda x: x)
        
        self.dataTypeConvert = lambda tag: {  
            # 'Date': lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
            # 'Time': lambda x: datetime.strptime(x, "%H:%M:%S.%f").time(),
            'Frame': int,
            'Fs': int,
            'n': int,
            'bits': int,
            'Channels': int,
            'Payload': self.payloadConvert,
            'PayloadRAW': self.payloadConvertRAW 
        }.get(tag, lambda x: x)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("SV Subscriber Client has connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    def on_message(self, client, userdata, msg):
        self.q.put(msg.payload)                         # Add new messages to Queue


    def close(self):
        """
        Close the connection with MQTT Broker
        """
        self.client.loop_stop()
        self.client.disconnect()


    def __del__(self):
        self.close()
    
    # Check the type of Payload / is it JSON or XML   
    def detect_format(self, payload):
        #payload = payload.strip()
    
        try:
            json.loads(payload)
            return "json"
        except Exception:
            pass
    
        try:
            etree.fromstring(payload)
            return "xml"
        except Exception:
            pass
    
        return "unknown"        
        
    # convert from base64 to  np.array
    def payloadConvert(self, payloadBase64):
        return np.frombuffer(bytearray(base64.standard_b64decode(payloadBase64)), dtype='>i2')/float(Subscriber.ADC_MAX_VALUE) * Subscriber.ADC_RANGE#big endian

    # convert from base64 to  np.array
    def payloadConvertRAW(self, payloadBase64):
        return np.frombuffer(bytearray(base64.standard_b64decode(payloadBase64)), dtype='>i2') #big endian

    def receive(self, xml="blank", timeout=1):
        """
        Receive data from MQTT SV Topic.

        :param timeout: max waiting time for xml data, in seconds
        :return: received data in a python dict
        """
        
        payload = self.q.get(timeout) 
        payload_format = self.detect_format(payload)
        
        if payload_format == "xml":
               
            xml = payload                      # Get message from Queue
            
            # parse the received data and store it in a dict
            level0 = etree.fromstring(xml)
            try:
                for level1 in list(level0):
                    text = level1.text
                    tag = level1.tag
                    if tag.startswith("Channel_"):
                        self.xmlInfo[tag] = dict()
                        for level2 in list(level1):
                            if level2.tag == "Payload":     # Create new key to store RAW ADC sampled values (SV)
                                self.xmlInfo[tag]["PayloadRAW"] = self.xmlTypeConvert("PayloadRAW")(level2.text)  
                            self.xmlInfo[tag][level2.tag] = self.xmlTypeConvert(level2.tag)(level2.text)
                    else:
                        self.xmlInfo[tag] = self.xmlTypeConvert(tag)(text)
    
            except AttributeError as e:
                print("Error occurred while parsing xml information")
                print(e)
                return None
            else:
                return self.xmlInfo
            
        # elif payload_format == "json":
            
        #     payload_dict = json.loads(payload)["OpenPMU"]
            
        #     # parse the received data and store it in a dict
        #     level0 = json.loads(payload)["OpenPMU"]
        #     try:
        #         for key, value in payload_dict:
        #             if key.startswith("Channel_"):
                        
        #                 for key, value in value:
        #                     if key == "Payload":     # Create new key to store RAW ADC sampled values (SV)
        #                         self.xmlInfo[tag]["PayloadRAW"] = self.xmlTypeConvert("PayloadRAW")(level2.text)  
        #                     self.xmlInfo[tag][level2.tag] = self.xmlTypeConvert(level2.tag)(level2.text)
        #             else:
        #                 self.xmlInfo[tag] = self.xmlTypeConvert(tag)(text)
    
        #     except AttributeError as e:
        #         print("Error occurred while parsing xml information")
        #         print(e)
        #         return None
        #     else:
        #         return self.xmlInfo
            
            
            
        #     # for key, value in payload_dict.items():
        #     #     if key.startswith("Channel_"):
        #     #         channel["PayloadBase64"]    = channel["Payload"]
        #     #         channel["PayloadRAW"]       = self.payloadConvertRAW(channel["Payload"])
        #     #         channel["Payload"]          = self.payloadConvert(channel["Payload"])
        #     #     else:
                    
            
        #     # return payload_dict
        
        
        # else:
        #     return None
        

        elif payload_format == "json":
            
            payload_dict = json.loads(payload)["OpenPMU"]

            try:
                for key, value in payload_dict.items():
                    
                    if key.startswith("Channel_"):
                        
                        for channel_key, channel_value in list(value.items()):
                            
                            if channel_key == "Payload":
                                # Preserve the original Base64 data
                                value["PayloadBase64"] = channel_value
                                
                                # Convert Payload to the normalised numpy array
                                value["Payload"] = self.payloadConvert(channel_value)
                                
                                # Convert Payload to the raw ADC numpy array
                                value["PayloadRAW"] = self.payloadConvertRAW(channel_value)
                            
                            else:
                                value[channel_key] = self.dataTypeConvert(channel_key)(channel_value)
                    
                    else:
                        payload_dict[key] = self.dataTypeConvert(key)(value)
            
            except AttributeError as e:
                print("Error occurred while parsing JSON information")
                print(e)
                return None
            
            return payload_dict

