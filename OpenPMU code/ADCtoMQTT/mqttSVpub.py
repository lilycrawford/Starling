"""
OpenPMU - MQTT Interface - SV Publisher
Copyright (C) 2024  www.OpenPMU.org

Licensed under GPLv3.  See LICENSE file or <http://www.gnu.org/licenses/>.

MQTT publisher used to send Sampled Value (SV) data.  This could be used to
send data from an ADC or Merging Unit to an MQTT broker.

The 'Publisher' object connects to MQTT Broker and provides 'send()' method to
publish SV Data to MQTT topic.  Use the 'config.json' file to set options.

Use the 'config.json' file to set options.

The SV data is expected to be in OpenPMU format.

--------------------
Changes 2024-06-10
* Improved use of MQTT terminology
"""

from paho.mqtt import client as mqtt_client
import random
import socket

__author__ = 'Xiaodong, OpenPMU.org'


class Publisher():
    """
    UDP interface to send phasor estimation results
    """

    def __init__(self, config):
        
        self.config = config
                     
        self.client_id = ("SV_pub-{rand}").format(rand = random.randint(0,10000))     # Generate a Client ID prefix "SV_sub" and random suffix
               
        self.client = mqtt_client.Client(client_id=self.client_id)        # Setup MQTT CLient
        # client.username_pw_set(config["mqttUsername"], config["mqttPassword"])
        
        self.client.on_connect = self.on_connect                # Setup "on_connect" callback function

        try:
            self.client.connect(self.config["mqttBroker"], self.config["mqttPort"])         # Connect to MQTT Broker
        except:
            brokerIP = socket.gethostbyname(self.config["mqttBroker"])        # For an unknown reason, MQTT client having difficulty with some hostnames / DNS
            self.client.connect(brokerIP, self.config["mqttPort"])            # Seems to affect the second client connecting, not the first
        
        
        self.client.loop_start()                                # Start client loop
        

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("SV Publisher Client has connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
            
    def close(self):
        """
        Close the connection with MQTT Broker
        """
        self.client.loop_stop()
        self.client.disconnect()


    def __del__(self):
        self.close()
        
    def send(self, SVdata):
        """
        send phasor estimation results

        :param resultDict: python dict of the result
        :return:
        """
        
        self.client.publish(self.config["mqttTopicSV"], SVdata, qos=0, retain=False)