
#! /usr/bin/env python
#pip3 install opcua


from opcua import Client
import datetime
from env import *

time = datetime.datetime.utcnow()

if __name__ == "__main__":

    class ahu:
        def __init__(self, ahu_name):
            client = Client("opc.tcp://192.168.88.89:4862/")
            client.set_security_string("Basic128Rsa15,SignAndEncrypt,my_cert.der,my_private_key.pem")
            client.application_uri = "urn:example.org:FreeOpcUa:python-opcua"
            client.secure_channel_timeout = 10000
            client.session_timeout = 10000
            try:
                client.connect()
                client.load_type_definitions()  # load definition of server specific structures/extension objects
               
                
                self.T1 = round( client.get_node("ns=1;s=t|Data_"+ahu_name+".T1_inlet").get_value(), 2)
                self.T2 = round( client.get_node("ns=1;s=t|Data_"+ahu_name+".T2_outlet").get_value(), 2)
                self.H1 = round( client.get_node("ns=1;s=t|Data_"+ahu_name+".H1_inlet").get_value(), 2)
                self.H2 = round( client.get_node("ns=1;s=t|Data_"+ahu_name+".H2_outlet").get_value(), 2)
            finally:
                client.disconnect()
    opc = ahu("W4")

    
    



    #ifclient = InfluxDBClient(ifhost, ifport, ifuser, ifpass, ifdb)
    #ifclient.write_points(body)
