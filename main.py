#! /usr/bin/env python
# pip3 install opcua

from opcua import Client
import datetime
from influxdb import InfluxDBClient
from env import *

time = datetime.datetime.utcnow()

if __name__ == "__main__":

    class opc:
        def __init__(self):
            client = Client("opc.tcp://192.168.88.89:4862/")
            client.set_security_string(
                "Basic128Rsa15,SignAndEncrypt,/root/OPC/my_cert.der,/root/OPC/my_private_key.pem"
            )
            client.application_uri = "urn:example.org:FreeOpcUa:python-opcua"
            client.secure_channel_timeout = 10000
            client.session_timeout = 10000
            try:
                client.connect()
                client.load_type_definitions()  # load definition of server specific structures/extension objects
                self.well_metr = round(
                    client.get_node("ns=1;s=t|well_metr").get_value(), 2
                )
                self.Dew_Point = round(
                    client.get_node("ns=1;s=t|FT329.05_pv").get_value(), 2
                )
                self.steam_6bar = round(
                    client.get_node("ns=1;s=t|PT319.05_pv").get_value(), 2
                )
                self.steam_10bar = round(
                    client.get_node("ns=1;s=t|PT318.05_pv").get_value(), 2
                )
                self.steam_16bar = round(
                    client.get_node("ns=1;s=t|PT317.05_pv").get_value(), 2
                )

                self.flame_boiler1 = (
                    10
                    if (client.get_node("ns=1;s=t|Flame_on_fbk_B1").get_value())
                    else 2
                )
                self.flame_boiler2 = (
                    10
                    if (client.get_node("ns=1;s=t|Flame_on_fbk_B2").get_value())
                    else 2
                )
                self.flame_boiler3 = (
                    10 if (client.get_node("ns=1;s=t|FLAME_FBK_B3").get_value()) else 2
                )

                self.steam_boiler1 = round(
                    client.get_node("ns=1;s=t|PT102.05_pv").get_value(), 2
                )
                self.steam_boiler2 = round(
                    client.get_node("ns=1;s=t|PT202.05_pv").get_value(), 2
                )
                self.steam_boiler3 = round(
                    client.get_node("ns=1;s=t|PT302.05_pv").get_value(), 2
                )
                self.well_metr_total_real = client.get_node(
                    "ns=1;s=t|well_metr_total_real"
                ).get_value()

            finally:
                client.disconnect()

    class ahu:
        def __init__(self, ahu_name):
            client = Client("opc.tcp://192.168.88.89:4862/")
            client.set_security_string(
                "Basic128Rsa15,SignAndEncrypt,/root/OPC/my_cert.der,/root/OPC/my_private_key.pem"
            )
            client.application_uri = "urn:example.org:FreeOpcUa:python-opcua"
            client.secure_channel_timeout = 10000
            client.session_timeout = 10000
            try:
                client.connect()
                client.load_type_definitions()  # load definition of server specific structures/extension objects

                self.T1 = round(
                    client.get_node(
                        "ns=1;s=t|Data_" + ahu_name + ".T1_inlet"
                    ).get_value(),
                    2,
                )
                self.T2 = round(
                    client.get_node(
                        "ns=1;s=t|Data_" + ahu_name + ".T2_outlet"
                    ).get_value(),
                    2,
                )
                self.H1 = round(
                    client.get_node(
                        "ns=1;s=t|Data_" + ahu_name + ".H1_inlet"
                    ).get_value(),
                    2,
                )
                self.H2 = round(
                    client.get_node(
                        "ns=1;s=t|Data_" + ahu_name + ".H2_outlet"
                    ).get_value(),
                    2,
                )
            finally:
                client.disconnect()

    opc = opc()
    W4 = ahu("W4")
    W7 = ahu("W7")
    W11 = ahu("W11")
    W81 = ahu("W81")
    W82 = ahu("W82")
    O1 = ahu("O1")

    body = [
        {
            "measurement": "Utility",
            "time": time,
            "fields": {
                "well_metr": opc.well_metr,
                "Dew_Point": opc.Dew_Point,
                "steam_6bar": opc.steam_6bar,
                "steam_10bar": opc.steam_10bar,
                "steam_16bar": opc.steam_16bar,
                "flame_boiler1": opc.flame_boiler1,
                "flame_boiler2": opc.flame_boiler2,
                "flame_boiler3": opc.flame_boiler3,
                "steam_boiler1": opc.steam_boiler1,
                "steam_boiler2": opc.steam_boiler2,
                "steam_boiler3": opc.steam_boiler3,
                "well_metr_total_real": opc.well_metr_total_real,
            },
        }
    ]

    body2 = [
        {
            "measurement": "AHU",
            "time": time,
            "fields": {
                "O1_T1": O1.T1,
                "O1_H1": O1.H1,
                "W11_T1": W11.T1,
                "W11_T2": W11.T2,
                "W11_H1": W11.H1,
                "W11_H2": W11.H2,
                "W4_T1": W4.T1,
                "W4_T2": W4.T2,
                "W4_H1": W4.H1,
                "W4_H2": W4.H2,
                "W7_T1": W7.T1,
                "W7_T2": W7.T2,
                "W7_H1": W7.H1,
                "W7_H2": W7.H2,
                "W81_T1": W81.T1,
                "W81_T2": W81.T2,
                "W81_H1": W81.H1,
                "W81_H2": W81.H2,
                "W82_T1": W82.T1,
                "W82_T2": W82.T2,
                "W82_H1": W82.H1,
                "W82_H2": W82.H2,
            },
        }
    ]
    # print(body2)

    ifclient = InfluxDBClient(ifhost, ifport, ifuser, ifpass, ifdb)
    ifclient.write_points(body)

    ifclient = InfluxDBClient(ifhost, ifport, ifuser, ifpass, ifdb)
    ifclient.write_points(body2)
