from kazoo.client import KazooClient
import time


print("Instantiating a KazooClient object")
zk = KazooClient(hosts="127.0.0.1:2181")
print("Connecting to the ZooKeeper Server")
zk.start()
print("client current state = {}".format(zk.state))


path = "/dischm"
if not zk.exists(path):
    zk.create(str ("/") + "dischm", value=bytes("{}", 'utf-8'), ephemeral=True, makepath=True)
    print ("diectory created")

if not zk.exists(path):
    print ("SHOULD NOT SEE THIS")


path = "/dischm2"
if not zk.exists(path):
    zk.create(str ("/") + "dischm2", value=bytes("{}", 'utf-8'), ephemeral=True, makepath=True)
    print ("diectory created")

if not zk.exists(path):
    print ("SHOULD NOT SEE THIS")


path = "/numPubs"
if not zk.exists(path):
    zk.create(str("/") + "numPubs", value=bytes("0", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")

if not zk.exists(path):
    print("SHOULD NOT SEE THIS")


path = "/numSubs"
if not zk.exists(path):
    zk.create(str("/") + "numSubs", value=bytes("0", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")

if not zk.exists(path):
    print("SHOULD NOT SEE THIS")


path = "/curDiscovery"
if not zk.exists(path):
    zk.create(str("/") + "curDiscovery", value=bytes("tcp://localhost:5555", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")

if not zk.exists(path):
    print("SHOULD NOT SEE THIS")


path = "/disclist"
if not zk.exists(path):
    zk.create(str("/") + "disclist", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")

if not zk.exists(path):
    print("SHOULD NOT SEE THIS")


path = "/curbroker"
if not zk.exists(path):
    zk.create(str("/") + "curbroker", value=bytes("localhost 6731", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/curbroker1"
if not zk.exists(path):
    zk.create(str("/") + "curbroker1", value=bytes("localhost 5570", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/brokerlist1"
if not zk.exists(path):
    zk.create(str("/") + "brokerlist1", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/curbroker2"
if not zk.exists(path):
    zk.create(str("/") + "curbroker2", value=bytes("localhost 5573", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/brokerlist2"
if not zk.exists(path):
    zk.create(str("/") + "brokerlist2", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/curbroker3"
if not zk.exists(path):
    zk.create(str("/") + "curbroker3", value=bytes("localhost 5576", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/brokerlist3"
if not zk.exists(path):
    zk.create(str("/") + "brokerlist3", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/pubweather"
if not zk.exists(path):
    zk.create(str("/") + "pubweather", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/pubhumidity"
if not zk.exists(path):
    zk.create(str("/") + "pubhumidity", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/pubairquality"
if not zk.exists(path):
    zk.create(str("/") + "pubairquality", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/publight"
if not zk.exists(path):
    zk.create(str("/") + "publight", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/pubpressure"
if not zk.exists(path):
    zk.create(str("/") + "pubpressure", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/pubtemperature"
if not zk.exists(path):
    zk.create(str("/") + "pubtemperature", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/pubsound"
if not zk.exists(path):
    zk.create(str("/") + "pubsound", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/pubaltitude"
if not zk.exists(path):
    zk.create(str("/") + "pubaltitude", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


path = "/publocation"
if not zk.exists(path):
    zk.create(str("/") + "publocation", value=bytes("", 'utf-8'), ephemeral=True, makepath=True)
    print("diectory created")


print ("Finished creating directories")


while (True):
    time.sleep(5)

