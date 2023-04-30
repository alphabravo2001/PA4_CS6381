###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.


import os  # for OS functions
import sys  # for syspath and system exception
import time  # for sleep
import logging  # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2


class BrokerMW():

    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None  # will be a ZMQ REQ socket to talk to Discovery service
        self.pub = None  # will be a ZMQ PUB socket for dissemination
        self.sub = None
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.topiclist = None
        self.curbindstring = None
        self.group = None


    ########################################
    # configure/initialize
    ########################################
    def configure(self, args, bindstring):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("BrokerMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr

            self.group = args.group

            # Next get the ZMQ context
            self.logger.debug("BrokerMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("BrokerMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REQ and PUB/SUB sockets
            self.logger.debug("BrokerMW::configure - obtain REQ and PUB sockets")
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.sub = context.socket(zmq.SUB)

            # Since are using the event loop approach, register the REQ socket for incoming events
            # Note that nothing ever will be received on the PUB socket and so it does not make
            # any sense to register it with the poller for an incoming message.
            self.logger.debug("BrokerMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.debug("BrokerMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            self.curbindstring = bindstring
            connect_str = bindstring
            self.req.connect(connect_str)

            self.logger.debug("BrokerMW::configure - bind to the pub socket")
            # note that we publish on any interface hence the * followed by port number.
            # We always use TCP as the transport mechanism (at least for these assignments)
            # Since port is an integer, we convert it to string to make it part of the URL
            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind(bind_string)


            self.logger.info("BrokerMW::configure completed")

        except Exception as e:
            raise e

    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop(self, timeout=None):
        ''' Event loop '''
        try:
            self.logger.info("BrokerMW::event_loop - run the event loop")

            # we are using a class variable called "handle_events" which is set to
            # True but can be set out of band to False in order to exit this forever
            # loop

            file = "/curbroker" + str(self.upcall_obj.group)
            value, stat = self.upcall_obj.zk.get(file)
            curbroker = value.decode("utf-8")

            cur = str(self.addr) + " " + str(self.port)



            iters = 0

            while (curbroker != cur):

                iters += 1

                if (iters % 5 == 0):
                    self.logger.info("BrokerAppln::invoke_operation - waiting to become leader")

                file = "/curbroker" + str(self.group)
                value, stat = self.upcall_obj.zk.get(file)

                curbroker = value.decode("utf-8")
                cur = str(self.addr) + " " + str(self.port)

                time.sleep(2)



            while self.handle_events:  # it starts with a True value
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll(timeout=timeout))

                # check if a timeout has occurred. We know this is the case when
                # the event mask is empty
                if not events:
                    # timeout has occurred so it is time for us to make appln-level
                    # method invocation. Make an upcall to the generic "invoke_operation"
                    # which takes action depending on what state the application
                    # object is in.
                    timeout = self.upcall_obj.invoke_operation()

                elif self.req in events:  # this is the only socket on which we should be receiving replies

                    # handle the incoming reply from remote entity and return the result
                    timeout = self.handle_reply()

                elif self.sub in events:
                    message = self.sub.recv_string()
                    print (message)
                    self.pub.send(bytes(message, "utf-8"))


                else:
                    raise Exception("Unknown event after poll")

        except Exception as e:
            raise e

    ########################################
    # register with the discovery service
    ########################################
    def register(self, name, topiclist):
            ''' register the appln with the discovery service '''

            try:
                self.logger.info("BrokerMW::register")

                # as part of registration with the discovery service, we send
                # what role we are playing, the list of topics we are publishing,
                # and our whereabouts, e.g., name, IP and port

                # The following code shows serialization using the protobuf generated code.

                # Build the Registrant Info message first.
                self.logger.debug("BrokerMW::register - populate the Registrant Info")
                reg_info = discovery_pb2.RegistrantInfo()  # allocate
                reg_info.id = name  # our id
                reg_info.addr = self.addr  # our advertised IP addr where we are publishing
                reg_info.port = self.port  # port on which we are subscribing
                self.logger.debug("BrokerMW::register - done populating the Registrant Info")

                self.topiclist = topiclist

                # Next build a RegisterReq message
                self.logger.debug("BrokerMW::register - populate the nested register req")
                register_req = discovery_pb2.RegisterReq()  # allocate
                register_req.role = discovery_pb2.ROLE_BOTH  # we are a publisher
                # It was observed that we cannot directly assign the nested field here.
                # A way around is to use the CopyFrom method as shown
                register_req.info.CopyFrom(reg_info)  # copy contents of inner structure
                register_req.topiclist[
                :] = topiclist  # this is how repeated entries are added (or use append() or extend ()
                self.logger.debug("BrokerMW::register - done populating nested RegisterReq")

                # Finally, build the outer layer DiscoveryReq Message
                self.logger.debug("BrokerMW::register - build the outer DiscoveryReq message")
                disc_req = discovery_pb2.DiscoveryReq()  # allocate
                disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
                # It was observed that we cannot directly assign the nested field here.
                # A way around is to use the CopyFrom method as shown
                disc_req.register_req.CopyFrom(register_req)
                self.logger.debug("BrokerMW::register - done building the outer message")

                for item in self.topiclist:
                    self.sub.setsockopt(zmq.SUBSCRIBE, bytes(item, "utf-8"))

                # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
                # a real string
                buf2send = disc_req.SerializeToString()
                self.logger.debug("Stringified serialized buf = {}".format(buf2send))

                # now send this to our discovery service
                self.logger.debug("BrokerMW::register - send stringified buffer to Discovery service")
                self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

                # now go to our event loop to receive a response to this request
                self.logger.debug("BrokerMW::register - now wait for reply")


            except Exception as e:
                raise e


    ########################################
    # check if the discovery service gives us a green signal to proceed
    #
    # Here we send the isready message and do the serialization
    #
    # No return value from this as it is handled in the invoke_operation
    # method of the application object.
    ########################################
    def is_ready(self):
        ''' check if discovery service is ready '''

        try:
            self.logger.info("BrokerMW::is_ready")

            # we do a similar kind of serialization as we did in the register
            # message but much simpler as the message format is very simple.
            # Then send the request to the discovery service

            # The following code shows serialization using the protobuf generated code.

            # first build a IsReady message
            self.logger.debug("BrokerMW::is_ready - populate the nested IsReady msg")
            isready_req = discovery_pb2.IsReadyReq()  # allocate
            # actually, there is nothing inside that msg declaration.
            self.logger.debug("BrokerMW::is_ready - done populating nested IsReady msg")

            # Build the outer layer Discovery Message
            self.logger.debug("BrokerMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.isready_req.CopyFrom(isready_req)
            self.logger.debug("BrokerMW::is_ready - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug("BrokerMW::is_ready - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info("BrokerMW::is_ready - request sent and now wait for reply")

        except Exception as e:
            raise e


    def request_pubs(self):
        ''' Request publishers from discovery '''

        pubs_req = discovery_pb2.RegisterPubsReq()

        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS

        disc_req.pubs_req.CopyFrom(pubs_req)

        # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
        # a real string
        buf2send = disc_req.SerializeToString()
        self.logger.debug("Stringified serialized buf = {}".format(buf2send))

        # now send this to our discovery service
        self.logger.debug("BrokerMW::request_pubs - send stringified buffer to Discovery service")
        self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

        # now go to our event loop to receive a response to this request
        self.logger.info("BrokerMW::request_pubs - request sent and now wait for reply")


    #################################################################
    # handle an incoming reply
    #################################################################
    def handle_reply(self):
        ''' Handle an incoming reply '''

        try:
            self.logger.info("BrokerMW::handle_reply")

            # let us first receive all the bytes
            bytesRcvd = self.req.recv()

            # now use protobuf to deserialize the bytes
            # The way to do this is to first allocate the space for the
            # message we expect, here DiscoveryResp and then parse
            # the incoming bytes and populate this structure (via protobuf code)
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            # demultiplex the message based on the message type but let the application
            # object handle the contents as it is best positioned to do so. See how we make
            # the upcall on the application object by using the saved handle to the appln object.
            #
            # Note also that we expect the return value to be the desired timeout to use
            # in the next iteration of the poll.
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                # let the appln level object decide what to do
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                # this is a response to is ready request
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                # this is a response to is ready request
                timeout = self.upcall_obj.pubslookup_response(disc_resp.pubs_resp)

            else:  # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e


    #################################################################
    # handle a SUB socket binding to publishers
    ##################################################################
    def lookup_bind(self, addr, port):
        self.sub.connect("tcp://{}:{}".format(addr, port))


    ########################################
    # set upcall handle
    #
    # here we save a pointer (handle) to the application object
    ########################################
    def set_upcall_handle(self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj



    ########################################
    # disable event loop
    #
    # here we just make the variable go false so that when the event loop
    # is running, the while condition will fail and the event loop will terminate.
    ########################################
    def disable_event_loop(self):
        ''' disable event loop '''
        self.handle_events = False



    def get_disc_value(self):
        try:
            # Now we are going to check if the znode that we just created
            # exists or not. Note that a watch can be set on create, exists
            # and get/set methods
            if self.upcall_obj.zk.exists("/curDiscovery"):

                # Now acquire the value and stats of that znode
                # value,stat = self.zk.get (self.zkName, watch=self.watch)
                value, stat = self.upcall_obj.zk.get("/curDiscovery")

                ret = value.decode("utf-8")
                return ret

            else:
                print("{} znode does not exist, why?".format("/curDiscovery"))

        except Exception as e:
            raise e



    def watch_znode_disc_change(self):

        @self.upcall_obj.zk.DataWatch("/curDiscovery")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_disc_change *********")
            self.req.disconnect(self.curbindstring)

            self.logger.info("BrokerMW::disc watch - connecting to new discovery")
            new_disc_str = self.get_disc_value()
            self.curbindstring = new_disc_str
            self.req.connect(new_disc_str)




    def watch_znode_pubweather_change(self):

        @self.upcall_obj.zk.DataWatch("/pubweather")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/pubweather")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])


    def watch_znode_pubhumidity_change(self):

        @self.upcall_obj.zk.DataWatch("/pubhumidity")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/pubhumidity")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])


    def watch_znode_pubairquality_change(self):

        @self.upcall_obj.zk.DataWatch("/pubairquality")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/pubairquality")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])


    def watch_znode_publight_change(self):

        @self.upcall_obj.zk.DataWatch("/publight")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/publight")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])


    def watch_znode_pubpressure_change(self):

        @self.upcall_obj.zk.DataWatch("/pubpressure")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/pubpressure")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])


    def watch_znode_pubsound_change(self):

        @self.upcall_obj.zk.DataWatch("/pubsound")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/pubsound")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])


    def watch_znode_pubtemperature_change(self):

        @self.upcall_obj.zk.DataWatch("/pubtemperature")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/pubtemperature")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])


    def watch_znode_pubaltitude_change(self):

        @self.upcall_obj.zk.DataWatch("/pubaltitude")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/pubaltitude")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])


    def watch_znode_publocation_change(self):

        @self.upcall_obj.zk.DataWatch("/publocation")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_pub_change *********")

            self.logger.info("BrokerMW::pub watch - connecting to new publisher")
            value, stat = self.upcall_obj.zk.get("/publocation")

            ret = value.decode("utf-8")

            if ret:
                arr = ret.split(":")[:-1]
                self.lookup_bind(arr[0], arr[1])
