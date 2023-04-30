###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#



import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import time
import json

from copy import deepcopy

# import serialization logic
from CS6381_MW import discovery_pb2

class SubscriberMW():

    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.sub = None  # will be a ZMQ SUB socket for accepting dissemination
        self.req = None  # will be a ZMQ REQ socket to talk to Discovery service
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop

        # used to track logging statistics
        self.toggle = None
        self.iters = 0
        self.logging_dict = {}
        self.filename = None

        self.zkIPAddr = None
        self.zkPort = None
        self.zk = None

        self.curbindstring = None
        self.accepting = False
        self.history = None


    ########################################
    # configure/initialize
    ########################################
    def configure(self, args, bindstring):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.debug("SubcriberMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = int(args.port)
            self.addr = args.addr

            # used for logging
            self.toggle = args.toggle
            self.filename = args.filename

            self.history = args.history

            # Next get the ZMQ context
            self.logger.debug("SubcriberMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("SubcriberMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REQ and PUB sockets
            self.logger.debug("SubcriberMW::configure - obtain REQ and SUB sockets")
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)

            # register the REQ socket for incoming events
            self.logger.debug("SubcriberMW::configure - register the REQ/SUB sockets for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing.
            self.logger.debug("SubcriberMW::configure - connect to Discovery service")
            # For these assignments we use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            self.curbindstring = bindstring
            connect_str = bindstring
            self.req.connect(connect_str)

            print("SubcriberMW::configure - saving the KazooClient object")
            self.zkIPAddr = args.zkIPAddr
            self.zkPort = args.zkPort

            self.logger.info("SubcriberMW::configure - setting the watches")

        except Exception as e:
            raise e


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



    def watch_znode_curbroker1_change(self):

        @self.upcall_obj.zk.DataWatch("/curbroker1")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_disc_change *********")

            self.logger.info("SubscriberMW::disc watch - connecting to new broker")
            value, stat = self.upcall_obj.zk.get("/curbroker1")

            ret = value.decode("utf-8")
            arr = ret.split()

            self.lookup_bind(arr[0], arr[1])


    def watch_znode_curbroker2_change(self):

        @self.upcall_obj.zk.DataWatch("/curbroker2")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_disc_change *********")

            self.logger.info("SubscriberMW::disc watch - connecting to new broker")
            value, stat = self.upcall_obj.zk.get("/curbroker2")

            ret = value.decode("utf-8")
            arr = ret.split()

            self.lookup_bind(arr[0], arr[1])


    def watch_znode_curbroker3_change(self):

        @self.upcall_obj.zk.DataWatch("/curbroker3")
        def dump_data_change(data, stat):
            print("\n*********** Inside watch_znode_disc_change *********")

            self.logger.info("SubscriberMW::disc watch - connecting to new broker")
            value, stat = self.upcall_obj.zk.get("/curbroker3")

            ret = value.decode("utf-8")
            arr = ret.split()

            self.lookup_bind(arr[0], arr[1])


    ########################################
    # register with the discovery service
    ########################################
    def register(self, name, topiclist):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("SubcriberMW::register")

            # as part of registration with the discovery service, we send
            # what role we are playing, the list of topics we are publishing,
            # and our whereabouts, e.g., name, IP and port

            # The following code shows serialization using the protobuf generated code.

            # Build the Registrant Info message first.
            self.logger.debug("SubcriberMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo()  # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are publishing
            reg_info.port = self.port  # port on which we are subscribing
            self.logger.debug("SubcriberMW::register - done populating the Registrant Info")

            self.topiclist = topiclist

            # Next build a RegisterReq message
            self.logger.debug("SubcriberMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq()  # allocate
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER  # we are a publisher
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            register_req.info.CopyFrom(reg_info)  # copy contents of inner structure
            register_req.topiclist[
            :] = topiclist  # this is how repeated entries are added (or use append() or extend ()
            self.logger.debug("SubcriberMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug("SubcriberMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug("SubcriberMW::register - done building the outer message")

            for item in self.topiclist:
                self.sub.setsockopt(zmq.SUBSCRIBE, bytes(item, "utf-8"))

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug("SubcriberMW::register - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.debug("SubcriberMW::register - now wait for reply")


        except Exception as e:
            raise e



    ########################################
    # check if the discovery service gives us a green signal to proceed
    ########################################
    def is_ready(self):
        ''' check if discovery service is ready'''

        try:
            self.logger.debug("SubcriberMW::is_ready")

            # we do a similar kind of serialization as we did in the register
            # message but much simpler, and then send the request to
            # the discovery service

            # The following code shows serialization using the protobuf generated code.

            # first build a IsReady message
            self.logger.debug("SubcriberMW::is_ready - populate the nested IsReady msg")
            isready_msg = discovery_pb2.IsReadyReq()  # allocate
            # actually, there is nothing inside that msg declaration.
            self.logger.debug("SubcriberMW::is_ready - done populating nested IsReady msg")

            # Build the outer layer Discovery Message
            self.logger.debug("SubcriberMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()

            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown

            disc_req.isready_req.CopyFrom(isready_msg)
            self.logger.info("SubcriberMW::is_ready - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug("SubcriberMW::is_ready - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes


            # now go to our event loop to receive a response to this request
            self.logger.info("SubcriberMW::is_ready - now wait for reply")


        except Exception as e:
            raise e


    def plz_lookup(self, topiclist):
        ''' request discovery service for publishers of its topics '''
        try:
            self.logger.info("SubcriberMW::plz_lookup")

            lookup_req = discovery_pb2.LookupPubByTopicReq()

            lookup_req.topiclist[:] = topiclist

            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.lookup_req.CopyFrom(lookup_req)

            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug("SubcriberMW::plz_lookup - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes


            # now go to our event loop to receive a response to this request
            self.logger.info("SubcriberMW::plz_lookup - now wait for reply")

        except Exception as e:
            raise e



    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop(self, timeout=None):
        ''' Event loop '''
        try:
            self.logger.debug("SubscriberMW::event_loop - run the event loop")

            while self.handle_events:  #starts with True value
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    # timeout has occurred so it is time for us to make appln-level
                    # method invocation. Make an upcall to the generic "invoke_operation"
                    # which takes action depending on what state the application
                    # object is in.
                    timeout = self.upcall_obj.invoke_operation()

                elif self.req in events:  # this is the only socket on which we should be receiving replies
                    # handle the incoming reply and return the result
                    timeout = self.handle_reply()

                elif self.sub in events:
                    message = self.sub.recv_string()

                    if message.endswith(","):
                        message = message[:-1]

                    matrix = message.split(",")

                    stack = []

                    if len(matrix) >= self.history:

                        matrix = matrix[::-1]
                        self.iters += 1

                        for i in range(self.history):

                            arr = matrix[i].split(":")

                            stack.append((arr[0], arr[1]))

                            end = str(time.time())
                            tot = float(end) - float(arr[2])

                            tot *= 1000  #convert to ms

                            t = arr[0]

                            if t not in self.logging_dict:
                                self.logging_dict[t] = [tot]
                            else:
                                self.logging_dict[t].append(tot)

                            print ("Stack of size: ", str(self.history))
                            print (stack)

                            if self.iters == 500:
                                json_object = json.dumps(self.logging_dict, indent=4)

                                with open(self.filename, "w") as outfile:
                                    outfile.write(json_object)

                                self.logger.info("SubscriberMW::quota reached - data written to outfile")

                                quit()


            self.logger.info("SubscriberMW::event_loop - out of the event loop")

        except Exception as e:
            raise e



    #################################################################
    # handle an incoming reply
    #################################################################
    def handle_reply(self):
        ''' Handle an incoming reply '''
        try:
            self.logger.info("SubcriberMW::handle_reply")

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
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)

            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)

            else:  # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e



    #################################################################
    # handle an outgoing response
    ##################################################################
    def handle_response(self, resp):
        buf2send = resp.SerializeToString()
        self.logger.debug("Stringified serialized buf = {}".format(buf2send))
        self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes


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