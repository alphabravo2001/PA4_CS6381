###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.

import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector


# Now import our CS6381 Middleware
from CS6381_MW.SubscriberMW import SubscriberMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

from threading import Timer

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

import zmq

from kazoo.client import KazooClient
import atexit

class SubscriberAppln():

    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        LOOKUP = 4,
        ACCEPT = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE  # state that are we in
        self.name = None  # our name (some unique name)
        self.topiclist = None  # the different topics that we publish on
        self.iters = None  # number of iterations of publication
        self.num_topics = None  # total num of topics we publish
        self.lookup = None  # one of the diff ways we do lookup
        self.dissemination = None  # direct or via broker
        self.mw_obj = None  # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements

        self.zkIPAddr = None  # ZK server IP address
        self.zkPort = None  # ZK server port num
        self.zk = None


    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("SubcriberAppln::configure")

            atexit.register(self.exitfunc)

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            # initialize our variables
            self.name = args.name  # our name
            #self.iters = args.iters  # num of iterations
            self.num_topics = args.num_topics  # total num of topics we publish

            # Now, get the configuration object
            self.logger.debug("SubcriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Now get our topic list of interest
            self.logger.debug("SubcriberAppln::configure - selecting our topic list")
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)  # let topic selector give us the desired num of topics

            self.logger.info("SubcriberAppln::configure - saving the KazooClient object")
            self.zkIPAddr = args.zkIPAddr   
            self.zkPort = args.zkPort
            hosts = self.zkIPAddr + str(":") + str(self.zkPort)
            self.zk = KazooClient(hosts)
            self.zk.start()

            if self.zk.exists("/curDiscovery"):
                # Now acquire the value and stats of that znode
                # value,stat = self.zk.get (self.zkName, watch=self.watch)
                value, stat = self.zk.get("/curDiscovery")
                bindstring = value.decode("utf-8")

            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug("SubcriberAppln::configure - initialize the middleware object")
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.configure(args,bindstring)  # pass remainder of the args to the m/w object

            self.logger.info("SubcriberAppln::configure - configuration complete")

        except Exception as e:
            raise e


    def watch_znode_pubscount_change(self):

        @self.zk.DataWatch("/numPubs")
        def dump_data_change():

            self.logger.info("SubscriberAppln::disc watch - number of publishers has changed ")

            if self.state == self.State.Accept:
                self.mw.plz_lookup(self.topiclist)



    def exitfunc(self):

        print("Exiting Subscriber - starting exitfunc")

        if self.zk.exists("/numSubs") and self.state == self.State.Accept:
            value, stat = self.zk.get("/numSubs")
            value = value.decode("utf-8")

            new_val = int(value) - 1
            new_bytes = bytes(str(new_val), 'utf-8')
            self.zk.set("/numSubs", new_bytes)

        print("Exiting Subscriber - exitfunc complete")



    ########################################
    # driver program
    ########################################
    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("SubcriberAppln::driver")

            # dump our contents (debugging purposes)
            self.dump()

            # First ask our middleware to keep a handle to us to make upcalls.
            # This is related to upcalls. By passing a pointer to ourselves, the
            # middleware will keep track of it and any time something must
            # be handled by the application level, invoke an upcall.
            self.logger.debug("SubcriberAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)

            # Now simply let the underlying middleware object enter the event loop
            # to handle events. However, a trick we play here is that we provide a timeout
            # of zero so that control is immediately sent back to us where we can then
            # register with the discovery service and then pass control back to the event loop
            #
            # As a rule, whenever we expect a reply from remote entity, we set timeout to
            # None or some large value, but if we want to send a request ourselves right away,
            # we set timeout is zero.
            #

            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)  # start the event loop

            self.logger.info("SubcriberAppln::driver completed")

        except Exception as e:
            raise e



    ########################################
    # generic invoke method called as part of upcall
    ##
    # This method will get invoked as part of the upcall made
    # by the middleware's event loop after it sees a timeout has
    # occurred.
    ########################################

    def invoke_operation(self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info("SubcriberAppln::invoke_operation")

            # check what state are we in. If we are in REGISTER state,
            # we send register request to discovery service. If we are in
            # ISREADY state, then we keep checking with the discovery
            # service.
            if (self.state == self.State.REGISTER):
                # send a register msg to discovery service
                self.logger.debug("SubcriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register(self.name, self.topiclist)

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a register request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None

            elif (self.state == self.State.ISREADY):
                # Now keep checking with the discovery service if we are ready to go
                #
                # Note that in the previous version of the code, we had a loop. But now instead
                # of an explicit loop we are going to go back and forth between the event loop
                # and the upcall until we receive the go ahead from the discovery service.

                self.logger.debug("SubcriberAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready()  # send the is_ready? request
                self.mw_obj.watch_znode_disc_change()

                self.logger.debug("SubcriberAppln::configure - connect to 3 Broker services")

                value, stat = self.zk.get("/curbroker1")
                ret = value.decode("utf-8")
                arr = ret.split()
                if arr:
                    self.mw_obj.lookup_bind(arr[0], arr[1])

                value, stat = self.zk.get("/curbroker2")
                ret = value.decode("utf-8")
                arr = ret.split()
                if arr:
                    self.mw_obj.lookup_bind(arr[0], arr[1])

                value, stat = self.zk.get("/curbroker3")
                ret = value.decode("utf-8")
                arr = ret.split()
                if arr:
                    self.mw_obj.lookup_bind(arr[0], arr[1])

                self.mw_obj.watch_znode_curbroker1_change()
                self.mw_obj.watch_znode_curbroker2_change()
                self.mw_obj.watch_znode_curbroker3_change()

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a isready request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None

            elif (self.state == self.State.LOOKUP):

                self.logger.info("SubcriberAppln::invoke_operation - LOOKUP State now activated")
                self.mw_obj.plz_lookup(self.topiclist)  # send the lookup request

                #self.mw_obj.event_loop()

                return None

            elif (self.state == self.State.ACCEPT):

                self.logger.info("SubcriberAppln::invoke_operation - start accepting dissemination")
                self.mw_obj.accepting = True


                if self.zk.exists("/numSubs"):
                    value, stat = self.zk.get("/numSubs")
                    value = value.decode("utf-8")

                    new_val = int(value) + 1
                    new_bytes = bytes(str(new_val), 'utf-8')
                    self.zk.set("/numSubs", new_bytes)

                #return to event loop to accept dissemination of topics
                return None

            else:
                raise ValueError("Undefined state of the appln object")

            self.logger.info("SubcriberAppln::invoke_operation completed")
        except Exception as e:
            raise e


    def register_response(self, register_resp):
        ''' handle register response '''
        try:
            self.logger.info("SubcriberAppln::register_response")
            if (register_resp.status != discovery_pb2.STATUS_FAILURE):
                self.logger.debug("SubcriberAppln::register_response - registration is a success")

                # set our next state to isready so that we can then send the isready message right away
                self.state = self.State.ISREADY

                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug(
                    "SubcriberAppln::register_response - registration is a failure with reason {}".format(
                        register_resp.reason))
                raise ValueError("Subcriber needs to have unique id")

        except Exception as e:
            raise e

        ########################################
        # handle isready response method called as part of upcall
        #
        # Also a part of upcall handled by application logic
        ########################################

    def isready_response(self, isready_resp):
        ''' handle isready response '''

        try:
            self.logger.info("SubcriberAppln::isready_response")

            # Notice how we get that loop effect with the sleep (10)
            # by an interaction between the event loop and these
            # upcall methods.

            if isready_resp.status == discovery_pb2.STATUS_FAILURE:
                # discovery service is not ready yet
                self.logger.debug("SubcriberAppln::driver - Not ready yet; check again")
                time.sleep(5)  # sleep between calls so that we don't make excessive calls

            else:
                # we got the go ahead
                # set the state to disseminate
                self.logger.info("SubcriberAppln::driver - LOOKUP STATE")
                self.state = self.State.LOOKUP

            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0

        except Exception as e:
            raise e


    def lookup_response(self, lookup_resp):
        ''' handle lookup response '''

        self.logger.info("SubcriberAppln::driver - Lookup Response")
        try:
            for tup in lookup_resp.array:
                addr_name = tup.addr
                port_name = tup.port

                self.mw_obj.lookup_bind(addr_name,port_name)

            self.state = self.State.ACCEPT

            self.logger.info("SubcriberAppln::driver - Lookup Response - State is now ACCEPT")

            return 0

        except Exception as e:
            raise e



    def dump(self):
        ''' Pretty print '''

        try:
            self.logger.info("**********************************")
            self.logger.info("SubcriberAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: {}".format(self.name))
            self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info("     Num Topics: {}".format(self.num_topics))
            self.logger.info("     TopicList: {}".format(self.topiclist))
            self.logger.info("**********************************")

        except Exception as e:
            raise e




def parseCmdLineArgs():
        # instantiate a ArgumentParser object
        parser = argparse.ArgumentParser(description="Subscriber Application")

        # Now specify all the optional arguments we support
        # At a minimum, you will need a way to specify the IP and port of the lookup
        # service, the role we are playing, what dissemination approach are we
        # using, what is our endpoint (i.e., port where we are going to bind at the
        # ZMQ level)

        parser.add_argument("-n", "--name", default="sub",
                            help="Some name assigned to us. Keep it unique per publisher")

        parser.add_argument("-a", "--addr", default="localhost",
                            help="IP addr of this publisher to advertise (default: localhost)")

        parser.add_argument("-p", "--port", default=1111,
                            help="Port number on which our underlying publisher ZMQ service runs, default=5577")

        parser.add_argument("-d", "--discovery", default="localhost:5555",
                            help="IP Addr:Port combo for the discovery service, default localhost:5555")

        parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

        parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=5,
                            help="Number of topics to publish, currently restricted to max of 9")

        parser.add_argument("-l", "--loglevel", type=int, default=logging.DEBUG,
                            choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                            help="logging level, choices 10,20,30,40,50: default 10=logging.DEBUG")

        parser.add_argument("-e", "--toggle", default=True, help="determine toggle for logging latency")

        parser.add_argument("-f", "--filename", default="latency1.json", help="filename for output")

        parser.add_argument("-zkp", "--zkPort", type=int, default=2181,
                            help="Port number on which our underlying publisher ZMQ service runs, default=5555")

        parser.add_argument("-zka", "--zkIPAddr", type=str, default="127.0.0.1",
                            help="ZooKeeper server port, default 2181")

        parser.add_argument("-his", "--history", type=int, default=10,
                            help="history paramater")

        return parser.parse_args()



###################################
#
# Main program
#
###################################
def main():
  try:
    # obtain a system wide logger and initialize it to debug level to begin with
    logging.info("Main - acquire a child logger and then log messages in the child")
    logger = logging.getLogger("SubscriberAppln")

    # first parse the arguments
    logger.debug("Main: parse command line arguments")
    args = parseCmdLineArgs()

    # reset the log level to as specified
    logger.debug("Main: resetting log level to {}".format(args.loglevel))
    logger.setLevel(args.loglevel)
    logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel()))

    # Obtain a publisher application
    logger.debug("Main: obtain the publisher appln object")
    sub_app = SubscriberAppln(logger)

    # configure the object
    logger.debug("Main: configure the publisher appln object")
    sub_app.configure(args)

    # now invoke the driver program
    logger.debug("Main: invoke the publisher appln driver")
    sub_app.driver()

  except Exception as e:
    logger.exception("Exception caught in main - {}".format(e))
    return


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
  # set underlying default logging capabilities
  logging.basicConfig(level=logging.DEBUG,
                      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

  main()


