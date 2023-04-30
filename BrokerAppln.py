###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 


# import the needed packages
import os  # for OS functions
import sys  # for syspath and system exception
import time  # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging  # for logging. Use it in place of print statements.
import atexit
import json

import zmq

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.BrokerMW import BrokerMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

from kazoo.client import KazooClient

class BrokerAppln():

    # these are the states through which our broker appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then
    # take decisions accordingly
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        ADDPUBS = 4,
        DISSEMINATION = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE  # state that are we in
        self.logger = logger  # internal logger for print statements
        self.poller = None  # used to wait on incoming replies
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.num_topics = None
        self.mw_obj = None  # handle to the underlying Middleware object

        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics

        self.group = None


    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("BrokerAppln::configure")

            self.port = args.port
            self.addr = args.addr

            atexit.register(self.exitfunc)

            self.zkIPAddr = args.zkIPAddr
            self.zkPort = args.zkPort
            hosts = self.zkIPAddr + str(":") + str(self.zkPort)
            self.zk = KazooClient(hosts)
            self.zk.start()

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            self.name = args.name  # our name
            self.num_topics = args.num_topics  # total num of topics we publish

            # Now, get the configuration object
            self.logger.debug("BrokerAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Now get our topic list of interest
            self.logger.debug("BrokerAppln::configure - selecting our topic list")
            #ts = TopicSelector()
            #self.topiclist = ts.interest(self.num_topics)  # let topic selector give us the desired num of topics

            self.group = args.group

            if (self.group == 1):
                self.topiclist = ["weather", "humidity", "airquality"]

            elif (self.group == 2):
                self.topiclist = ["light", "pressure", "temperature"]

            elif (self.group == 3):
                self.topiclist = ["sound", "altitude", "location"]


            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug("BrokerAppln::configure - initialize the middleware object")
            self.mw_obj = BrokerMW(self.logger)

            if self.zk.exists("/curDiscovery"):

                # Now acquire the value and stats of that znode
                # value,stat = self.zk.get (self.zkName, watch=self.watch)
                value, stat = self.zk.get("/curDiscovery")
                bindstring = value.decode("utf-8")


            self.update_brokerlist()

            self.mw_obj.configure(args, bindstring)  # pass remainder of the args to the m/w object

            self.logger.info("BrokerAppln::configure - configuration complete")


        except Exception as e:
            raise e


    ########################################
    # driver program
    ########################################
    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("BrokerAppln::driver")

            # dump our contents (debugging purposes)
            self.dump()

            # First ask our middleware to keep a handle to us to make upcalls.
            # This is related to upcalls. By passing a pointer to ourselves, the
            # middleware will keep track of it and any time something must
            # be handled by the application level, invoke an upcall.
            self.logger.debug("BrokerAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)

            # the next thing we should be doing is to register with the discovery
            # service. But because we are simply delegating everything to an event loop
            # that will call us back, we will need to know when we get called back as to
            # what should be our next set of actions.  Hence, as a hint, we set our state
            # accordingly so that when we are out of the event loop, we know what
            # operation is to be performed.  In this case we should be registering with
            # the discovery service. So this is our next state.
            self.state = self.State.REGISTER

            # Now simply let the underlying middleware object enter the event loop
            # to handle events. However, a trick we play here is that we provide a timeout
            # of zero so that control is immediately sent back to us where we can then
            # register with the discovery service and then pass control back to the event loop
            #
            # As a rule, whenever we expect a reply from remote entity, we set timeout to
            # None or some large value, but if we want to send a request ourselves right away,
            # we set timeout is zero.
            #
            self.mw_obj.event_loop(timeout=0)  # start the event loop

            self.logger.info("BrokerAppln::driver completed")

        except Exception as e:
            raise e

    ########################################
    # handle register response method called as part of upcall
    #
    # As mentioned in class, the middleware object can do the reading
    # from socket and deserialization. But it does not know the semantics
    # of the message and what should be done. So it becomes the job
    # of the application. Hence this upcall is made to us.
    ########################################
    def register_response(self, reg_resp):
        ''' handle register response '''

        try:
            self.logger.info("BrokerAppln::register_response")

            if (reg_resp.status != discovery_pb2.STATUS_FAILURE):
                self.logger.debug("BrokerAppln::register_response - registration is a success")

                # set our next state to isready so that we can then send the isready message right away
                self.state = self.State.ISREADY

                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug(
                    "BrokerAppln::register_response - registration is a failure with reason {}".format(
                        reg_resp.reason))
                raise ValueError("Publisher needs to have unique id")

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
            self.logger.info("BrokerAppln::isready_response")

            # Notice how we get that loop effect with the sleep (10)
            # by an interaction between the event loop and these
            # upcall methods.
            if isready_resp.status == discovery_pb2.STATUS_FAILURE:
                # discovery service is not ready yet
                self.logger.debug("BrokerAppln::driver - Not ready yet; check again")
                time.sleep(5)  # sleep between calls so that we don't make excessive calls

            else:
                # we got the go ahead
                # set the state to disseminate
                self.logger.debug("BrokerAppln::driver - Add publishers")
                self.state = self.State.ADDPUBS

            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0

        except Exception as e:
            raise e


    def pubslookup_response(self, pubslookup_resp):

        try:
            self.logger.info("BrokerAppln::pubslookup_response started")
            for tup in pubslookup_resp.array:
                addr = tup.addr
                port = tup.port
                self.mw_obj.lookup_bind(addr, port)

            self.state = self.State.DISSEMINATION

            self.logger.info("BrokerAppln::pubslookup_response completed")

        except Exception as e:
            raise e


    def invoke_operation(self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info("BrokerAppln::invoke_operation")

            # check what state are we in. If we are in REGISTER state,
            # we send register request to discovery service. If we are in
            # ISREADY state, then we keep checking with the discovery
            # service.
            if (self.state == self.State.REGISTER):
                # send a register msg to discovery service
                self.logger.debug("BrokerAppln::invoke_operation - register with the discovery service")
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

                self.logger.debug("BrokerAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready()  # send the is_ready? request

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a isready request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None

            elif (self.state == self.State.ADDPUBS):

                # We are here because both registration and is ready is done. So the only thing
                # left for us as a publisher is dissemination, which we do it actively here.
                # self.mw_obj.event_loop()

                self.logger.debug("BrokerAppln::invoke_operation - add publishers")
                self.mw_obj.request_pubs()

                self.mw_obj.watch_znode_pubweather_change()
                self.mw_obj.watch_znode_pubhumidity_change()
                self.mw_obj.watch_znode_pubairquality_change()
                self.mw_obj.watch_znode_publight_change()
                self.mw_obj.watch_znode_pubpressure_change()
                self.mw_obj.watch_znode_pubsound_change()
                self.mw_obj.watch_znode_pubtemperature_change()
                self.mw_obj.watch_znode_pubaltitude_change()
                self.mw_obj.watch_znode_publocation_change()

                return None

            elif (self.state == self.State.DISSEMINATION):

                self.logger.info("BrokerAppln::invoke_operation - Dissemination through broker can now begin")


                return None

            else:
                raise ValueError("Undefined state of the appln object")

            self.logger.info("BrokerAppln::invoke_operation completed")
        except Exception as e:
            raise e

    ########################################
    # dump the contents of the object
    ########################################
    def dump(self):
        ''' Pretty print '''

        try:
            self.logger.info("**********************************")
            self.logger.info("BrokerAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: {}".format(self.name))
            self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info("     Dissemination: {}".format(self.dissemination))
            self.logger.info("     Num Topics: {}".format(self.num_topics))
            self.logger.info("     TopicList: {}".format(self.topiclist))
            self.logger.info("**********************************")

        except Exception as e:
            raise e


    def update_brokerlist(self):

        try:
            file = "/brokerlist" + str(self.group)

            if self.zk.exists(file):

                # Now acquire the value and stats of that znode
                # value,stat = self.zk.get (self.zkName, watch=self.watch)
                value = self.zk.get(file)[0]
                value = value.decode("utf-8")

                if value:
                    add = "," + str(self.addr) + " " + str(self.port)
                else:
                    add = str(self.addr) + " " + str(self.port)

                new_string = value + add

                new_string = bytes(new_string, 'utf-8')
                self.zk.set(file, new_string)

            else:
                print("{} znode does not exist, why?".format(file))

        except Exception as e:
            raise e


    def leaderelection(self):

        try:

            file = "/brokerlist" + str(self.group)
            cur_file = "/curbroker" + str(self.group)

            if self.zk.exists(file):

                # Now acquire the value and stats of that znode
                # value,stat = self.zk.get (self.zkName, watch=self.watch)
                value = self.zk.get(file)[0]
                ret = value.decode("utf-8")

                if ret.endswith(","):
                    ret = ret[:-1]

                arr = ret.split(',')
                arr.pop(0)

                if arr:
                    new_broker = arr[0]

                    if self.zk.exists(cur_file):
                        new_bytes = bytes(new_broker, 'utf-8')
                        self.zk.set(cur_file, new_bytes)

                else:
                    print("BrokerAppln: LEADER ELECTION NOT AVAILABLE - not enough brokers available")


                listToStr = ' '.join([str(elem) for elem in arr])
                new_bytes_list = bytes(listToStr, 'utf-8')
                self.zk.set(file, new_bytes_list)


            else:
                print("{} znode does not exist, why?".format(file))

        except Exception as e:
            raise e


    def exitfunc(self):
        self.leaderelection()
        print ("Broker has successfully ended")



def parseCmdLineArgs():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser(description="Broker Application")

  # Now specify all the optional arguments we support
  # At a minimum, you will need a way to specify the IP and port of the lookup
  # service, the role we are playing, what dissemination approach are we
  # using, what is our endpoint (i.e., port where we are going to bind at the
  # ZMQ level)

  parser.add_argument("-n", "--name", default="broker", help="Some name assigned to us. Keep it unique per publisher")

  parser.add_argument("-a", "--addr", default="localhost",
                      help="IP addr of this publisher to advertise (default: localhost)")

  parser.add_argument("-p", "--port", type=int, default=5570,
                      help="Port number on which our underlying publisher ZMQ service runs, default=5570")

  parser.add_argument("-d", "--discovery", default="localhost:5555",
                      help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=9,
                      help="Number of topics to publish, currently restricted to max of 9")

  parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

  parser.add_argument("-f", "--frequency", type=int, default=1,
                      help="Rate at which topics disseminated: default once a second - use integers")

  parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                      choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                      help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

  parser.add_argument("-zkp", "--zkPort", type=int, default=2181,
                      help="Port number on which our underlying publisher ZMQ service runs, default=5555")

  parser.add_argument("-zka", "--zkIPAddr", type=str, default="127.0.0.1",
                      help="ZooKeeper server port, default 2181")

  parser.add_argument("-g", "--group", type=int, choices=range(1, 3), default=1,
                      help="broker group")

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
    logger = logging.getLogger("BrokerAppln")

    # first parse the arguments
    logger.debug("Main: parse command line arguments")
    args = parseCmdLineArgs()

    # reset the log level to as specified
    logger.debug("Main: resetting log level to {}".format(args.loglevel))
    logger.setLevel(args.loglevel)
    logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel()))

    # Obtain a publisher application
    logger.debug("Main: obtain the broker appln object")
    driver_app = BrokerAppln(logger)

    # configure the object
    logger.debug("Main: configure the broker appln object")
    driver_app.configure(args)

    # now invoke the driver program
    logger.debug("Main: invoke the broker appln driver")
    driver_app.driver()

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
