"""
Send SolarEdge performance metrics to Graphite.

By default the metrics are sent to the Graphite/carbon pickle listener port
(usually 2004), which accepts batches of metrics in a pickled nested list of
tuples format. With the -t option they are sent to the plain text listener
port (usually 2003) instead, one metric at a time.

The `Metrics` class converts a nested dictionary (such as produced by
`json.loads` on some SolarEdge traffic that has been processed by semonitor)
into the nested list of tuples structure expected by carbon. `sendPickle`
pickles it, adds the requisite header, and sends it to Graphite, `sendText`
sends each of the metrics as a line of text.

You may need to adjust the delay parameter, depending upon the capacity and
busyness of the Graphite server. I have found 0.2 works fine **for me** in
production mode, when I am "following" some existing metrics in real time, as
the SolarEdge inverter emits them, but I needed to increase it to as much as
5.0 the first time I sent a new batch of metrics to Graphite, otherwise the
carbon listener seemed to be swamped and didn't have time to create all the
new metrics. To be completely precise, I had problems when I sent an existing
batch of metrics to a new "test" base, which meant Graphite / carbon had to
create a whole lot of new whisper files all at once.
"""

import getopt
import json
import math
import pickle
import socket
import struct
import sys
import time

from se.metrics import open_input, unwrap_metricsDict

try:
    import syslog
except ImportError:
    # Allow for the fact that syslog is not (to my knowledge) available on Windows
    from se import winsyslog as syslog


# log a message to syslog
def log(*args):
    message = args[0] + " "
    for arg in args[1:]:
        message += arg.__str__() + " "
    # TODO: Make this align better with the multiple logging options available elsewhere in semonitor
    syslog.syslog(message)


# Time to wait, in seconds, after sending a batch (or for more data when following).
delay = 0.2
# Time to wait, in seconds, after sending a single metric as text.
textDelay = 0.1


def getJsonStr(inFile, following):
    jsonStr = inFile.readline()
    if jsonStr == "" and following:
        while jsonStr == "":
            # wait for more data
            time.sleep(delay)
            jsonStr = inFile.readline()
    return jsonStr


class Metrics(list):
    """
    Convert a (json) nested dictionary of dictionaries of metrics into a list
    of (name, (timestamp, value)) tuples, the format expected by the Graphite
    (carbon) pickle interface, and send them off to carbon.
    """

    def __init__(self, metricsDict, base):
        for baseName, devAttrs in unwrap_metricsDict(metricsDict):
            # convert date and time to Unix time
            try:
                (year, month, day) = devAttrs["Date"].split("-")
                (hour, minute, second) = devAttrs["Time"].split(":")
            except KeyError:
                log(
                    "Date or Time is missing or incorrectly formatted for this set of metrics"
                )
                (year, month, day, hour, minute, second) = (1970, 1, 1, 0, 1, 1)
            # Set the dst parameter in mktime to -1, so that the system determines whether dst is in effect!
            # Without this, when dst is in effect, the timeStamp is 3600 seconds into the future!
            timeStamp = time.mktime(
                (
                    int(year),
                    int(month),
                    int(day),
                    int(hour),
                    int(minute),
                    int(second),
                    0,
                    0,
                    -1,
                )
            )
            # Treat every attribute as a metric - except for non-numeric ones!
            for devAttr in devAttrs:
                if devAttr not in {"Date", "Time", "Undeciphered_data"}:
                    try:
                        # Weed out attributes with non numeric values (Graphite does this too, but why clog the network?)
                        test = float(devAttrs[devAttr])
                        if math.isnan(test):
                            # It's a NaN!
                            pass
                        else:
                            fullName = f"{base}{baseName}.{devAttr}"
                            # A nested tuple as per the expectations of the pickle Graphite interface
                            self.append(
                                (fullName, (timeStamp, str(devAttrs[devAttr])))
                            )
                    except ValueError:
                        # It is not a numeric metric
                        pass

    def sendPickle(self, graphiteSocket):
        # Remove the non list attributes from self, to keep graphite (actually the carbon listener) happy.
        payload = pickle.dumps(list(self))
        header = struct.pack("!L", len(payload))
        graphiteSocket.send(header + payload)
        time.sleep(delay)

    def sendText(self, graphiteSocket):
        for fullName, (timeStamp, value) in self:
            metric = f"{fullName} {value} {timeStamp}\n"
            graphiteSocket.send(metric)
            time.sleep(textDelay)


def main(argv=None):
    # parameter defaults
    base = ""
    hostName = "localhost"
    # The port must be the one which Graphite (carbon) has configured to be the
    # listener port of the protocol in use. The usual Graphite installation
    # defaults are 2004 for the pickle and 2003 for the text protocol.
    port = None
    text = False
    following = False

    # get program arguments and options
    (opts, args) = getopt.getopt(
        sys.argv[1:] if argv is None else argv, "b:fh:p:t"
    )
    inFile = open_input(args)
    for opt in opts:
        if opt[0] == "-b":
            base = opt[1] + "."
        if opt[0] == "-h":
            hostName = opt[1]
        if opt[0] == "-p":
            port = int(opt[1])
        if opt[0] == "-f":
            following = True
        if opt[0] == "-t":
            text = True
    if port is None:
        port = 2003 if text else 2004

    # Timeout if no connection after 10 seconds,
    #   in which case check that Graphite is running, and the hostName and port are correct.
    socket.setdefaulttimeout(10.0)
    graphiteSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    graphiteSocket.connect((hostName, port))
    with inFile:
       jsonStr = getJsonStr(inFile, following)
       while jsonStr != "":
           try:
               metrics = Metrics(json.loads(jsonStr), base=base)
               if text:
                   metrics.sendText(graphiteSocket)
               else:
                   metrics.sendPickle(graphiteSocket)
           except ValueError:
               log(
                   "WARNING json.loads had a problem with the following jsonStr\n",
                   jsonStr,
                   "\n",
                   "=" * 80,
                   "\n",
               )
        jsonStr = getJsonStr(inFile, following)
        while jsonStr != "":
            if text:
                Metrics(json.loads(jsonStr), base=base).sendText(graphiteSocket)
            else:
                try:
                    Metrics(json.loads(jsonStr), base=base).sendPickle(
                        graphiteSocket
                    )
                except ValueError:
                    log(
                        "WARNING json.loads had a problem with the following jsonStr\n",
                        jsonStr,
                        "\n",
                        "=" * 80,
                        "\n",
                    )
            jsonStr = getJsonStr(inFile, following)
    graphiteSocket.close()


if __name__ == "__main__":
    main()
