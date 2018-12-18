import time
import subprocess
from urllib2 import urlopen
from graphite.compat import HttpResponse
from graphite.util import json
import random
import socket


class GraphiteConn:

    def __init__(self, host="localhost", port=2003, attempts=3):
        self.host = host
        self.port = port
        self.attempts = attempts
        self.conn = self._create_socket()

    def write_metric(self, metric, value, timestamp):
        line = "{0} {1} {2}\n".format(metric, value, timestamp)
        for attempt in range(self.attempts):
            if self.conn:
                try:
                    self.conn.sendall(line)
                    return
                except socket.error:
                    pass
            self.conn = self._create_socket()

    def close(self):
        """
        close conn to graphite server
        """
        try:
            if self.conn:
                self.conn.close()
        except Exception as e:
            raise e

    def _create_socket(self):
        """
        creates a socket
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
        except Exception:
            sock = None
        return sock

# Create a connection to graphite server
graphite_conn = GraphiteConn()

def full_path_zon_test(request):
    # 0. Parse parameters
    queryParams = request.GET.copy()
    queryParams.update(request.POST)
    # TODO: remove the default values, the default values are what we are using currently
    port = queryParams.get('port','31509')
    metric_name = queryParams.get('metric', 'test.fullstack.graphite')

    # 1. Send data to graphite
    timestamp = int(time.time())
    frm = timestamp - (timestamp % 60) - 30
    random_data = _send_random_data(metric_name, timestamp)

    # 2. Wait (allow some latency)
    time.sleep(1)

    # 3. Query graphite
    try:
        res = urlopen("http://localhost:{0}/render/?format=json&target={1}&from={2}&cacheOnly".format(port, metric_name, str(frm)))
        s = res.read().decode('utf-8')
        json_obj = json.loads(s)
    except Exception:
        raise

    # 4. Check Result
    result = ""
    details = "Expected value: {0}".format(random_data)
    if not json_obj:
        result = "fail"
        details = "graphite query response is empty"
    else:
        json_obj = json_obj[0]
        if "target" not in json_obj:
            result = "fail"
            details = "target field is missing"
        elif "datapoints" not in json_obj:
            result = "fail"
            details = "datapoints field is missing"
        elif len(json_obj["datapoints"]) < 1 or len(json_obj["datapoints"]) > 2: # allow graphite return 2 intervals, (the latest one is likely None)
            result = "fail"
            details = "the number of returned datapoints is inconsistent"
        elif json_obj["datapoints"][0][0] is None:
            result = "fail"
            details = "datapoint is returned but its value is None"
        else:
            real_value = int(json_obj["datapoints"][0][0])
            expected_value = random_data
            if real_value == expected_value:
                result = "pass"
            else:
                result = "fail"
                details = "Expected value: {0}, Real Value: {1}".format(expected_value, real_value)

    # 5. Response
    result_json_obj = {"result": result, "details": details}
    response = HttpResponse(content=json.dumps(result_json_obj),
                            content_type='application/json')
    return response

def _send_random_data(metric_name, timestamp):
    data = random.randint(0, 9)
    graphite_conn.write_metric(metric_name, data, timestamp)
    return data
