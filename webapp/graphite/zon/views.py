import time
import subprocess
from urllib2 import urlopen
from graphite.compat import HttpResponse
from graphite.util import json
import random


def full_path_zon_test(request):
	# 0. Parse parameters
	queryParams = request.GET.copy()
	queryParams.update(request.POST)
	# TODO: remove the default values, the default values are what we are using currently
	port = queryParams.get('port','31509')
	metric_name = queryParams.get('metric', 'test.fullstack.yun')

	# 1. Send data to graphite
	random_data = _send_random_data(metric_name)

	# 2. Wait (allow some latency)
	time.sleep(1)

	# 3. Query graphite
	try:
		res = urlopen("http://localhost:{0}/render/?format=json&target={1}&from=-1min&noCache".format(port, metric_name))
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
		elif len(json_obj["datapoints"]) != 1:
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


def _send_random_data(metric_name):
	data = random.randint(0, 9)
	subprocess.call("echo \"{0} {1} `date +%s`\" | nc localhost 2003".format(metric_name, data), shell=True)
	return data
