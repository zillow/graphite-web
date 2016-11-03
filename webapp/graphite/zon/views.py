import time
import subprocess
from urllib2 import urlopen
from graphite.compat import HttpResponse
from graphite.util import json
import random


def full_path_zon_test(request):
	# 1. Send data to graphite
	random_data = _send_random_data()

	# 2. Wait (allow some latency)
	time.sleep(0.030)

	# 3. Query graphite
	try:
		res = urlopen("http://localhost:31509/render/?format=json&target=test.fullstack.yun&from=-1min")
		s = res.read().decode('utf-8')
		json_obj = json.loads(s)
	except Exception:
		raise

	# 4. Check Result
	result = ""
	details = ""
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


def _send_random_data():
	data = random.randint(0, 9)
	subprocess.call("echo \"test.fullstack.yun {0} `date +%s`\" | nc localhost 2003".format(data), shell=True)
	return data
