from graphite.compat import HttpResponse
from graphite.util import json
from graphite.carbonlink import CarbonLink


def cache_metric(request):
	queryParams = request.GET.copy()
	metric = queryParams.get('metric','yun.test')
	datapoints = CarbonLink.query(metric)
	result_json_obj = {
		"target": metric,
		"datapoints": datapoints
	}
	response = HttpResponse(content=json.dumps(result_json_obj),
                            content_type='application/json')
	return response