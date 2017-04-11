from graphite.logger import log
from graphite.node import BranchNode, LeafNode
from graphite.carbonlink import CarbonLink
from graphite.readers import CarbonCacheReader


class CarbonCacheFinder:
    """
    Designed to find any metric that exists in carbon cache, and create
    a node if exists.
    """
    def __init__(self):
        pass

    def find_nodes(self, query):
        clean_patterns = query.pattern.replace('\\', '')
        has_wildcard = clean_patterns.find('{') > -1 or clean_patterns.find('[') > -1 or clean_patterns.find('*') > -1 or clean_patterns.find('?') > -1

        # CarbonLink has some hosts
        if CarbonLink.hosts:
            metric = clean_patterns
            # query pattern has no wildcard
            if not has_wildcard:
                exists = CarbonLink.precheck(metric, query.startTime)
                if exists:
                    metric_path = metric
                    # TODO: check any info we need to put into reader @here
                    reader = CarbonCacheReader(metric)
                    yield LeafNode(metric_path, reader)
            else:
                # expand queries in CarbonLink
                metrics = CarbonLink.expand_query(metric)
                # check all metrics in same valid query range
                exists = all((CarbonLink.precheck(m, query.startTime) for m in metric))
                if exists:
                    for metric in metrics:
                        reader = CarbonCacheReader(metric)
                        yield LeafNode(metric, reader)
