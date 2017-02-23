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

        # 1) CarbonLink has some hosts
        # 2) has no wildcard
        if CarbonLink.hosts and not has_wildcard:
            metric = clean_patterns
            exists = CarbonLink.precheck(metric, query.startTime)
            if exists:
                metric_path = metric
                # TODO: check any info we need to put into reader @here
                reader = CarbonCacheReader(metric)
                yield LeafNode(metric_path, reader)
