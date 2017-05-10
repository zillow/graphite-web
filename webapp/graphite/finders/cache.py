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

    def find_nodes(self, query, cache_incomplete_nodes=None):
        clean_patterns = query.pattern.replace('\\', '')
        has_wildcard = clean_patterns.find('{') > -1 or clean_patterns.find('[') > -1 or clean_patterns.find('*') > -1 or clean_patterns.find('?') > -1

        if cache_incomplete_nodes is None:
            cache_incomplete_nodes = {}

        # CarbonLink has some hosts
        if CarbonLink.hosts:
            metric = clean_patterns

            # Let's combine these two cases:
            # 1) has_wildcard
            # 2) single metric query
            # Expand queries in CarbonLink
            # we will get back a list of tuples (metric_name, is_leaf) here.
            # For example,
            # [(metric1, False), (metric2, True)]
            metrics = CarbonLink.expand_query(metric)
            # dedup, because of BranchNodes
            metrics = list(set(metrics))
            # check all metrics in same valid query range
            prechecks = []
            for m, is_leaf in metrics:
                if is_leaf:
                    prechecks.append(CarbonLink.precheck(m, query.startTime))
                else:  # return True for BranchNode
                    prechecks.append((True, True))
            exists = all((exist for exist, partial_exist in prechecks))
            partial_exists = all((partial_exist for exist, partial_exist in prechecks))
            if exists:
                for metric, is_leaf in metrics:
                    if is_leaf:
                        reader = CarbonCacheReader(metric)
                        yield LeafNode(metric, reader)
                    else:
                        yield BranchNode(metric)
            elif partial_exists:
                for metric, is_leaf in metrics:
                    if is_leaf:
                        reader = CarbonCacheReader(metric)
                        cache_incomplete_nodes[metric] = LeafNode(metric, reader)
                    else:
                        cache_incomplete_nodes[metric] = BranchNode(metric)
