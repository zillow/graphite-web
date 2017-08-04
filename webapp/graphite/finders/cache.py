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

    def find_nodes(self, query, cache_incomplete_nodes=None, cache_states=None):
        clean_patterns = query.pattern.replace('\\', '')
        has_wildcard = clean_patterns.find('{') > -1 or clean_patterns.find('[') > -1 or clean_patterns.find('*') > -1 or clean_patterns.find('?') > -1

        if cache_incomplete_nodes is None:
            cache_incomplete_nodes = {}

        if cache_states is None:
            cache_states = {}

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
            exists = True
            partial_exists = True
            should_ignores = None
            # filter
            filtered_metrics = []
            for m, is_leaf in metrics:
                if is_leaf:
                    exist, partial_exist, should_ignore = CarbonLink.precheck(m, query.startTime)
                else:  # return True for BranchNode
                    exist, partial_exist, should_ignore = (True, True, False)

                exists = exists and exist
                partial_exists = partial_exists and partial_exist

                if should_ignores is None:
                    should_ignores = should_ignore
                else:
                    should_ignores = should_ignores and should_ignore

                if not should_ignore:
                    filtered_metrics.append((m, is_leaf))

            # If we can ignore, we then stop keep searching disk.
            # otherwise, we may end up by searching disk.
            cache_states["should_ignore"] = should_ignores

            if exists:
                for metric, is_leaf in filtered_metrics:
                    if is_leaf:
                        reader = CarbonCacheReader(metric)
                        yield LeafNode(metric, reader)
                    else:
                        yield BranchNode(metric)
            elif partial_exists:
                for metric, is_leaf in filtered_metrics:
                    if is_leaf:
                        reader = CarbonCacheReader(metric)
                        cache_incomplete_nodes[metric] = LeafNode(metric, reader)
                    else:
                        cache_incomplete_nodes[metric] = BranchNode(metric)
