class Router:
    """
    All the routing should go here
    """
    def __init__(self):
        self.routes = [
                        # healthcheck
                        ('^/healthcheck', 'index'),
                        # consumergroup related routes
                        ('^/consumergroup/(?P<group_id>.+)/(?P<topic>.+)/status', 'get_status'),
                        ('^/consumergroup/(?P<group_id>.+)/(?P<topic>.+)/total_lag', 'get_total_lag'),
                        # for backward compatibility for older client apps (kafka-backpressure-monitor)
                        ('^/consumergroup/(?P<group_id>.+)/topics/(?P<topic>.+)', 'get_total_lag'),
                        ('^/consumergroup/(?P<group_id>.+)/topics/', 'get_topics')
                       ]

    def __call__(self, *args, **kwargs):
        return self.routes