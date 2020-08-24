from typing import Optional

import consul

client: Optional[consul.Consul] = None


def connect(host='localhost', port=8500):
    global client
    client = consul.Consul(host=host, port=port)


def check_connection(func):
    def wrapped(*args, **kwargs):
        if not client:
            raise RuntimeError('Not connected! Please connect to a Consul agent first using the "connect" method.')
        return func(*args, **kwargs)
    return wrapped


@check_connection
def get_service_address(service: str) -> str:
    index, service_defs = client.catalog.service(service)
    if not service_defs or len(service_defs) == 0:
        raise Exception('Service {} is not registered to the Consul agent.')
    service_def = service_defs.pop()
    return '{}:{}'.format(service_def['Address'], service_def['ServicePort'])
