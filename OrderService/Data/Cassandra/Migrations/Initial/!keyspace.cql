CREATE KEYSPACE IF NOT EXISTS order_service_keyspace
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
        };