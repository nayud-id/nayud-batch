CREATE KEYSPACE IF NOT EXISTS {{KEYSPACE}}
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  '{{DATACENTER}}': {{RF}}
}
AND durable_writes = {{DURABLE_WRITES}};