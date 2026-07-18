khởi động: 
docker compose -f clickhouse_single/docker-compose.yml up -d

Kiểm tra cluster: 
curl "http://localhost:8125/" -d "SELECT cluster, shard_num, replica_num, host_name FROM system.clusters WHERE cluster='cluster_2s2r' FORMAT Pretty"

CHÚ Ý: Cụm chưa set node id