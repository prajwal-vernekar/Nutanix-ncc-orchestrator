# Nutanix NCC Orchestrator + Prometheus Monitoring

monitoring for Nutanix clusters using NCC Orchestrator → node_exporter textfile collector → Prometheus → Grafana)
​

## Architecture

```
Nutanix Clusters (10.48.*) 
     ↓ NCC API (4h cron)
NCC Orchestrator (.prom files)
     ↓ /var/lib/node_exporter/textfile/
node_exporter (:9100) ✓ 39 metrics
     ↓ Prometheus (:9090)
Grafana (:3000) + Alerts
```
Metrics Exposed
| Metric	| Type	| Description | PromQL Example|
| --------| ---------| ---------------------- | -------------- |
|`nutanix_ncc_check_result`	| gauge	| Individual NCC checks (1=present)	| nutanix_ncc_check_result{severity="FAIL"}
|`​nutanix_ncc_check_summary_total`	| gauge	| Counts per severity	|sum(nutanix_ncc_check_summary_total{severity="FAIL"}) by (cluster)|
|`nutanix_ncc_check_total`	| gauge	| Total checks per cluster	|nutanix_ncc_check_total|


## Prerequisites
- Linux VM (bastion) with node_exporter running
- NCC Orchestrator binary (ncc-orchestrator-linux-amd64)
- Nutanix cluster Prism credentials

## Deployment
1. Node Exporter + Textfile Collector

  - Install node_exporter
```
wget https://github.com/prometheus/node_exporter/releases/download/v1.8.2/node_exporter-1.8.2.linux-amd64.tar.gz
tar xvfz node_exporter-*.tar.gz
sudo mv node_exporter-*/node_exporter /usr/local/bin/
sudo useradd --no-create-home --shell /bin/false node_exporter
sudo mkdir -p /var/lib/node_exporter/textfile
```
  - Systemd service
```
sudo tee /etc/systemd/system/node_exporter.service <<EOF
[Unit]
Description=Node Exporter
Wants=network-online.target
After=network-online.target

[Service]
User=node_exporter
Group=node_exporter
Type=simple
ExecStart=/usr/local/bin/node_exporter \\
  --collector.textfile.directory=/var/lib/node_exporter/textfile \\
  --web.listen-address=:9100
Restart=always

[Install]
WantedBy=default.target
EOF
```
  - Enable Service  
```
sudo systemctl daemon-reload
sudo systemctl enable --now node_exporter
```  
  - Verify:  
```curl localhost:9100/metrics | grep node_textfile_mtime_seconds```
​

2. NCC Orchestrator Setup

```
sudo mkdir -p /opt/nutanix-ncc
sudo cp ncc-orchestrator-linux-amd64 /opt/nutanix-ncc/
sudo chmod 755 /opt/nutanix-ncc/ncc-orchestrator-linux-amd64
sudo chown node_exporter:node_exporter /var/lib/node_exporter/textfile
```

  - Test run:

```
cd /opt/nutanix-ncc
sudo ./ncc-orchestrator-linux-amd64 \
  --clusters="10.48.52.74,10.48.52.75,10.48.70.178,10.48.108.245" \
  --prom-dir=/var/lib/node_exporter/textfile \
  --insecure-skip-verify \
  --password=Simps/4u
```

  - Verify:
    ```ls -la /var/lib/node_exporter/textfile/*.prom → .prom files generated```

3. Fix .prom Format (Critical)

  - Node_exporter requires metric{labels} 1 format:

```
for f in /var/lib/node_exporter/textfile/*.prom; do
  sed -i 's/\(nutanix_ncc_check_result{[^}]*}\)\([[:space:]]*\)$/\1 1\2/' "$f"
done
```
Verify metrics: ```curl localhost:9100/metrics | grep -c '^nutanix_ncc_'```
​

4. Production Cron (Every 4h)

```
sudo crontab -e
```
```
# NCC Orchestrator - Every 4 hours
0 */4 * * * cd /opt/nutanix-ncc && \
  ./ncc-orchestrator-linux-amd64 \
  --clusters="10.48.52.74,10.48.52.75,10.48.70.178,10.48.108.245" \
  --prom-dir=/var/lib/node_exporter/textfile \
  --insecure-skip-verify --password=Simps/4u && \
  chown node_exporter:node_exporter /var/lib/node_exporter/textfile/*.prom \
  >> /var/log/ncc-orchestrator.log 2>&1
```

5. Prometheus

```
# Config: /etc/prometheus/prometheus.yml
global:
  scrape_interval: 5m

scrape_configs:
  - job_name: 'bastion-node'
    static_configs:
      - targets: ['localhost:9100']
  - job_name: 'nutanix-ncc'
    static_configs:
      - targets: ['localhost:9100']
    scrape_interval: 5m

```
  - rule_files:  
    - /etc/prometheus/ncc.rules.yml

```
# Rules: /etc/prometheus/ncc.rules.yml
groups:
  - name: ncc
    rules:
      - alert: NCCFailures
        expr: sum(nutanix_ncc_check_summary_total{severity="FAIL"}) by (cluster) > 0
        for: 30m
        labels: {severity: critical}
        annotations:
          summary: "NCC FAIL on {{ $labels.cluster }}"
      
      - alert: NCCWarnings
        expr: sum(nutanix_ncc_check_summary_total{severity="WARN"}) by (cluster) > 0
        for: 30m
        labels: {severity: warning}
      
      - alert: NCCStaleData
        expr: (time() - node_textfile_mtime_seconds{file=~".*prom"}) > 21600
        for: 1h
        labels: {severity: warning}
```
Start: ```sudo systemctl enable --now prometheus```

Verify:  
```curl http://localhost:9090/targets → nutanix-ncc UP```

6. Grafana Dashboard


Install: ```sudo apt install grafana && sudo systemctl enable --now grafana-server```

Access: ```http://bastion:3000 (admin/admin)```

Prometheus datasource: ```http://localhost:9090```

## Dashboard queries:

```
- Critical: sum(nutanix_ncc_check_summary_total{severity=~"FAIL|WARN|ERR"}) by (cluster)
- Table: nutanix_ncc_check_result{severity!="INFO"}
- Total: nutanix_ncc_check_total
```

## Troubleshooting
|Issue|	Fix|
|---- | ----| 
|failed to parse textfile data | 	```sed -i 's/\(nutanix_ncc_check_result{[^}]*}\)$/\1 1/' *.prom```|​
|No NCC metrics	|```curl localhost:9100/metrics \| grep nutanix_ncc_check_total```|
|Cron not running	|```sudo tail /var/log/ncc-orchestrator.log```|
| Prometheus targets DOWN	|Check localhost:9100 accessibility|

### Grafana Panels

1. **Stat**: sum(nutanix_ncc_check_summary_total{severity=~"FAIL|WARN"}) by (cluster)
2. **Table**: nutanix_ncc_check_result{severity!="INFO"}
3. **Bar**: nutanix_ncc_check_total by (cluster)
4. **Heatmap**: count(nutanix_ncc_check_result) by (severity, cluster)

## Alerting
NCCFailures: FAIL > 0 → critical

NCCWarnings: WARN > 0 → warning

NCCStaleData: Files >6h old → warning
