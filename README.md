# IoTS Projekat 2 — Message Broker Benchmark

Asinhroni event-driven mikroservisni sistem za IoT podatke iz smart agriculture dataseta.

## Arhitektura

- **Data Ingestion** (C# .NET 8) — čita `BIED_Smart_Agriculture_Dataset.csv` u batch-evima od 20 zapisa na svakih 5 sekundi i odmah šalje na broker
- **Data Storage** (Node.js) — pretplaćen na broker, upisuje poruke u PostgreSQL (batch upis na 500 poruka u benchmark scenarijima A/C)
- **Analytics** (Python FastAPI) — stream processing sa 10s tumbling window i detekcijom praga temperature
- **PostgreSQL** — efemerna baza (podaci se brišu pri `docker compose down`)
- **Broker** — MQTT (Mosquitto) ili Kafka (KRaft, bez Zookeepera)

## Preduslovi

- Docker i Docker Compose
- Bash (Git Bash ili WSL) za benchmark skripte
- Python 3 (za agregaciju `docker stats` rezultata)

## Brzo pokretanje (MQTT)

```bash
cp .env.example .env
docker compose --profile mqtt --profile ingestion up --build
```

## Pokretanje sa Kafka brokerom

```bash
cp .env.example .env
# U .env postavi: BROKER_TYPE=kafka
docker compose --profile kafka --profile ingestion up --build
```

## Konfiguracija

| Varijabla | Podrazumevano | Opis |
|-----------|---------------|------|
| `BROKER_TYPE` | `mqtt` | `mqtt` ili `kafka` |
| `MQTT_TOPIC` | `iot/agriculture/sensors` | MQTT topic |
| `KAFKA_TOPIC` | `iot-agriculture-sensors` | Kafka topic |
| `BATCH_SIZE` | `20` | Broj zapisa po batch-u (ingestion) |
| `BATCH_INTERVAL_SECONDS` | `5` | Pauza između batch-eva (ingestion) |
| `STORAGE_BATCH_SIZE` | `1` | Batch upis u PostgreSQL (`500` za benchmark A/C) |
| `MQTT_SUBSCRIBE_QOS` | `2` | QoS pretplate storage servisa |
| `TEMP_ALERT_THRESHOLD` | `50` | Prag temperature za alarm (°C) |
| `BENCHMARK_INSTANT_ALERT` | `false` | Instant alarm za scenario D |
| `BENCHMARK_ALERT_THRESHOLD` | `40` | Prag za benchmark scenario D |

## Provera rada

```bash
docker compose logs -f data-ingestion
docker compose logs -f data-storage
docker compose logs -f analytics
docker compose exec postgres psql -U iot -d iot_agriculture -c "SELECT COUNT(*) FROM sensor_readings;"
curl http://localhost:8000/health
curl http://localhost:8000/metrics
curl http://localhost:3000/metrics
```

## Zaustavljanje

```bash
docker compose --profile mqtt --profile ingestion down
```

---

## Eksperimentalni benchmark scenariji

Scenariji su **nezavisni** i pokreću se **sekvencijalno** (nikad paralelno). Rezultati se čuvaju u `results/scenario-{a|b|c|d}/{mqtt|kafka}/`.

### Pokretanje (Bash)

```bash
# Scenario A — Massive Sensor Ingestion
bash benchmarks/mqtt/scenario-a/run_100_qos0.sh
bash benchmarks/kafka/scenario-a/run_100_acks1.sh
bash benchmarks/mqtt/scenario-a/run_all.sh   # svih 9 MQTT konfiguracija

# Scenario B — Edge Connectivity Failures
bash benchmarks/mqtt/scenario-b/run.sh
bash benchmarks/kafka/scenario-b/run.sh

# Scenario C — Burst Event Load
bash benchmarks/mqtt/scenario-c/run.sh
bash benchmarks/kafka/scenario-c/run.sh

# Scenario D — Real-Time Alerting
bash benchmarks/mqtt/scenario-d/run.sh
bash benchmarks/kafka/scenario-d/run.sh
```

### Pokretanje (PowerShell)

```powershell
.\benchmarks\ps1\Run-ScenarioA-Mqtt.ps1
.\benchmarks\ps1\Run-ScenarioA-Kafka.ps1
.\benchmarks\ps1\Run-ScenarioB-Mqtt.ps1
.\benchmarks\ps1\Run-ScenarioB-Kafka.ps1
.\benchmarks\ps1\Run-ScenarioC-Mqtt.ps1
.\benchmarks\ps1\Run-ScenarioC-Kafka.ps1
.\benchmarks\ps1\Run-ScenarioD-Mqtt.ps1
.\benchmarks\ps1\Run-ScenarioD-Kafka.ps1
```

Za svaki run generišu se:
- `{config}_{timestamp}.txt` — summary metrike
- `{config}_{timestamp}_stats.csv` — raw `docker stats` uzorci
- `{config}_{timestamp}_resources.json` — avg/peak CPU, RAM, Network po kontejneru
- `summary.csv` — agregat za popunjavanje tabela ispod

**Napomena:** Vrednosti u tabelama su u formatu `avg/peak`. Popuni ih iz `results/scenario-*/{mqtt|kafka}/summary.csv` nakon pokretanja benchmark-a.

---

### Scenario A — Massive Sensor Ingestion (MQTT)

| Uređaji | QoS | Poslato | Primljeno | Izgubljeno % | Trajanje (s) | mosquitto CPU | mosquitto RAM | mosquitto Net | data-storage CPU | data-storage RAM | data-storage Net | analytics CPU | analytics RAM | analytics Net | postgres CPU | postgres RAM | postgres Net |
|---------|-----|---------|-----------|--------------|--------------|---------------|---------------|---------------|------------------|------------------|------------------|---------------|---------------|---------------|--------------|--------------|--------------|
| 100 | 0 | | | | | | | | | | | | | | | | |
| 100 | 1 | | | | | | | | | | | | | | | | |
| 100 | 2 | | | | | | | | | | | | | | | | |
| 1000 | 0 | | | | | | | | | | | | | | | | |
| 1000 | 1 | | | | | | | | | | | | | | | | |
| 1000 | 2 | | | | | | | | | | | | | | | | |
| 10000 | 0 | | | | | | | | | | | | | | | | |
| 10000 | 1 | | | | | | | | | | | | | | | | |
| 10000 | 2 | | | | | | | | | | | | | | | | |

### Scenario A — Massive Sensor Ingestion (Kafka)

| Uređaji | Acks | Poslato | Primljeno | Izgubljeno % | Trajanje (s) | kafka CPU | kafka RAM | kafka Net | data-storage CPU | data-storage RAM | data-storage Net | analytics CPU | analytics RAM | analytics Net | postgres CPU | postgres RAM | postgres Net |
|---------|------|---------|-----------|--------------|--------------|-----------|-----------|-----------|------------------|------------------|------------------|---------------|---------------|---------------|--------------|--------------|--------------|
| 100 | 0 | | | | | | | | | | | | | | | | |
| 100 | 1 | | | | | | | | | | | | | | | | |
| 100 | all | | | | | | | | | | | | | | | | |
| 1000 | 0 | | | | | | | | | | | | | | | | |
| 1000 | 1 | | | | | | | | | | | | | | | | |
| 1000 | all | | | | | | | | | | | | | | | | |
| 10000 | 0 | | | | | | | | | | | | | | | | |
| 10000 | 1 | | | | | | | | | | | | | | | | |
| 10000 | all | | | | | | | | | | | | | | | | |

### Scenario B — Edge Connectivity Failures (MQTT)

| Outage (s) | Recovery (s) | Poruke u testu | Resubscribe (s) | mosquitto CPU/RAM/Net | data-storage CPU/RAM/Net | analytics CPU/RAM/Net | postgres CPU/RAM/Net | Napomena |
|------------|--------------|----------------|-----------------|-----------------------|--------------------------|-----------------------|----------------------|----------|
| 30 | | | | | | | | |

### Scenario B — Edge Connectivity Failures (Kafka)

| Outage (s) | Recovery (s) | Offset pre | Offset posle | Lag posle | kafka CPU/RAM/Net | data-storage CPU/RAM/Net | analytics CPU/RAM/Net | postgres CPU/RAM/Net | Napomena |
|------------|--------------|------------|--------------|-----------|-------------------|--------------------------|-----------------------|----------------------|----------|
| 30 | | | | | | | | | |

### Scenario C — Burst Event Load (MQTT)

| Baseline (msg/s) | Burst (msg/s) | Peak backlog | Recovery (s) | mosquitto CPU/RAM/Net | data-storage CPU/RAM/Net | analytics CPU/RAM/Net | postgres CPU/RAM/Net |
|------------------|---------------|--------------|--------------|-----------------------|--------------------------|-----------------------|----------------------|
| 50 | 5000 | | | | | | |

### Scenario C — Burst Event Load (Kafka)

| Baseline (msg/s) | Burst (msg/s) | Peak LAG | Recovery (s) | kafka CPU/RAM/Net | data-storage CPU/RAM/Net | analytics CPU/RAM/Net | postgres CPU/RAM/Net |
|------------------|---------------|----------|--------------|-------------------|--------------------------|-----------------------|----------------------|
| 50 | 5000 | | | | | | |

### Scenario D — Real-Time Alerting (MQTT)

| Run | published_at | alert_at | E2E latency (ms) | mosquitto CPU/RAM/Net | analytics CPU/RAM/Net |
|-----|--------------|----------|--------------------|-----------------------|-----------------------|
| 1 | | | | | |
| 2 | | | | | |
| ... | | | | | |
| **Avg/P95** | | | | | |

### Scenario D — Real-Time Alerting (Kafka)

| Run | published_at | alert_at | E2E latency (ms) | kafka CPU/RAM/Net | analytics CPU/RAM/Net |
|-----|--------------|----------|--------------------|-------------------|-----------------------|
| 1 | | | | | |
| 2 | | | | | |
| ... | | | | | |
| **Avg/P95** | | | | | |

---

## Struktura projekta

```
├── docker-compose.yml
├── .env.example
├── BIED_Smart_Agriculture_Dataset.csv
├── benchmarks/
│   ├── common/           # lib.sh, docker_stats.sh, payloads
│   ├── mqtt/             # scenario-a/b/c/d
│   ├── kafka/            # scenario-a/b/c/d
│   └── ps1/              # PowerShell wrapperi
├── infra/
│   ├── mosquitto/mosquitto.conf
│   └── postgres/init.sql
├── results/              # benchmark izlazi (gitignored)
└── services/
    ├── data-ingestion/   # C# .NET 8
    ├── data-storage/     # Node.js
    └── analytics/        # Python FastAPI
```
