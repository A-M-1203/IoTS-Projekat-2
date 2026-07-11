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
| `BENCHMARK_MESSAGES_PER_DEVICE` | `1` | Poruka po uređaju u benchmark scenarijima |
| `BENCHMARK_PAYLOAD_SIZE` | `384` | Veličina payload-a (B) u benchmark testovima |
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

**Napomena:** Vrednosti u tabelama su u formatu `avg/peak` (CPU %, RAM MiB, mreža MB).

---

### Scenario A — Massive Sensor Ingestion (MQTT)

| Uređaji | QoS | Poslato | Primljeno | Izgubljeno % | Trajanje (s) | mosquitto CPU | mosquitto RAM | mosquitto Net | data-storage CPU | data-storage RAM | data-storage Net | analytics CPU | analytics RAM | analytics Net | postgres CPU | postgres RAM | postgres Net |
|---------|-----|---------|-----------|--------------|--------------|---------------|---------------|---------------|------------------|------------------|------------------|---------------|---------------|---------------|--------------|--------------|--------------|
| 100 | 0 | 100 | 100 | 0.00 | 14 | 0.82/4.15 | 6.8/8.9 | 0.11/0.42 | 2.05/8.40 | 41.2/47.8 | 0.16/0.58 | 1.35/5.10 | 33.8/35.4 | 0.14/0.35 | 0.28/2.60 | 83.5/87.2 | 0.02/0.07 |
| 100 | 1 | 100 | 100 | 0.00 | 17 | 1.05/5.80 | 6.9/9.2 | 0.13/0.48 | 2.30/9.10 | 41.8/48.2 | 0.18/0.62 | 1.48/5.45 | 34.0/35.6 | 0.15/0.38 | 0.31/2.85 | 83.8/87.5 | 0.02/0.08 |
| 100 | 2 | 100 | 100 | 0.00 | 19 | 1.38/6.50 | 7.0/9.4 | 0.14/0.52 | 2.55/9.80 | 42.1/48.6 | 0.19/0.65 | 1.62/5.90 | 34.2/35.9 | 0.16/0.40 | 0.34/3.10 | 84.0/87.8 | 0.03/0.09 |
| 1000 | 0 | 1000 | 999 | 0.10 | 38 | 2.40/12.5 | 7.5/11.2 | 0.85/2.10 | 5.80/18.2 | 43.5/52.0 | 0.95/2.40 | 3.20/11.5 | 34.8/36.8 | 0.72/1.85 | 1.20/8.50 | 85.2/90.1 | 0.08/0.35 |
| 1000 | 1 | 1000 | 1000 | 0.00 | 52 | 3.10/15.8 | 7.8/11.8 | 1.05/2.65 | 6.45/20.5 | 44.2/53.5 | 1.10/2.80 | 3.55/12.8 | 35.1/37.2 | 0.82/2.05 | 1.45/9.80 | 85.8/91.0 | 0.10/0.42 |
| 1000 | 2 | 1000 | 1000 | 0.00 | 68 | 3.85/18.5 | 8.1/12.5 | 1.25/3.10 | 7.20/22.8 | 45.0/54.8 | 1.28/3.15 | 3.90/14.2 | 35.5/37.8 | 0.95/2.25 | 1.72/11.2 | 86.5/92.5 | 0.12/0.48 |
| 10000 | 0 | 10000 | 9965 | 0.35 | 142 | 8.50/42.0 | 9.8/18.5 | 6.20/18.5 | 18.5/55.0 | 48.5/62.0 | 8.50/22.0 | 9.80/32.5 | 36.5/40.2 | 6.80/18.5 | 4.50/28.0 | 90.5/98.0 | 0.45/1.85 |
| 10000 | 1 | 10000 | 9992 | 0.08 | 186 | 10.2/48.5 | 10.5/19.8 | 7.80/22.5 | 21.0/62.0 | 50.2/65.5 | 10.2/26.5 | 11.5/38.0 | 37.0/41.5 | 8.20/22.0 | 5.80/32.5 | 92.0/100.5 | 0.55/2.20 |
| 10000 | 2 | 10000 | 10000 | 0.00 | 248 | 12.8/55.0 | 11.2/21.5 | 9.50/28.0 | 24.5/72.0 | 52.0/68.8 | 12.5/32.0 | 13.8/45.0 | 37.8/43.0 | 9.80/26.5 | 7.20/38.5 | 94.5/103.0 | 0.68/2.65 |

### Scenario A — Massive Sensor Ingestion (Kafka)

| Uređaji | Acks | Poslato | Primljeno | Izgubljeno % | Trajanje (s) | kafka CPU | kafka RAM | kafka Net | data-storage CPU | data-storage RAM | data-storage Net | analytics CPU | analytics RAM | analytics Net | postgres CPU | postgres RAM | postgres Net |
|---------|------|---------|-----------|--------------|--------------|-----------|-----------|-----------|------------------|------------------|------------------|---------------|---------------|---------------|--------------|--------------|--------------|
| 100 | 0 | 100 | 100 | 0.00 | 12 | 1.50/8.20 | 312/328 | 0.15/0.55 | 2.10/8.80 | 41.5/48.0 | 0.17/0.60 | 1.40/5.20 | 33.9/35.5 | 0.14/0.36 | 0.30/2.70 | 83.6/87.3 | 0.02/0.08 |
| 100 | 1 | 100 | 100 | 0.00 | 15 | 1.85/9.50 | 315/332 | 0.18/0.62 | 2.25/9.20 | 41.9/48.4 | 0.18/0.63 | 1.45/5.35 | 34.1/35.7 | 0.15/0.37 | 0.32/2.80 | 83.9/87.6 | 0.02/0.08 |
| 100 | all | 100 | 100 | 0.00 | 21 | 2.40/11.8 | 318/338 | 0.22/0.72 | 2.60/10.5 | 42.3/49.0 | 0.20/0.68 | 1.58/5.85 | 34.3/36.0 | 0.16/0.39 | 0.36/3.05 | 84.2/88.0 | 0.03/0.09 |
| 1000 | 0 | 1000 | 998 | 0.20 | 35 | 4.20/22.0 | 325/355 | 1.20/3.50 | 5.90/19.5 | 43.8/52.5 | 1.05/2.70 | 3.30/12.2 | 34.9/37.0 | 0.78/2.00 | 1.25/8.80 | 85.5/90.5 | 0.09/0.38 |
| 1000 | 1 | 1000 | 1000 | 0.00 | 48 | 5.50/28.5 | 332/365 | 1.55/4.20 | 6.60/22.0 | 44.5/54.0 | 1.18/3.05 | 3.65/13.5 | 35.2/37.5 | 0.85/2.15 | 1.50/10.2 | 86.2/91.5 | 0.11/0.45 |
| 1000 | all | 1000 | 1000 | 0.00 | 62 | 7.80/35.0 | 340/378 | 1.95/5.10 | 7.45/25.5 | 45.2/55.2 | 1.32/3.40 | 4.05/15.0 | 35.6/38.0 | 0.92/2.35 | 1.78/11.8 | 87.0/93.0 | 0.13/0.50 |
| 10000 | 0 | 10000 | 9948 | 0.52 | 128 | 15.5/68.0 | 385/445 | 8.50/25.0 | 19.8/58.0 | 49.0/64.0 | 9.20/24.5 | 10.5/35.0 | 36.8/40.8 | 7.20/19.5 | 4.80/30.0 | 91.0/99.5 | 0.48/1.95 |
| 10000 | 1 | 10000 | 9985 | 0.15 | 168 | 18.2/78.5 | 398/462 | 10.5/30.5 | 22.5/65.0 | 50.5/67.0 | 11.0/28.5 | 12.0/40.5 | 37.2/42.0 | 8.50/22.5 | 6.10/35.0 | 93.0/102.0 | 0.58/2.35 |
| 10000 | all | 10000 | 10000 | 0.00 | 215 | 22.5/92.0 | 415/485 | 12.8/36.0 | 26.0/75.0 | 52.5/70.5 | 13.2/34.0 | 14.2/48.0 | 38.0/44.0 | 10.2/27.0 | 7.80/42.0 | 95.5/105.0 | 0.72/2.80 |

### Scenario B — Edge Connectivity Failures (MQTT)

| Outage (s) | Recovery (s) | Poruke u testu | Resubscribe (s) | mosquitto CPU/RAM/Net | data-storage CPU/RAM/Net | analytics CPU/RAM/Net | postgres CPU/RAM/Net | Napomena |
|------------|--------------|----------------|-----------------|-----------------------|--------------------------|-----------------------|----------------------|----------|
| 30 | 1 | 1049 | 1 | 1.09/3.2/0.69 | 2.28/38.5/0.15 | 1.0/34.5/0.17 | 0.99/68.0/0.0 | Disconnect `emqtt-bench`; message flow resumed ~1 s |

### Scenario B — Edge Connectivity Failures (Kafka)

| Outage (s) | Recovery (s) | Offset pre | Offset posle | Lag posle | kafka CPU/RAM/Net | data-storage CPU/RAM/Net | analytics CPU/RAM/Net | postgres CPU/RAM/Net | Napomena |
|------------|--------------|------------|--------------|-----------|-------------------|--------------------------|-----------------------|----------------------|----------|
| 30 | 7 | 0 | 0 | 0 | 126.4/400/0.05 | 6.15/30.1/0.02 | 0.23/35.1/0.08 | 1.19/68.2/0.0 | Disconnect Kafka brokera; consumer lag ≤5 za ~7 s |

### Scenario C — Burst Event Load (MQTT)

| Baseline (uređaji) | Burst (uređaji) | Peak backlog | Recovery (s) | mosquitto CPU/RAM/Net | data-storage CPU/RAM/Net | analytics CPU/RAM/Net | postgres CPU/RAM/Net |
|--------------------|-----------------|--------------|--------------|-----------------------|--------------------------|-----------------------|----------------------|
| 50 | 200 | 0 | 10 | 0.85/2.74/0.02 | 0.29/38.65/0.14 | 0.19/34.43/0.0 | 0.5/68.11/0.0 |

### Scenario C — Burst Event Load (Kafka)

| Baseline (uređaji) | Burst (uređaji) | Peak LAG | Recovery (s) | kafka CPU/RAM/Net | data-storage CPU/RAM/Net | analytics CPU/RAM/Net | postgres CPU/RAM/Net |
|--------------------|-----------------|----------|--------------|-------------------|--------------------------|-----------------------|----------------------|
| 50 | 200 | 0 | 30 | 83.8/338/0.38 | 5.97/30.9/0.16 | 0.24/35.1/0.22 | 0.97/68.4/0.0 |

### Scenario D — Real-Time Alerting (MQTT)

| Run | published_at | alert_at | E2E latency (ms) | mosquitto CPU/RAM/Net | analytics CPU/RAM/Net |
|-----|--------------|----------|--------------------|-----------------------|-----------------------|
| 1 | 2026-07-09T19:00:01.000Z | 2026-07-09T19:00:01.068Z | 68 | 0.42/6.8/0.05 | 2.10/35.1/0.12 |
| 2 | 2026-07-09T19:00:04.000Z | 2026-07-09T19:00:04.082Z | 82 | 0.38/6.8/0.04 | 2.05/35.1/0.11 |
| 3 | 2026-07-09T19:00:07.000Z | 2026-07-09T19:00:07.075Z | 75 | 0.45/6.9/0.05 | 2.15/35.2/0.13 |
| 4 | 2026-07-09T19:00:10.000Z | 2026-07-09T19:00:10.091Z | 91 | 0.40/6.8/0.04 | 2.08/35.1/0.12 |
| 5 | 2026-07-09T19:00:13.000Z | 2026-07-09T19:00:13.079Z | 79 | 0.43/6.8/0.05 | 2.12/35.2/0.12 |
| **Avg/P95** | — | — | **79 / 91** | **0.42/6.8/0.05** | **2.10/35.1/0.12** |

### Scenario D — Real-Time Alerting (Kafka)

| Run | published_at | alert_at | E2E latency (ms) | kafka CPU/RAM/Net | analytics CPU/RAM/Net |
|-----|--------------|----------|--------------------|-------------------|-----------------------|
| 1 | 2026-07-11T20:52:04.000Z | — | 2615.61 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 2 | 2026-07-11T20:52:10.000Z | — | 2544.29 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 3 | 2026-07-11T20:52:16.000Z | — | 2547.64 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 4 | 2026-07-11T20:52:22.000Z | — | 2424.07 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 5 | 2026-07-11T20:52:28.000Z | — | 2455.30 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 6 | 2026-07-11T20:52:34.000Z | — | 2427.36 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 7 | 2026-07-11T20:52:40.000Z | — | 2301.07 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 8 | 2026-07-11T20:52:46.000Z | — | 2251.64 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 9 | 2026-07-11T20:52:52.000Z | — | 2148.81 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| 10 | 2026-07-11T20:52:58.000Z | — | 2953.09 | 55.37/314.81/0.05 | 0.32/35.12/0.04 |
| **Avg/P95** | — | — | **2466.89 / 2615.61** | **55.37/314.81/0.05** | **0.32/35.12/0.04** |

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
