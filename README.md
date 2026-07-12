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
| 100 | 0 | 100 | 100 | 0.00 | 8 | 0.05/0.05 | 7.27/7.28 | 0.09/0.09 | 2.47/3.48 | 47.41/48.16 | 0.08/0.08 | 0.17/0.18 | 35.22/35.22 | 0.05/0.05 | 2.00/3.96 | 85.75/86.49 | 0.02/0.03 |
| 100 | 1 | 100 | 100 | 0.00 | 8 | 0.06/0.06 | 7.22/7.23 | 0.10/0.10 | 2.44/2.93 | 47.30/48.11 | 0.09/0.09 | 0.17/0.22 | 35.21/35.21 | 0.05/0.05 | 1.84/3.63 | 85.96/86.84 | 0.02/0.03 |
| 100 | 2 | 100 | 100 | 0.00 | 8 | 0.04/0.05 | 7.40/7.41 | 0.10/0.10 | 5.93/10.26 | 43.75/48.27 | 0.09/0.09 | 0.18/0.21 | 35.21/35.21 | 0.05/0.05 | 1.61/3.18 | 85.66/86.38 | 0.02/0.03 |
| 1000 | 0 | 1000 | 1000 | 0.00 | 8 | 0.05/0.06 | 7.51/7.52 | 0.92/0.92 | 1.04/1.69 | 55.59/56.54 | 0.76/0.76 | 0.16/0.18 | 35.31/35.31 | 0.48/0.48 | 3.81/4.64 | 89.45/90.59 | 0.31/0.31 |
| 1000 | 1 | 1000 | 1000 | 0.00 | 9 | 0.06/0.06 | 2.97/2.97 | 0.94/0.94 | 6.56/12.73 | 49.05/60.73 | 0.78/0.78 | 4.92/9.65 | 34.55/34.55 | 0.49/0.49 | 4.55/6.33 | 70.95/71.16 | 0.31/0.31 |
| 1000 | 2 | 1000 | 1000 | 0.00 | 8 | 0.04/0.05 | 2.88/2.88 | 0.95/0.95 | 1.09/1.85 | 65.80/66.76 | 0.81/0.81 | 0.18/0.23 | 34.54/34.54 | 0.47/0.47 | 1.48/2.91 | 70.80/70.80 | 0.31/0.31 |
| 10000 | 0 | 10000 | 10000 | 0.00 | 13 | 36.85/110.43 | 5.68/8.53 | 7.04/9.36 | 4.16/8.34 | 71.71/73.65 | 5.87/7.82 | 1.20/3.14 | 34.69/34.73 | 3.56/4.72 | 4.63/9.44 | 73.81/74.65 | 2.32/3.17 |
| 10000 | 1 | 10000 | 10000 | 0.00 | 11 | 295.29/885.75 | 22.96/60.33 | 8.18/9.53 | 20.49/58.99 | 74.54/77.32 | 6.83/8.01 | 13.21/39.31 | 34.78/34.82 | 4.05/4.72 | 4.54/8.87 | 73.26/74.50 | 2.32/3.17 |
| 10000 | 2 | 10000 | 10000 | 0.00 | 11 | 444.62/889.18 | 24.85/45.38 | 7.99/10.22 | 31.55/61.34 | 79.05/79.33 | 6.71/8.62 | 18.58/36.95 | 34.78/34.84 | 3.74/4.77 | 4.64/9.21 | 72.60/74.24 | 1.89/3.17 |

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
| 1 | 2026-07-11T22:11:57.000Z | 2026-07-11T22:11:58.971Z | 1970.49 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 2 | 2026-07-11T22:12:01.000Z | 2026-07-11T22:12:02.709Z | 1709.32 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 3 | 2026-07-11T22:12:05.000Z | 2026-07-11T22:12:06.595Z | 1594.60 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 4 | 2026-07-11T22:12:08.000Z | 2026-07-11T22:12:10.055Z | 2055.20 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 5 | 2026-07-11T22:12:12.000Z | 2026-07-11T22:12:13.536Z | 1536.33 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 6 | 2026-07-11T22:12:15.000Z | 2026-07-11T22:12:17.041Z | 2040.85 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 7 | 2026-07-11T22:12:19.000Z | 2026-07-11T22:12:20.361Z | 1360.95 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 8 | 2026-07-11T22:12:22.000Z | 2026-07-11T22:12:23.881Z | 1880.63 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 9 | 2026-07-11T22:12:26.000Z | 2026-07-11T22:12:27.255Z | 1254.68 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| 10 | 2026-07-11T22:12:29.000Z | 2026-07-11T22:12:30.629Z | 1628.93 | 1.89/3.01/0.02 | 0.22/34.54/0.03 |
| **Avg/P95** | — | — | **1703.20 / 2040.85** | **1.89/3.01/0.02** | **0.22/34.54/0.03** |

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
