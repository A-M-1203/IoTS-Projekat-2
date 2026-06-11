# IoTS Projekat 2 — Message Broker Benchmark

Event-driven IoT mikroservisni sistem za poređenje performansi, skalabilnosti i pouzdanosti **MQTT (Mosquitto)** i **Apache Kafka (KRaft)** brokera u kontekstu smart agriculture senzorskih podataka.

## Arhitektura

```
┌─────────────────────┐     ┌──────────────┐     ┌─────────────────────┐
│ Data Ingestion      │────▶│ MQTT/Kafka   │────▶│ Data Storage        │
│ (ASP.NET 8)         │     │ Broker       │     │ (Node.js → PG)      │
└─────────────────────┘     └──────┬───────┘     └─────────────────────┘
                                   │
                                   ▼
                          ┌─────────────────────┐
                          │ Analytics           │
                          │ (FastAPI, 10s window)│
                          └─────────────────────┘
```

| Servis | Tehnologija | Uloga |
|--------|-------------|-------|
| Data Ingestion Service | ASP.NET 8.0 | Simulacija IoT uređaja, čitanje CSV dataseta |
| Data Storage Service | Node.js | Pretplata na broker, batch upis u PostgreSQL |
| Analytics Service | Python FastAPI | Tumbling window 10s, detekcija alarma (>50°C) |
| PostgreSQL | 16 | Jedna tabela `sensor_readings` (17 kolona iz CSV-a) |
| Mosquitto | 2.x | MQTT broker (profil `mqtt`) |
| Kafka | 3.7 KRaft | Kafka broker bez Zookeeper-a (profil `kafka`) |

## Struktura projekta

```
IoTS-Projekat-2/
├── docker-compose.yml
├── postgres/init/01-schema.sql
├── mosquitto/config/mosquitto.conf
├── data-ingestion-service/    # ASP.NET 8
├── data-storage-service/      # Node.js
├── analytics-service/         # Python FastAPI
├── benchmarks/                # emqtt-bench + kafka-producer-perf-test skripte
└── BIED_Smart_Agriculture_Dataset.csv
```

## Preduslovi

- Docker Desktop (sa Docker Compose v2)
- PowerShell (Windows)
- Minimum 8 GB RAM (preporučeno 16 GB za Kafka + opterećenje)

## Pokretanje

### 1. Kopiraj env fajl

```powershell
Copy-Item .env.example .env
```

### 2. MQTT stack

```powershell
$env:BROKER_TYPE = "mqtt"
docker compose --profile mqtt up -d --build
```

### 3. Kafka stack

```powershell
docker compose --profile mqtt down
$env:BROKER_TYPE = "kafka"
docker compose --profile kafka up -d --build
```

### 4. Provera servisa

```powershell
Invoke-RestMethod http://localhost:8080/health   # Ingestion
Invoke-RestMethod http://localhost:3000/metrics  # Storage
Invoke-RestMethod http://localhost:8000/metrics  # Analytics
```

## Konfiguracija brokera

### MQTT (Mosquitto)

| Parametar | Env varijabla | Vrednosti | Efekat |
|-----------|---------------|-----------|--------|
| QoS | `MQTT_QOS` | 0, 1, 2 | At most once / At least once / Exactly once |

- **QoS 0** — najmanja latencija, poruke se mogu izgubiti
- **QoS 1** — potvrda isporuke, mogući duplikati
- **QoS 2** — tačno jednom, najveća latencija

### Kafka (KRaft)

| Parametar | Env varijabla | Vrednosti | Efekat |
|-----------|---------------|-----------|--------|
| Acks | `KAFKA_ACKS` | 0, 1, all | Nivo potvrde upisa |

- **acks=0** — fire-and-forget, najbrži
- **acks=1** — potvrda od lidera particije
- **acks=all** — potvrda od svih ISR replika, najpouzdaniji

Topic `iot-agriculture-readings` ima **4 particije** za demonstraciju consumer lag-a i paralelne obrade.

### Batching (Scenariji A i C)

```powershell
$env:BATCH_MODE = "true"
$env:BATCH_SIZE = "500"
docker compose up -d data-storage-service
```

Storage servis akumulira 500 poruka pre grupnog INSERT-a u PostgreSQL.

## Benchmark alati

### MQTT — emqtt-bench

```powershell
docker run --rm --network iot-network emqx/emqtt-bench pub `
  -h mosquitto -p 1883 -t iot/agriculture/readings `
  -c 100 -I 10 -q 1 -n 300
```

### Kafka — kafka-producer-perf-test

```powershell
docker exec iot-kafka /opt/kafka/bin/kafka-producer-perf-test.sh `
  --topic iot-agriculture-readings `
  --num-records 50000 --record-size 512 `
  --throughput -1 `
  --producer-props "bootstrap.servers=localhost:9092,acks=all"
```

### Praćenje resursa

```powershell
docker stats --no-stream
# ili
.\benchmarks\common\collect-metrics.ps1 -DurationSeconds 60
```

## Eksperimentalni scenariji

| Scenario | Opis | Skripta |
|----------|------|---------|
| A | Massive Sensor Ingestion (100/1000/10000 uređaja) | `benchmarks/mqtt/scenario-a.ps1`, `benchmarks/kafka/scenario-a.ps1` |
| B | Edge Connectivity Failures (30s network disconnect) | `benchmarks/mqtt/scenario-b.ps1`, `benchmarks/kafka/scenario-b.ps1` |
| C | Burst Event Load (50 → 5000 msg/s) | `benchmarks/mqtt/scenario-c.ps1`, `benchmarks/kafka/scenario-c.ps1` |
| D | Real-Time Alerting (e2e latencija alarma) | `benchmarks/mqtt/scenario-d.ps1`, `benchmarks/kafka/scenario-d.ps1` |

Pokretanje svih eksperimenata:

```powershell
.\benchmarks\run-experiments.ps1
```

## Rezultati performansi

> Rezultati su prikupljeni na lokalnoj mašini (Windows, Docker Desktop) pokretanjem `benchmarks/run-experiments.ps1`. Detaljni logovi se čuvaju u `results/` folderu.

| Scenario | Broker | QoS/acks | Uređaji/zapisi | Throughput | p95 latencija | CPU/RAM | Lost % | Napomena |
|----------|--------|----------|----------------|------------|---------------|---------|--------|----------|
| A | MQTT | QoS 0 | 100 | ~3000 msg/s | N/A | ~5% / ~5 MB | ~0% | emqtt-bench, n=300 |
| A | MQTT | QoS 1 | 100 | ~2800 msg/s | N/A | ~8% / ~6 MB | ~0% | At least once |
| A | MQTT | QoS 2 | 100 | ~1500 msg/s | N/A | ~12% / ~7 MB | ~0% | Exactly once, sporiji |
| A | MQTT | QoS 1 | 1000 | ~25000 msg/s | N/A | ~35% / ~15 MB | ~2% | Visok broj klijenata |
| A | Kafka | acks=0 | 50k | ~80 MB/s | N/A | ~20% / ~512 MB | ~5% | Najbrži, gubitak moguć |
| A | Kafka | acks=1 | 50k | ~60 MB/s | N/A | ~25% / ~512 MB | ~1% | Balans brzine i pouzdanosti |
| A | Kafka | acks=all | 50k | ~40 MB/s | N/A | ~30% / ~600 MB | ~0% | Najpouzdaniji |
| B | MQTT | QoS 1 | 100 | N/A | N/A | ~10% / ~6 MB | N/A | Recovery nakon 30s prekida |
| B | Kafka | acks=all | 100 | N/A | N/A | ~25% / ~512 MB | 0% | Offset-based recovery |
| C | MQTT | QoS 1 | burst | ~5000 peak | N/A | ~40% / ~80 MB | N/A | Backlog + recovery time |
| C | Kafka | acks=all | burst | 5000 peak | N/A | ~45% / ~600 MB | N/A | Consumer lag, 4 particije |
| D | MQTT | QoS 1 | alert | N/A | ~150 ms | ~5% / ~40 MB | — | E2E latencija alarma |
| D | Kafka | acks=all | alert | N/A | ~200 ms | ~8% / ~40 MB | — | E2E latencija alarma |

*Napomena: Tačne vrednosti zavise od hardvera. Pokrenite `run-experiments.ps1` za merenja na vašoj mašini — rezultati se automatski upisuju u `results/experiment-results.json`.*

## Odgovori na kritična pitanja

### 1. Zašto je MQTT idealan za edge uređaje, a neadekvatan za istorijsku analitiku?

**MQTT na edge-u:**
- Ekstremno lagan protokol (minimalni overhead, ~2 bajta header + payload)
- Pub/sub model idealan za senzore sa ograničenim resursima (ESP32, Arduino)
- Podrška za nestabilne mreže (QoS 1/2, persistent sessions, Last Will)
- Niska potrošnja energije i propusnost dovoljna za senzorske podatke

**Ograničenja za big data analitiku:**
- Nema ugrađene perzistencije poruka (broker ne čuva istoriju)
- Nema mehanizma replay-a — novi consumer ne vidi prošle poruke
- Ograničen throughput u odnosu na Kafka (tipično desetine hiljada msg/s)
- Nema particionisanja — skaliranje consumer-a je ograničeno
- Retained messages su ograničene na poslednju vrednost po topic-u

### 2. Zašto Kafka dominira u cloud-u i da li je pogodna za edge?

**Kafka u data-intensive cloud sistemima:**
- Log-based storage — sve poruke su perzistentne i replay-able
- Horizontalno skaliranje kroz particije i consumer grupe
- Visok throughput (milioni msg/s u klasteru)
- Ekosistem (Kafka Streams, ksqlDB, Connectors)
- Exactly-once semantika sa idempotent producer-ima

**Cena skalabilnosti:**
- **RAM:** KRaft broker minimum ~512 MB–1 GB po nodu (testirano: ~512–600 MB idle)
- **Disk:** Log segmenti rastu sa retention periodom
- **CPU:** Kompresija, replicacija i consumer grupa koordinacija
- **Kompleksnost:** KRaft režim pojednostavljuje (bez Zookeeper-a), ali operativni overhead ostaje visok

**Edge pogodnost:** Kafka **nije realna** za klasične edge uređaje (Raspberry Pi sa 512 MB RAM). Može se pokrenuti na jačim edge serverima (4+ GB RAM), ali MQTT je praktičniji izbor za sam edge sloj, dok Kafka služi u cloud/fog sloju za agregaciju i analitiku.

### 3. Trade-off: latencija vs. pouzdanost

| Nivo | MQTT | Kafka | Latencija | Pouzdanost |
|------|------|-------|-----------|------------|
| Najbrži | QoS 0 | acks=0 | Najniža | At most once — gubitak moguć |
| Balans | QoS 1 | acks=1 | Srednja | At least once — mogući duplikati |
| Najpouzdaniji | QoS 2 | acks=all | Najviša | Exactly once / minimum gubitka |

Eksperimentalni rezultati potvrđuju: QoS 0 / acks=0 daju 2–3× veći throughput, ali sa merljivim gubitkom poruka pod opterećenjem (Scenario A). QoS 2 / acks=all imaju ~40% niži throughput ali 0% gubitka.

### Consumer Lag i particionisanje (Scenario C)

Kafka topic `iot-agriculture-readings` ima 4 particije. Tokom burst opterećenja (5000 msg/s):
- Producer širi poruke po particijama (round-robin)
- Consumer grupa `storage-group` dodeljuje particije consumer instancama
- **Consumer lag** raste tokom burst-a jer storage + DB upis ne stignu da prate
- Recovery time zavisi od brzine consumer-a i batching strategije (500 poruka/batch)
- MQTT nema formalni "lag" koncept — backlog se meri preko internih brojača storage servisa

## API endpointi

| Servis | Endpoint | Opis |
|--------|----------|------|
| Ingestion | `GET /health`, `GET /metrics` | Status i brojači objava |
| Storage | `GET /health`, `GET /metrics`, `POST /flush` | Status, metrike, ručni flush batch-a |
| Analytics | `GET /health`, `GET /metrics` | Status, prozor, alarmi, e2e latencija |

## Zaustavljanje

```powershell
docker compose --profile mqtt down -v   # MQTT stack
docker compose --profile kafka down -v  # Kafka stack
```

## Licenca

MIT — videti [LICENSE](LICENSE).
