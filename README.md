# IoTS Projekat 2 — Message Broker Benchmark

Asinhroni event-driven mikroservisni sistem za IoT podatke iz smart agriculture dataseta.

## Arhitektura

- **Data Ingestion** (C# .NET 8) — čita `BIED_Smart_Agriculture_Dataset.csv` u batch-evima od 20 zapisa na svakih 5 sekundi i odmah šalje na broker
- **Data Storage** (Node.js) — pretplaćen na broker, upisuje poruke u PostgreSQL
- **Analytics** (Python FastAPI) — stream processing sa 10s tumbling window i detekcijom praga temperature
- **PostgreSQL** — efemerna baza (podaci se brišu pri `docker compose down`)
- **Broker** — MQTT (Mosquitto) ili Kafka (izbor preko env varijable i Docker profila)

## Preduslovi

- Docker i Docker Compose

## Brzo pokretanje (MQTT)

```bash
cp .env.example .env
docker compose --profile mqtt up --build
```

## Pokretanje sa Kafka brokerom

```bash
cp .env.example .env
# U .env postavi: BROKER_TYPE=kafka
docker compose --profile kafka up --build
```

Ili jednolinijski:

```bash
BROKER_TYPE=kafka docker compose --profile kafka up --build
```

## Konfiguracija

Kopiraj `.env.example` u `.env` i prilagodi po potrebi:

| Varijabla | Podrazumevano | Opis |
|-----------|---------------|------|
| `BROKER_TYPE` | `mqtt` | `mqtt` ili `kafka` |
| `MQTT_TOPIC` | `iot/agriculture/sensors` | MQTT topic |
| `KAFKA_TOPIC` | `iot-agriculture-sensors` | Kafka topic |
| `BATCH_SIZE` | `20` | Broj zapisa po batch-u |
| `BATCH_INTERVAL_SECONDS` | `5` | Pauza između batch-eva (sekunde) |
| `TEMP_ALERT_THRESHOLD` | `50` | Prag temperature za alarm (°C) |
| `WINDOW_SIZE_SECONDS` | `10` | Veličina tumbling prozora |

Napomena: dataset ima temperature uglavnom 15–25°C. Za test alarma postavi npr. `TEMP_ALERT_THRESHOLD=22`.

## Provera rada

```bash
# Logovi ingestion servisa
docker compose logs -f data-ingestion

# Logovi storage servisa
docker compose logs -f data-storage

# Logovi analytics servisa (window stats i alarmi)
docker compose logs -f analytics

# Broj upisanih redova u bazi
docker compose exec postgres psql -U iot -d iot_agriculture -c "SELECT COUNT(*) FROM sensor_readings;"

# Health check analytics servisa
curl http://localhost:8000/health

# Poslednja statistika prozora
curl http://localhost:8000/stats
```

## Zaustavljanje

```bash
docker compose --profile mqtt down
```

PostgreSQL koristi `tmpfs` — svi podaci u bazi se brišu kada se kontejneri zaustave.

## Struktura projekta

```
├── docker-compose.yml
├── .env.example
├── BIED_Smart_Agriculture_Dataset.csv
├── infra/
│   ├── mosquitto/mosquitto.conf
│   └── postgres/init.sql
└── services/
    ├── data-ingestion/    # C# .NET 8
    ├── data-storage/      # Node.js
    └── analytics/         # Python FastAPI
```
