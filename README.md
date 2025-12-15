# ADALPHA Backend

Real-time social media trend intelligence backend powered by Confluent Kafka and Google Cloud.

**Built for [AI Partner Catalyst: Accelerate Innovation Hackathon](https://ai-partner-catalyst.devpost.com/) - Confluent Challenge**

## Features

- **Real-time Data Streaming**: SSE (Server-Sent Events) for live updates
- **Kafka Integration**: Confluent Cloud for scalable message streaming
- **VKS Score Calculation**: Viral Kinetic Score from Flink SQL
- **Demo Mode**: 5 scenarios for presentations (viral_spike, steady_growth, etc.)
- **Cloud Ready**: Dockerfile for Google Cloud Run deployment

## Architecture

```
Demo Generator ──► market-stream (Kafka) ──► Flink SQL
                                                  │
                                                  ▼
Browser ◄── SSE Stream (Python) ◄── vks-scores (Kafka)
```

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/YOUR_USERNAME/adalpha-backend.git
cd adalpha-backend
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: source venv/Scripts/activate  # Windows Git Bash
pip install -r requirements.txt
```

### 2. Configure Environment

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your Confluent Cloud credentials:

```env
KAFKA_BOOTSTRAP_SERVERS=your-kafka-server:9092
KAFKA_API_KEY=your-api-key
KAFKA_API_SECRET=your-api-secret
```

### 3. Run Locally

```bash
uvicorn app.main:app --reload --port 8000
```

### 4. Test the API

- **Swagger UI**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

```bash
# Start demo data generator
curl -X POST "http://localhost:8000/api/demo/start" \
  -H "Content-Type: application/json" \
  -d '{"scenario": "viral_spike", "interval": 3.0}'

# Check stream status
curl http://localhost:8000/api/stream/status
```

### 5. Test Dashboard

Open `test-dashboard.html` in your browser to see real-time data visualization.

## API Endpoints

### Health & Status

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API info |
| `/health` | GET | Health check with Kafka status |

### Demo Generator

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/demo/start` | POST | Start demo data generation |
| `/api/demo/stop` | POST | Stop demo |
| `/api/demo/status` | GET | Get demo status |
| `/api/demo/scenarios` | GET | List available scenarios |
| `/api/demo/scenario/{name}` | POST | Switch scenario |

### Real-time Streaming (SSE)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/stream/vks` | GET | SSE stream for VKS scores |
| `/api/stream/trends` | GET | SSE stream for trend data |
| `/api/stream/all` | GET | Combined SSE stream |
| `/api/stream/status` | GET | Stream manager status |

### Kafka Testing

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/test/kafka/status` | GET | Kafka connection status |
| `/api/test/kafka/topics` | GET | List available topics |

## Demo Scenarios

| Scenario | Description |
|----------|-------------|
| `viral_spike` | Hashtag explodes from thousands to 500M views |
| `steady_growth` | Consistent 10-20% growth per interval |
| `declining` | Peak followed by decay |
| `multi_trend` | Multiple hashtags competing |
| `oscillating` | Wave-like engagement pattern |

## Deployment

### Deploy to Google Cloud Run

```bash
gcloud run deploy adalpha-backend \
  --source . \
  --region us-central1 \
  --project YOUR_PROJECT_ID \
  --allow-unauthenticated \
  --memory 512Mi \
  --cpu 1 \
  --set-secrets "KAFKA_API_KEY=kafka-api-key:latest,KAFKA_API_SECRET=kafka-api-secret:latest" \
  --set-env-vars "KAFKA_BOOTSTRAP_SERVERS=pkc-n3603.us-central1.gcp.confluent.cloud:9092"
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | Confluent Cloud bootstrap server |
| `KAFKA_API_KEY` | Yes | Confluent Cloud API key |
| `KAFKA_API_SECRET` | Yes | Confluent Cloud API secret |
| `GEMINI_API_KEY` | No | Google Gemini API key |
| `TIKHUB_API_KEY` | No | TikHub API key |
| `DEBUG` | No | Enable debug mode (default: false) |

## Project Structure

```
adalpha-backend/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI entry point
│   ├── config.py               # Configuration management
│   ├── api/
│   │   ├── __init__.py
│   │   ├── demo.py             # Demo generator endpoints
│   │   ├── stream.py           # SSE streaming endpoints
│   │   └── kafka_test.py       # Kafka test endpoints
│   ├── services/
│   │   ├── __init__.py
│   │   ├── kafka_client.py     # Kafka producer/consumer
│   │   ├── demo_generator.py   # Demo data generator
│   │   └── stream_manager.py   # SSE connection manager
│   └── models/
│       ├── __init__.py
│       └── schemas.py          # Pydantic models
├── test-dashboard.html         # Browser test page
├── requirements.txt
├── Dockerfile
├── .env.example
├── .gitignore
└── README.md
```

## Tech Stack

- **Framework**: FastAPI
- **Streaming**: Confluent Kafka, SSE
- **Validation**: Pydantic v2
- **AI** (optional): Google Gemini
- **Deployment**: Docker, Google Cloud Run

## License

MIT License - see [LICENSE](LICENSE) file.

## Links

- **Hackathon**: [AI Partner Catalyst](https://ai-partner-catalyst.devpost.com/)
- **Frontend**: [TrendPulse AI](https://trendpulse-ai-real-time-social-intelligence-88968107416.us-west1.run.app)
- **Confluent Cloud**: [confluent.cloud](https://confluent.cloud)

---

*Built with Confluent Kafka + Google Cloud for the AI Partner Catalyst Hackathon*
