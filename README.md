# Kafka Playground

This project is a playground for experimenting with Apache Kafka using multiple .NET services.

## Project Structure

- `Producer` – .NET service for producing messages to Kafka.
- `Consumer` – .NET service for consuming messages from Kafka.
- `docker-compose.yml` – Orchestrates Kafka, Zookeeper, Kafka UI, Producer, and multiple Consumer containers.

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/)

### Running the Playground

1. **Clone the repository:**
2. **Start all services:**
```
docker-compose up --build
```

3. **Access Kafka UI:**
   - Open [http://localhost:8080](http://localhost:8080) in your browser to view and manage Kafka topics, messages, and consumer groups.

4. **Producer and Consumers:**
   - Producer is available on port `5000`.
   - Consumers are available on ports `5001`, `5002`, and `5003`.

### Stopping the Playground

To stop and remove all containers:
```
docker-compose down
```

## License

This project is for educational and demonstration purposes.