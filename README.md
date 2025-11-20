# Kafka Playground

This project is a playground for experimenting with Apache Kafka using multiple .NET services.

## Project Structure

- `Producer` -- .NET service for producing messages to Kafka.
- `Consumer` -- .NET console service for consuming messages from Kafka.
- `OrderService` -- .NET web service for consuming messages to Kafka. The revamp of `Consumer` project
- `docker-compose.yml` -- Orchestrates Kafka, Zookeeper, Kafka UI, Producer, and multiple Consumer containers.

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)

### Running the Playground

1. **Clone the repository:**
2. **Adjust configuration:**
	* Create the `.env` file to change the configuration.
3. **Start all services:**
```
docker-compose up --build
```

4. **Access the services:**
   - **Kafka UI:** [http://localhost:8080](http://localhost:8080) � manage Kafka topics, messages, and consumer groups.
   - **pgAdmin:** [http://localhost:5050](http://localhost:5050) � manage PostgreSQL databases.
   - **Producer:** available on port `5000`.
   - **Consumers:** available on ports `5001`, `5002`, and `5003`.

### Stopping the Playground

To stop and remove all containers:
```
docker-compose down
```

To also remove volumes (including PostgreSQL data):
```
docker-compose down -v
```

## License

This project is for educational and demonstration purposes.