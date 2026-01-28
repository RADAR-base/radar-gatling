# radar-gatling
Gatling tests for RADAR-Kubernetes

This project contains Gatling load tests for RADAR-Kubernetes components, particularly focusing on data ingestion.

## Prerequisites
- Java 21 or later
- Gradle (optional, using the provided `gradlew` is recommended)

## Configuration
The tests are configured using Typesafe Config. The main configuration file is located at:
`src/gatling/resources/test.conf`

Key configuration parameters:
- `baseUrl`: The base URL of the RADAR-Kubernetes installation (e.g., `http://localhost`).
- `clientId` & `clientSecret`: Management Portal client credentials.
- `mpAdminUsername` & `mpAdminPassword`: Management Portal administrator credentials.
- `projectName`: The name of the project to use for testing.
- `numberOfParticipants`: Number of subjects to register and use for data ingestion.
- `messageNumber`: Number of messages to send per request.
- `messageSize`: Number of answers in each questionnaire message.

Additional resource files:
- `src/gatling/resources/users.csv`: Contains user data for simulations.
- `src/gatling/resources/topics.csv`: Contains Kafka topics used in simulations.

## Running the tests

You can run the Gatling tests using the Gradle wrapper:

```bash
./gradlew gatlingRun
```

To run a specific simulation, use:

```bash
./gradlew gatlingRun-org.radarbase.TestDataIngestion
```

## Available Simulations

- `org.radarbase.TestDataIngestion`: A comprehensive simulation that sets up an organization, project, source types, and subjects, and then performs load testing by ingesting questionnaire data.
- `computerdatabase.ComputerDatabaseSimulation`: A sample simulation provided by Gatling.

## Reports
After the tests are completed, Gatling generates HTML reports. You can find them in:
`build/reports/gatling/`
