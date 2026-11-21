# Member 1: Data Source and Kafka Producer

This part reads the customer churn CSV and sends every customer to the Kafka
topic `churn_raw`, one row at a time, so the file behaves like a live stream.

customer_churn_data.csv -> kafka_producer.py -> Kafka topic churn_raw -> Member 2 (NiFi)

## Files

- `customer_churn_data.csv` is the dataset, 7,043 customers and 23 columns.
- `kafka_producer.py` reads the CSV and publishes each row to Kafka.

## What gets sent

- Topic: `churn_raw`, on the Kafka server at `localhost:9092`.
- One message per customer, written as JSON.
- Each message has the 23 CSV columns plus an `event_time` field that the producer adds.
- Every value is sent as text, and empty cells are sent as `null`.
- Nothing is cleaned here. Cleaning is Member 2's job in NiFi.

## Things to know about the data

- These columns have missing values: `gender` has 396, `StreamingTV` has 427, `StreamingMovies` has 506 and `numAdminTickets` has 592.
- `TotalCharges` is text, and 11 of its values are invalid. They belong to customers with tenure 0.
- `numAdminTickets` comes as values like "0.0" and `numTechTickets` as "0". Both need converting to numbers.
- `Location` is a single field in the form `Region - State`. It has to be split into two columns.
- The data has no churn date, no income and no exact age. Only `SeniorCitizen` is there.
- The real-time stream is simulated. We replay a static file row by row.

## How to run

1. Start Kafka from `C:\kafka` and leave that window open:

   `.\bin\windows\kafka-server-start.bat .\config\server.properties`

2. Open a new window in `C:\kafka` and create the topic:

   `.\bin\windows\kafka-topics.bat --create --topic churn_raw --bootstrap-server localhost:9092`

3. Install the Python package:

   `pip install kafka-python==2.2.15`

4. Go to the folder that has the script and the CSV, and run:

   `python kafka_producer.py`

   When it finishes it prints `Done. 7043 messages published to 'churn_raw'.`

Only run the producer once. Kafka keeps the messages, so a second run adds
every customer again and the data ends up duplicated.

## Problems we ran into on Windows

- If `java` is not recognized, install a JDK, add it to PATH and open a new CMD.
- If `wmic` is not recognized, run `set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G` before starting the server.
- If you get "The input line is too long", move Kafka to `C:\kafka`.
- If `pip` is not recognized, use `py -m pip install ...` and `py kafka_producer.py`.
