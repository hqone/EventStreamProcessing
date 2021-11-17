# Event stream processing
## Python + Kafka + Spark + MongoDB + JSON WebService

Application have tree parts:
* EventGenerator.py
  * This part generate fake data, the max volume is 100k, then send them into kafka.
* Spark.py + EventReceiver.py
  * Spark.py read data and aggregate it and sent bank to kafka on another topic.
  * EventReceiver.py read computed data from kafka and write it into MongoDB. 
* WebService
  * Share two services to view data from MongoDB.

## Run

This run all services except EventGenerator.py:
> docker-compose up -d --build

Next must run EventGenerator.py locally:
1. Activate venv
>.\venv\Scripts\activate
2. Run generator
### Linux
> export PYTHONPATH="/usr/src/app/" && python -u ./app/EventReceiver.py 
### Windows
> SET PYTHONPATH=%cd%

> python .\app\EventGenerator.py

