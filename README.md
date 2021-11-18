# Event stream processing
## Python + Kafka + Spark + MongoDB + JSON WebService

Application have tree parts:
* EventGenerator.py
  * This part generate fake data, the max volume is 100k, then send them into kafka.
* Spark.py + EventReceiver.py
  * Spark.py read data and aggregate it and sent back to kafka on another topic.
  * EventReceiver.py read computed data from kafka and write it into MongoDB. 
* WebService
  * Provides two services to display data from MongoDB.

## Run

Run commands from the project root directory.
### This run all services except EventGenerator.py:
> docker-compose up -d --build

### Next run EventGenerator.py locally:
1. Activate venv
>.\venv\Scripts\activate
2. Run generator
### Linux
> export PYTHONPATH=pwd && python -u ./app/EventReceiver.py 
### Windows
> SET PYTHONPATH=%cd%

> python .\app\EventGenerator.py

