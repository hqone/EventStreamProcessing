from app.Resources import Resources
from app.Utils import log_performance, get_app_logger

logger = get_app_logger(__name__)


class EventReceiver:
    """
    EventReceiver specializes in consuming messages from one kafka topic and batch save them into storage.
    """

    def __init__(self, resources: Resources):
        """
        Init EventReceiver resources.
        """
        self.__resources = resources

    @log_performance
    def run(self):
        """
        Eternal loop for consume computed payloads from Kafka, then save them in storage.
        """
        while True:
            try:
                payloads = self.__resources.get_kafka_consumer()  # type: dict

                for payload in payloads:
                    logger.debug('Received message: {}'.format(payload.value))

                    # Upsert computed stats from PySpark
                    self.__resources.get_storage().computed_data.update_one(
                        {'eventTime': payload.value['eventTime'],
                         'miasto zamieszkania': payload.value['miasto zamieszkania']},
                        {'$set': payload.value},
                        upsert=True
                    )
            except Exception as e:
                logger.error(e)


if __name__ == '__main__':
    logger.info('START EventReceiver')
    EventReceiver(
        Resources()
    ).run()
    logger.info('END EventReceiver')
