import argparse
from typing import Generator
from time import sleep
from random import randint, choice

from kafka.producer.future import FutureRecordMetadata

from app.Resources import Resources
from app.Utils import get_app_logger, log_performance
from faker import Faker

logger = get_app_logger(__name__)


class EventGenerator:
    MAX_NB_OF_EVENTS = 100000

    def __init__(self, resources: Resources):
        """
        Init EventGenerator resources with Dependency Injection pattern.
        """
        # Set args
        self.__num_of_events, self.__min_delay_ms, self.__max_delay_ms = EventGenerator.parse_arguments()

        # DI
        self.__resources = resources
        # Init privates
        self.__first_names = []
        self.__last_names = []
        self.__cities = []

        # Generate fake data
        self.generate_fake_data()

    @staticmethod
    def parse_arguments():
        ap = argparse.ArgumentParser()
        ap.add_argument("-e", "--num_of_events", required=True, type=int, choices=range(0, 100001),
                        metavar="[0-100000]", help="how many event you want to generate")
        ap.add_argument("-m", "--min_delay_ms", required=True, type=int, choices=range(0, 1001),
                        metavar="[0-1000]", help="minimum delay between events in ms")
        ap.add_argument("-M", "--max_delay_ms", required=True, type=int, choices=range(0, 1001),
                        metavar="[0-1000]", help="maximum delay between events in ms")
        args = vars(ap.parse_args())
        return args['num_of_events'], args['min_delay_ms'], args['max_delay_ms']

    @log_performance
    def run(self):
        """
        Generate events and send them into Kafka Service.
        :return: None
        """
        logger.info('Start sending events with params: {}'.format(
            (self.__num_of_events, self.__min_delay_ms, self.__max_delay_ms)
        ))

        for _ in self.send_events():
            pass

        logger.debug('Block until all async messages are sent.')
        self.__resources.get_kafka_producer().flush()

    def generate_fake_data(self):
        """
        Generate fake data.
        :return: None
        """
        faker = Faker()
        for _ in range(100):
            self.__first_names.append(faker.first_name())
            self.__last_names.append(faker.last_name())
            self.__cities.append(faker.city())

    def send_events(self) -> Generator[
        FutureRecordMetadata, None, None
    ]:
        """
        Generator method, every next call sends a new event and does delay.
        :return: None
        """
        for i in range(min(self.__num_of_events, self.MAX_NB_OF_EVENTS)):
            # Dodanie nowego zdarzenia przy użyciu generatora.

            payload = {
                'imię': choice(self.__first_names),
                'nazwisko': choice(self.__last_names),
                'wiek': randint(10, 99),
                'miasto zamieszkania': choice(self.__cities)
            }

            yield self.__resources.get_kafka_producer().send(Resources.TOPIC_RAW_DATA, payload)

            sleep_delay_s = randint(self.__min_delay_ms, self.__max_delay_ms) / 1000
            logger.debug('{} events are sent. Sleep delay: {}s'.format(i + 1, sleep_delay_s))
            # Przerwa między dodawaniem zdarzeń.
            sleep(sleep_delay_s)


if __name__ == '__main__':
    logger.info('START EventGenerator')
    EventGenerator(
        Resources()
    ).run()
    logger.info('END EventGenerator')
