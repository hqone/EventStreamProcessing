import json
import socketserver
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Tuple

from app.Resources import Resources


class WebService(BaseHTTPRequestHandler):
    HOST_NAME = 'web-service'
    EXTERNAL_HOST_NAME = 'localhost'
    SERVER_PORT = 8080

    def __init__(self, request: bytes, client_address: Tuple[str, int], server: socketserver.BaseServer) -> None:
        """
        Init resources at WebService
        :param request:
        :param client_address:
        :param server:
        """
        self.__resources = Resources()
        super().__init__(request, client_address, server)

    def _set_headers(self):
        """
        Set HTTP headers for JSON content.
        """
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        """
        Main WebService method, have three routes:
        - /average_age
        - /most_populated_cities
        - /* - other routes return json with URL to two standard services
        """
        self._set_headers()
        if self.path == '/average_age':
            self.wfile.write(json.dumps(self.get_average_age()).encode(encoding='utf_8'))
        elif self.path == '/most_populated_cities':
            self.wfile.write(json.dumps(self.get_most_populated_cities()).encode(encoding='utf_8'))
        else:
            self.wfile.write(json.dumps(
                {'services': {
                    'average_age': 'http://{}:{}/average_age'.format(
                        self.EXTERNAL_HOST_NAME, self.SERVER_PORT
                    ),
                    'most_populated_cities': 'http://{}:{}/most_populated_cities'.format(
                        self.EXTERNAL_HOST_NAME, self.SERVER_PORT
                    )
                }}).encode(encoding='utf_8'))

    def get_most_populated_cities(self) -> list:
        """
        Return TOP 10 cites with the largest number of residents.
        :return: list of cities: [{"_id": "Pozna\u0144", "count": 2}, {"_id": "Katowice", "count": 2}, (...)]
        """
        return list(self.__resources.get_storage().computed_data.aggregate([
            {'$group': {
                '_id': '$miasto zamieszkania',
                'population': {'$sum': '$count'}
            }},
            {'$sort': {'population': -1}},
            {'$limit': 10}
        ]))

    def get_average_age(self) -> list:
        """
        Return real average age from all given events.
        :return: list with age:
        """
        return list(self.__resources.get_storage().computed_data.aggregate([{
            '$group': {
                '_id': 'avg_wiek',
                'numerator': {'$sum': {'$multiply': ['$avg_wiek', '$count']}},
                'denominator': {'$sum': '$count'}
            }
        }, {
            '$project': {
                'average': {'$divide': ['$numerator', '$denominator']}
            }
        }]))


if __name__ == "__main__":
    webServer = HTTPServer((WebService.HOST_NAME, WebService.SERVER_PORT), WebService)
    print('Server started http://{}:{}/'.format(
        WebService.EXTERNAL_HOST_NAME, WebService.SERVER_PORT
    ))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
