import requests
import time


class B24:
    def __init__(self, domain: str, user_id: int, token: str):
        self.domain = domain
        self.user_id = user_id
        self.token = token

    def get(self, url: str, params: dict = None):
        resp = requests.get('https://' + self.domain +
                            '/rest/' + str(self.user_id) + '/' + self.token + '/' + url, params=params)
        return resp

    def post(self, url: str, json: dict = None, data:dict = None, files:dict = None, wait_for_limit:bool = False):
        if wait_for_limit:
            for k in range(0, 5):
                time.sleep(k*10)
                resp = requests.post('https://' + self.domain +
                                     '/rest/' + str(self.user_id) + '/' + self.token + '/' + url, json=json, files=files, data=data)

                if 'error' not in resp.json().keys():
                    return resp

        resp = requests.post('https://' + self.domain +
                             '/rest/' + str(self.user_id) + '/' + self.token + '/' + url, json=json, files=files,
                             data=data)
        return resp

    def get_list(self, url: str, b24_filter: dict = None, select: list = None, entityTypeId: int = None,
                 total_count_only: bool=False):
        entities = []

        start_pos = 0
        total = 1
        while start_pos < total:
            data = {'start': start_pos, 'filter': b24_filter}
            if entityTypeId:
                data['entityTypeId'] = entityTypeId
            if select:
                data['select'] = select
            response = self.post(url, json=data).json()
            if 'error' in response.keys():
                if response['error'] == 'QUERY_LIMIT_EXCEEDED':
                    time.sleep(5)
                    print('delay 5s')
                    continue
            start_pos += 50
            if 'total' not in response:
                print('НЕТ ключа total в ответе:', response)
                total = None
            else:
                total = response['total']
            if total_count_only:
                return total
            if start_pos == 50:
                print(url, 'Total_count =', total)
            if start_pos % 1000 == 0:
                time.sleep(1)
                print('delay 1s')
            result = response['result']
            if entityTypeId:
                result = result['items']
            for entity in result:
                entities.append(entity)
        return entities
    
    def call(self, method: str, params: dict = None):
        """Прямой вызов метода API Bitrix24, без использования get_list"""
        response = self.post(method, json=params).json()
        if 'error' in response:
            print(f"[Ошибка API] Метод: {method} — {response['error_description']}")
        return response

