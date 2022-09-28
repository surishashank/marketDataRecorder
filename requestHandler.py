import requests
import time
import logging

class requestHandler:
    def __init__(self, limit_per_sec, cooldown_period_sec):
        self.limit_per_sec = limit_per_sec
        self.cooldown_period_sec = cooldown_period_sec
        self.send_times_arr = []
        logging.debug(f'Initialized requestHandler with limit_per_sec:{limit_per_sec} '
                      f'cooldown_period_sec:{cooldown_period_sec}')

    # This is the only function that all external classes should call.
    def get(self, request_url, params = None, timeout = 10):
        try:
            prepared_request = requests.PreparedRequest()
            prepared_request.prepare_url(request_url, params)
            logging.info(f'Sending request: {prepared_request.url}')
            # response = requests.get(request_url, params=params, timeout=timeout)
            response = self.sendRequest(prepared_request.url, timeout=timeout)
        except Exception as e:
            logging.exception(f'Connection error. Cooling down for {self.cooldown_period_sec}...')
            time.sleep(self.cooldown_period_sec)
            return self.get(request_url, params, timeout)

        if not response.ok:
            logging.error(f'Request url:{request_url} returned with status code:{response.status_code} Sleeping'
                          f' for {self.cooldown_period_sec} seconds...')
            time.sleep(self.cooldown_period_sec)
            return self.get(request_url, params, timeout)

        return response

    def sendRequest(self, request_url, timeout):
        sendTime = time.time()
        if self.getNumRequestSentInLastSecond(sendTime) < self.limit_per_sec:
            response = requests.get(request_url, timeout=timeout)
            self.send_times_arr.append(sendTime)
        else:
            time.sleep(0.1)
            return self.sendRequest(request_url, timeout)

        return response

    def getNumRequestSentInLastSecond(self, currentTime):
        numRequestSentInLastSecond = 0
        for sendTime in reversed(self.send_times_arr):
            diff = currentTime - sendTime
            if diff < 1:
                numRequestSentInLastSecond += 1
            else:
                break
        self.send_times_arr = self.send_times_arr[len(self.send_times_arr)-numRequestSentInLastSecond:]
        logging.debug(f'Limit:{self.limit_per_sec} NumReqSent:{numRequestSentInLastSecond} CurrentTime:{currentTime}')
        return numRequestSentInLastSecond
