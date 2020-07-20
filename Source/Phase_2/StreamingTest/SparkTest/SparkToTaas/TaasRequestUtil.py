import ConfigParser
import requests
import json
config = ConfigParser.RawConfigParser()
config.read('parameters.cfg')


class TaasRequestUtil:

    def __init__(self):
        self.url = config.get("TAAS_URL", "service_url")
        self.login_url = config.get("TAAS_URL", "login_url")
        self.tokenize_url = config.get("TAAS_URL", "tokenize_url")
        self.detokenize_url = config.get("TAAS_URL", "detokenize_url")

        self.filename = config.get("USER_CREDENTIAL", "filename")
        self.filepath = config.get("USER_CREDENTIAL", "passwordfile")
        self.username = config.get("USER_CREDENTIAL", "username")

        self.token = None

    def login(self):
        files = {'password': open(self.filepath + self.filename, 'rb')}
        payload = {'username': self.username}

        if self.token is None:
            response = requests.post(self.url + self.login_url, files=files, data=payload )
            json_response = json.loads(response.text)
            if 'token' in json_response:
                self.token = json_response['token']
            else:
                print json_response['message']


    def tokenize(self, input):
        # Once login fails try connecting again
        if self.token is None:
             self.login()
        else:
            #print 'tokenizing'
            payload  = {'input':input}
            headers = {'x-access-token': self.token}
            response = requests.post(self.url + self.tokenize_url, data= payload, headers= headers)
            output_json =  json.loads(response.text)['obj']
            token_pair = (output_json['RAW_DATA'], output_json['TOKEN_DATA'])
            return token_pair

    def detokenize(self, input):
         # Once login fails try connecting again
        if self.token is None:
             self.login()
        else:
            #print 'detokenizing'
            payload  = {'input': input}
            headers = {'x-access-token': self.token}
            response = requests.post(self.url + self.detokenize_url, data= payload, headers= headers)
            output_json =  json.loads(response.text)['obj']
            detoken_pair = (output_json['TOKEN_DATA'],output_json['RAW_DATA'])
            return detoken_pair
