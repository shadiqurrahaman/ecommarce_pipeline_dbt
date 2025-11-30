import json,os,requests
from configs.etl_base import EtlBase # import all config


class Slack:
    def __init__(self):
        EtlBase.__init__(self)
        
    def post_reports_to_slack(self,title_name,test_data,url,bar_color):       
        data = {"attachments":[
                            {"color": bar_color,
                            "title": title_name,
                            "text": test_data}
                            ]}

        json_params_encoded = json.dumps(data)
        slack_response = requests.post(url=url,data=json_params_encoded,headers={"Content-type":"application/json"})
        if slack_response.text == 'ok':
                print('\n Successfully posted  report on Slack channel')
        else:
                print('\n Something went wrong. Unable to post report on Slack channel. Slack Response:'+slack_response)     
        
            