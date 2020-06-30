import urllib3
import json
import os

http = urllib3.PoolManager()

def lambda_handler(event, context):
    teams_webhook_id = os.environ['TEAMS_WEBHOOK_ID']
    teams_webhook_url = "https://outlook.office.com/webhook/"
    teams_webhook_uri = teams_webhook_url+teams_webhook_id

    message_json = json.loads(event['Records'][0]['Sns']['Message'])
    state = message_json['NewStateValue']
    descr = message_json['AlarmDescription']

    if state == "OK":
        theme_color = "1BBB05"
    elif state == "ALARM":
        theme_color = "FF0000"
    else:
        theme_color = "000000"

    webhook_payload = {
        "@context": "http://schema.org/extentions",
        "@type": "MessageCard",
        "themeColor": theme_color,
        "title": state,
        "text": descr
    }

    webhook_encoded_body = json.dumps(webhook_payload).encode('utf-8')
    resp = http.request('POST', teams_webhook_uri, body=webhook_encoded_body)

    print({
        "message": "["+state+"] "+descr,
        "status_code": resp.status,
        "response": resp.data
    })
