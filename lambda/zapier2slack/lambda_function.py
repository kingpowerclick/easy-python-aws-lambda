from __future__ import print_function
import logging
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def slack_report(pusher_details):
    hook_url = pusher_details.get('hook_url', None)
    if not hook_url:
        raise Exception('hook_url not defined.')

    report_name = pusher_details.get('report_name', None)
    if not report_name:
        raise Exception('report_name not defined.')

    r = requests.post(hook_url, data={'report_name': report_name})

    logger.info(r.json())


def lambda_handler(event, context):
    logger.info('Trigger event...')

    zapier_event = event.get('Zapier2SlackEvent', None)
    if not zapier_event:
        raise Exception('No Zapier2Slack event defined.')

    pusher_type = zapier_event.get('pusher_type', None)
    pusher_details = zapier_event.get('pusher_details', None)
    if not pusher_type or not pusher_details:
        raise Exception('pusher_type or pusher_details not defined.')

    if pusher_type == 'slack_report':
        slack_report(pusher_details)

    logger.info('Event {} done from zapier2slack pusher.'.format(pusher_type))
