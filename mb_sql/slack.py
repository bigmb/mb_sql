'''
Module for slack integration
'''

import requests
import json

__all__ = ['send_slack_message']

def send_slack_message(message, webhook):
    """Send a Slack message to a channel via a webhook. 
    
    Args:
        message (dict): Dictionary containing Slack message, i.e. {"text": "This is a test"}
        webhook (str): Full Slack webhook URL for your chosen channel. 
    
    Returns:
        HTTP response code
    """

    return requests.post(webhook, json.dumps(message))

