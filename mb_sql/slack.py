<<<<<<< HEAD
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

=======
import requests
import json

__all__ = ['slack_msg']

def slack_msg(webhook,msg,logger=None):
    """
    Send a message to a slack channel
    Args:
        webhook (str): Slack webhook URL
        msg (str): Message to send
        logger (logging.Logger): Logger to use
    Returns:
        None
    """
    
    response = requests.post(
        url=webhook,
        data=json.dumps(msg),
        headers={'Content-Type': 'application/json'})

    if logger:
        logger.info('Slack response: %s', response.text)
>>>>>>> 2ca5e624b0b6a32d834b0ad98d141b06c920ccb7
