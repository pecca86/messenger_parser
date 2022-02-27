'''
Parses the Facebook Messenger threads_db2 sqlite database and downloads all attachments from the cloud.
Author: Pekka Ranta-aho
'''
from importlib.metadata import entry_points
import sqlite3
import csv
import json
from collections import defaultdict
from datetime import date, datetime
import requests
import os



def download_all_attachments(attachments):
    '''
    Loops trough the attachment column and downloads all the files based on the stored URL from the cloud.
    '''

    #Convert the string into a JSON formatted object
    data = json.loads(attachments)

    # Create a sequence number in order to get unique file names for nameless files
    sequence_number = 0

    for entry in data:
        if entry.get('audio_uri'):
            url = entry.get('audio_uri', 'invalid_uri')
            filename = entry.get('filename', f'audio_{sequence_number}.mp4')
            sequence_number += 1
            if len(filename) <= 0:
                filename = f'audio_{sequence_number}.mp4'
                sequence_number += 1

            r = requests.get(url, allow_redirects=True)
            open(f'files/{filename}', 'wb').write(r.content)
        
        if entry.get('urls'):
            url = parse_image_attachment_uris(entry['urls'])
            filename = entry.get('filename', f'image_{sequence_number}.jpg')
            sequence_number += 1
            if len(filename) <= 0:
                filename = f'image_{sequence_number}.jpg'
                sequence_number += 1

            r = requests.get(url, allow_redirects=True)
            open(f'files/{filename}', 'wb').write(r.content)
            
        
        if entry.get('video_data_url'):
            url = entry.get('video_data_url')
            filename = entry.get('filename', f'video_{sequence_number}.mp4')
            sequence_number += 1
            if len(filename) <= 0:
                filename = f'video_{sequence_number}.mp4'
                sequence_number += 1

            r = requests.get(url, allow_redirects=True)
            open(f'files/{filename}', 'wb').write(r.content)

        sequence_number += 1
            


def download_attachments(messages_dict):
    videos = {}
    images = {}
    audio = {}

    sequence_number = 0

    #Parse file name and url as key-value pairs
    for k, v in messages_dict.items():
        for item in messages_dict[k]:
            for key, value in item['attachments'].items():
                for k, v in item['attachments'][key].items():
                    if 'image' in item['attachments'][key][k]['file_type']:
                        images[item['attachments'][key][k].get('file_name', "NO FILENAME")] = item['attachments'][key][k].get('file_url', "NO URL")
                    elif 'video' in item['attachments'][key][k]['file_type']:
                        videos[item['attachments'][key][k].get('file_name', "NO FILENAME")] = item['attachments'][key][k].get('video_url', "NO URL")
                    elif 'audio' in item['attachments'][key][k]['file_type']:
                        audio[item['attachments'][key][k].get('file_name', "NO FILENAME")] = item['attachments'][key][k].get('audio_uri', "NO URL")
                    else:
                        pass

    
    # DOWNLOAD ALL VIDEOS
    for k,v in videos.items():
        filename = ""
        url = ""
        if k == "NO FILENAME":
            filename = f'video_{sequence_number}.mp4'
            sequence_number += 1
        else:
            filename = f'{k}.mp4'
        url = v

        r = requests.get(url, allow_redirects=True)
        open(f'files/{filename}', 'wb').write(r.content)

    # DOWNLOAD ALL IMAGES
    for k,v in images.items():
        filename = ""
        url = ""
        if k == "No file name":
            filename = f'image_{sequence_number}.jpg'
            sequence_number += 1
        else:
            filename = f'{k}.jpg'
        url = v

        r = requests.get(url, allow_redirects=True)
        open(f'files/{filename}', 'wb').write(r.content)

    # DOWNLOAD ALL AUDIO FILES
    for k,v in audio.items():
        filename = ""
        url = ""
        if k == "No file name":
            filename = f'audio_{sequence_number}.mp4'
            sequence_number += 1
        else:
            filename = f'{k}.mp4'
        url = v

        r = requests.get(url, allow_redirects=True)
        open(f'files/{filename}', 'wb').write(r.content)
        

def parse_to_csv(messages_dict):
    with open('messenger_chat.csv', 'w', newline='', encoding="utf-16") as messenger_csv:
        csv_writer = csv.writer(messenger_csv, delimiter = '#')

        csv_writer.writerow([
            'Message', 
            'Message participants', 
            'Sender', 
            'Timestamp', 
            'Timestamp sent', 
            'Attachments',
            'Event type',
            'Is videocall',
            'Call data'
            ])


        for k, v in messages_dict.items():
            for item in messages_dict[k]:
                csv_writer.writerow([
                    item.get('text', 'no text'),
                    item.get('msg_participants', 'no msg_participants'),
                    item.get('sender', 'no sender'),
                    item.get('timestamp', 'no timestamp'),
                    item.get('timestamp_sent', 'no timestamp_sent'),
                    item.get('attachments', 'no attachments'),
                    item.get('event_type', 'no event_type'),
                    item.get('is_video_call', 'no is_video_call'),
                    item.get('call_data', 'no call_data'),
                ])
                
        
def parse_participants(participants):
    '''
    Parses the message participants from a string formatted as
    FACEBOOK:ID1:ID2
    '''
    participant_list = participants.split(":")
    sender = ""
    receiver = ""
    if participant_list[0] == "ONE_TO_ONE":
        sender = participant_list[1]
        receiver = participant_list[2]

    return (sender, receiver)


def parse_sender(sender):
    '''
    Parses the sender name from the JSON data
    '''
    sender_data = json.loads(sender)
    return sender_data['name']


def parse_image_attachment_uris(attachment_uri):
    '''
    Parses out the file URI from the nested JSON object
    '''
    data = json.loads(attachment_uri)
    uri_data = json.loads(data['MEDIUM_PREVIEW'])
    return uri_data['src']

def parse_video_attachment_urls(attachment_url):
    pass

def parse_attachment_data(attachment_data):
    '''
    Parses the attachment data from a JSON object
    '''
    data = json.loads(attachment_data)

    parsed_data = {}
    image_attachment_dict = {}
    video_attachment_dict = {}
    audio_attachment_dict = {}

    try:
        for entry in data:
            if "image/jpeg" in entry['mime_type']:
                # Create a dictionary with filename as key, url, type, size, timestamp as a list of values
                #filename_file_info_dict = dict(zip(filenames, [list(a) for a in zip(file_urls, file_types, file_sizes, file_timestamps)]))
                time = datetime.fromtimestamp(int((entry['setReceivedTimestampMs'])/1000))
                timestamp = date.strftime(time, format="%d.%m.%Y %H:%M:%S")
                image_attachment_dict[entry['filename']] = {
                    "file_url": parse_image_attachment_uris(entry['urls']),
                    "file_name": entry['filename'],
                    "file_type": entry['mime_type'],
                    "file_size": entry['file_size'],
                    "time_stamp": timestamp
                }
            
                parsed_data['attachment'] = image_attachment_dict
                
                
            elif "video" in entry['mime_type']:
                video_data_length = entry['video_data_length']
                video_data_length_ms = entry['video_data_length_ms']
                video_data_url = entry['video_data_url']
                file_type = entry['mime_type']
                file_name = entry['filename']
                if len(file_name) <= 0:
                    file_name = "No file name"
                file_size = entry['file_size']

                video_attachment_dict[file_name] = {
                    "file_name": file_name,
                    "video_length_seconds": video_data_length,
                    "video_length_ms": video_data_length_ms,
                    "video_url": video_data_url,
                    "file_type": file_type,
                    "file_size": file_size
                }
                parsed_data['attachment'] = video_attachment_dict


            elif "audio" in entry['mime_type']:
                file_name = entry['filename']
                file_size = entry['file_size']
                is_voicemail = entry['is_voicemail']
                audio_uri = entry['audio_uri']
                duration_seconds = entry['durationS']
                duration_ms = entry['durationMs']
                file_type = entry['mime_type']
                #received_timestamp = entry['setReceivedTimestampMs']

                time = datetime.fromtimestamp(int((entry['setReceivedTimestampMs'])/1000))
                received_timestamp = date.strftime(time, format="%d.%m.%Y %H:%M:%S")

                audio_attachment_dict[file_name] = {
                    "file_name": file_name,
                    "file_type": file_type,
                    "file_size": file_size,
                    "is_voicemail": is_voicemail,
                    "duration_seconds": duration_seconds,
                    "duration_ms": duration_ms,
                    "received_timestamp": received_timestamp,
                    "audio_uri": audio_uri
                }

                parsed_data['attachment'] = audio_attachment_dict
            else:
                pass

    except KeyError as e:
        pass
        
    return parsed_data

def parse_call_data(call_data):
    '''
    Parser the call information from a JSON object
    '''
    data = json.loads(call_data)
    parsed_data = {}
    try:
        parsed_data = {
            "event": data["event"],
            "caller_id": data['caller_id'],
            "caller_name": contacts_dict["FACEBOOK:"+data['caller_id']],
            "video": data['video'],
            "call_duration": data['call_duration']
        }
    except KeyError as e:
        pass
    return parsed_data


#Create files folder
if not os.path.exists('files'):
    os.mkdir('files')
con = sqlite3.connect('threads_db2', uri=True)
con.row_factory = sqlite3.Row
cur = con.cursor()

# Kontaktit
cur.execute('SELECT * FROM thread_users')

contacts_dict = defaultdict(lambda: "NULL")

# Kerää kaikki kontaktit dictonaryyn
for record in cur.fetchall():
    contacts_dict[record['user_key']] = record['name']


# Select all messages
message_threads = defaultdict(lambda: "not found")

cur.execute('SELECT * FROM messages')

for record in cur.fetchall():
    
    #Get the participants in the conversation
    participant_tuple = parse_participants(record['thread_key'])

    #Append FACEBOOK: in order to search the name of the participants from our conctacts dictionary
    participant1_key = f'FACEBOOK:{participant_tuple[0]}'
    participant2_key = f'FACEBOOK:{participant_tuple[1]}'

    participants = [contacts_dict[participant1_key], contacts_dict[participant2_key]]

    # Check if there is a value for sender
    if record['sender']:
        sender = parse_sender(record['sender'])
    else:
        sender = ""

    # Check if there is a value for text
    if record['text']:
        text = record['text']
    else:
        text = ""

    #Check if there is a timestamp and a sent timestamp
    if record['timestamp_ms']:
        time = datetime.fromtimestamp(int((record['timestamp_ms'])/1000))
        timestamp = date.strftime(time, format="%d.%m.%Y %H:%M:%S")
        #timestamp = record['timestamp_ms']
    else:
        timestamp = ""
    
    if record['timestamp_sent_ms']:
        time = datetime.fromtimestamp(int((record['timestamp_sent_ms'])/1000))
        timestamp_sent = date.strftime(time, format="%d.%m.%Y %H:%M:%S")
        #timestamp_sent = record['timestamp_sent_ms']
    else:
        timestamp_sent = ""
    
    #Check if there are any attachments to the message
    if record['attachments']:
        download_all_attachments(record['attachments'])
        attachments = parse_attachment_data(record['attachments'])
    else:
        attachments = {}
    
    #Check type of event
    if record['admin_text_thread_rtc_event']:
        event_type = record['admin_text_thread_rtc_event']
    else:
        event_type = ""
    
    #Check if the event was a video call
    if record['admin_text_thread_rtc_is_video_call']:
        is_video_call = record['admin_text_thread_rtc_is_video_call']
    else:
        is_video_call = ""
    
    #Get call information
    if record['generic_admin_message_extensible_data']:
        call_data = parse_call_data(record['generic_admin_message_extensible_data'])
        #call_data = record['generic_admin_message_extensible_data']
    else:
        call_data = {}

    #Construct the data
    data =  {
        "text": text,
        "msg_participants": participants,
        "sender": sender,
        "timestamp": timestamp,
        "timestamp_sent": timestamp_sent,
        "attachments": attachments,
        "event_type": event_type,
        "is_video_call": is_video_call,
        "call_data": call_data
    }

    #Check if thread_key already exist, otherwise create it
    if message_threads[record['thread_key']] == "not found":
        message_threads[record['thread_key']] = []
    message_threads[record['thread_key']].append(data)



parse_to_csv(message_threads)
#download_attachments(message_threads)

            
