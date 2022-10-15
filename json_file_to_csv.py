import sys
import json
import csv
import os
from os import listdir
from os.path import isfile, join
import glob
import pandas as pd

#dynamic variables              
inp_no = 0
dir_no = 0

#constant variables
input_dir = "/home/joe/Documents/code/facebook/json/"
files = [f for f in listdir(input_dir) if isfile(join(input_dir, f))]
input_json = open(input_dir + files[inp_no], "r").read()
json_data = json.loads(input_json)
table = "messages"
message_csv = "/home/joe/Documents/code/facebook/csv/message_output.csv"
extra_content = "/home/joe/Documents/code/facebook/csv/extra_content.csv"
content_stitched = "/home/joe/Documents/code/facebook/csv/content_stitched_col.csv"
header_file = "/home/joe/Documents/code/facebook/csv/header.csv"
extra_header_file = "/home/joe/Documents/code/facebook/csv/header2.csv"
header_stitched = "/home/joe/Documents/code/facebook/csv/header_stitched.csv"
completed_csv = "/home/joe/Documents/code/facebook/csv/complete_stitched.csv"

#header_experimental = list(json_data[table][0])
#list(json_data) #gather headers + "participants" and "messages". unnecessary fo rcurrent process but may be usefuli in future
#stitches extra content to the right of main content
#headers
    #main header
header = [
    "sender_name", 
    "type", 
    "content", 
    "timestamp_ms", 
    "photos", 
    "share", 
    "sticker",
    "gifs",
    "videos",
    "audio_files",
    "call_duration", 
    "missed",
    "files",
    "reactions"
    ]
with open(header_file, "w") as csvfile:
    w = csv.DictWriter(csvfile, fieldnames = header)
    w.writeheader()
    #extra header

list_of_headers = [
    "title",
    "is_still_participant",
    "thread_type",
    "thread_path" 
]
with open(extra_header_file, 'w') as myfile: #create extra header file
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(list_of_headers)
    #stitch headers

reader = csv.reader(open(header_file, 'r'))
reader1 = csv.reader(open(extra_header_file, 'r'))
writer = csv.writer(open(header_stitched, 'w'))
for row in reader:
    row1 = next(reader1)
    writer.writerow(row + row1)
    writer = csv.DictWriter(sys.stdout, json_data[table][0].keys()) #this line fixes header_stitched somehow? investigate

def conversion_loop():
    while inp_no <= (len(files)-1):
        print('start loop')
        list_of_extras = [
        json_data["title"],
        json_data["is_still_participant"],
        json_data["thread_type"],
        json_data["thread_path"]
        ]
        #while files left in dir:
        #content
        #main content
        json_data = json.loads(input_json)
        writer = csv.DictWriter(sys.stdout, json_data[table][0].keys()) #this line fixes header_stitched somehow? investigate
        with open(message_csv, "w") as csvfile:
            w = csv.DictWriter(csvfile, fieldnames = header)
            for data in json_data[table]:
                w.writerow(data)
        print('extra content')
        def count_rows():
            with open(message_csv,"r") as f:
                reader = csv.reader(f,delimiter = ",")
                data = list(reader)
                row_count = len(data)
                return row_count
        print('create extra content file')
        with open(extra_content, 'w') as myfile: 
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for i in range(0,count_rows()):
                wr.writerow(list_of_extras)
        print('stitch content columns')
        message = pd.read_csv(message_csv, header = None, low_memory=False)
        extra = pd.read_csv(extra_content, header = None, low_memory=False)
        df = pd.concat([message, extra], axis = 1)
        df.to_csv(content_stitched, index = False, header = None)
        print('stitch content to header')
        message = pd.read_csv(content_stitched, header = None, low_memory=False)
        extra = pd.read_csv(header_stitched, header = None, low_memory=False)
        df = pd.concat([extra, message], axis = 0)
        df.to_csv(header_stitched, index = False, header = None)
        print(files[inp_no])
        inp_no = inp_no + 1
        print("inp_no ==========" + str(inp_no))
        if inp_no <= (len(files)-1):
            files = [f for f in listdir(input_dir) if isfile(join(input_dir, f))]
            input_json = open(input_dir + files[inp_no], "r").read()
            json_data = json.loads(input_json)
            #dir_no = dir_no + 1
            #np_no = 0

dir_no = 0
if there are json files in directory:
    json_to_csv
dir_no = dir_no + 1
inp_no = 0