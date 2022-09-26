#!/usr/bin/env python
# coding: utf-8

pip install Imbox

import os
from imbox import Imbox # pip install imbox
import traceback

# enable less secure apps on your google account
# https://myaccount.google.com/lesssecureapps

host = "imap.gmail.com"
username = "dbdaproject2022@gmail.com"
password = 'vcyq batk nibo hkax'
download_folder = "/home/talentum/shared/project"
# download_folder = "/home/talentum/shared/project"

if not os.path.isdir(download_folder):
   os.makedirs(download_folder, exist_ok=True)

mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)# secure socket layer
messages = mail.messages(subject='data-part-2') # defaults to inbox



for (uid, message) in messages:
    mail.mark_seen(uid) # optional, mark message as read

    for idx, attachment in enumerate(message.attachments):
        try:
            att_fn = attachment.get('Data_part2.xlsx', 'emailDoc.xlsx')   # param 1: search as, param 2: save file as
            download_path = f"{download_folder}/{att_fn}"
            print(download_path)
            with open(download_path, "wb") as fp:
                fp.write(attachment.get('content').read())
        except:
            pass
            print(traceback.print_exc())

mail.logout()
