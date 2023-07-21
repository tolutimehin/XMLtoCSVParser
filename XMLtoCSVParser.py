#CODE TO LOOP THROUGH XML FILES IN AN SFTP SERVER, DYNAMMICALLY GET MEDICATION DOSAGE PER PATIENT AND WRITE IT INTO DB. 

#pip install -r req.txt

import shutil as sh
import pandas as pd
import numpy as np
import xml.etree.ElementTree as et

import pysftp
import paramiko
from base64 import decodebytes

import os
from os import listdir
from os.path import isfile, join

from datetime import datetime
from datetime import date

import subprocess




def xml_to_csv(filenames):
#DEFINE COLUMNS AS ARRAYS TO STORE VALUES

  EVENT_TIME_STAMP = []
  USER_ID = []
  USER_FIRST_NAME =[]
  USER_LAST_NAME = []
  PROVIDER_TYPE= []
  #PROVIDER_NUMBER=[]
  #PRIMARY_CREDENTIAL = []
  NATIONAL_PROVIDER_ID = []
  VISIT_NUMBER =[]
  PATIENT_ID = []
  PATIENT_FIRST_NAME =[]
  PATIENT_LAST_NAME = []
  PATIENT_SEX = []
  PATIENT_DOB = []
  PATIENT_STATUS = []
  DRUG_LABEL = []
  #DRUG_NAME = []
  SCHEDULE_DRUG = []
  SCHEDULE_DRUG_CLASS = []
  TOTAL_ADMINISTERED_AMOUNT = []
  TOTAL_WASTAGE_AMOUNT = []
  #ADMINISTERED_AMOUNT = []
  #ADMINISTERED_DATE = []
  AMOUNT_UNIT = []
  SCHEDULED_DATE = []
  REQUESTING_PHYSICIAN_FIRST_NAME = []
  REQUESTING_PHYSICIAN_LAST_NAME = []
  PHYSICIAN_NID = []
 
#LOOP THROUGH AND PROCESS EACH FILE GOTTEN FROM THE FTP SERVER 

  for file in filenames:
    print("Processing File: ",file)
    tree=et.parse(str(file))
    root=tree.getroot()
   
    ####need a while loop
#scan root for the tags in xml which would represent columns 
    count = 0
    for label in root.iter('label'):
      count = count + 1
      if label.text:  
        DRUG_LABEL.append(label.text)
      else:
        DRUG_LABEL.append('')

 # search for schedule, if none found, insert blankspace, if not append the tags found   
 if root.find(".//schedule-drug") is None:
      SCHEDULE_DRUG = SCHEDULE_DRUG +  ["" for _ in range(count)]
    else:
      for scheduled_drug in root.iter('schedule-drug'):
        if scheduled_drug.text:  
          SCHEDULE_DRUG.append(scheduled_drug.text)
        else:
          SCHEDULE_DRUG.append('')

    if root.find(".//schedule-drug-class") is None:
      SCHEDULE_DRUG_CLASS = SCHEDULE_DRUG_CLASS +  ["" for _ in range(count)]
    else:
      for scheduled_drug_class in root.iter('schedule-drug-class'):
        if scheduled_drug_class.text:  
          SCHEDULE_DRUG_CLASS.append(scheduled_drug_class.text)
        else:
          SCHEDULE_DRUG_CLASS.append('')

    if root.find(".//administered-amount") is None:
      TOTAL_ADMINISTERED_AMOUNT = TOTAL_ADMINISTERED_AMOUNT +  ["" for _ in range(count)]
    else:
      for adminsitered_amount in root.iter('administered-amount'):
        if adminsitered_amount.text:  
          TOTAL_ADMINISTERED_AMOUNT.append(adminsitered_amount.text)
        else:
          TOTAL_ADMINISTERED_AMOUNT.append('')

    if root.find(".//wastage-amount") is None:
      TOTAL_WASTAGE_AMOUNT = TOTAL_WASTAGE_AMOUNT +  ["" for _ in range(count)]
    else:
      for wastage_amount in root.iter('wastage-amount'):
        if wastage_amount.text:  
          TOTAL_WASTAGE_AMOUNT.append(wastage_amount.text)
        else:
          TOTAL_WASTAGE_AMOUNT.append('')
   
    #for value in root.iter('value'):
    #  print(value.text)
    if root.find(".//amount-unit") is None:
      AMOUNT_UNIT = AMOUNT_UNIT +  ["" for _ in range(count)]
    else:
      for amount_unit in root.iter('amount-unit'):
        if amount_unit.text:  
          AMOUNT_UNIT.append(amount_unit.text)
        else:
          AMOUNT_UNIT.append("*")

   
    #for static columns


    if (root.find('.//case-info').find('scheduled-at') is None):
      SCHEDULED_DATE = SCHEDULED_DATE +  ["" for _ in range(count)]
    else:
      SCHEDULED_DATE = SCHEDULED_DATE + [root.find('.//case-info').find('scheduled-at').text for _ in range(count)]
      schedule = len(SCHEDULED_DATE)
      i= 0
      while i < schedule:
        s=''.join(SCHEDULED_DATE [i])
        s = s.replace('T', ' ').replace('-06:00', '')
        SCHEDULED_DATE [i] = s
        i +=1


     
    #for all static columns

    if (root.find('.//provider').find('user').find('national-provider-id') is None):
      NATIONAL_PROVIDER_ID = NATIONAL_PROVIDER_ID +  ["" for _ in range(count)]
    else:
      NATIONAL_PROVIDER_ID = NATIONAL_PROVIDER_ID + [root.find('.//provider').find('user').find('national-provider-id').text for _ in range(count)]

    if (root.find('.//provider').find('user').find('provider-type') is None):
      PROVIDER_TYPE = PROVIDER_TYPE +  ["" for _ in range(count)]
    else:
      PROVIDER_TYPE= PROVIDER_TYPE + [root.find('.//provider').find('user').find('provider-type').text for _ in range(count)]


   
    USER_LAST_NAME= USER_LAST_NAME + [root.find('.//provider').find('user').find('last-name').text for _ in range(count)]
   
    USER_FIRST_NAME= USER_FIRST_NAME +[root.find('.//provider').find('user').find('first-name').text for _ in range(count)]

    PATIENT_ID= PATIENT_ID + [root.find('.//patient').find('patient-number').text for _ in range(count)]
   
    PATIENT_FIRST_NAME= PATIENT_FIRST_NAME + [root.find('.//patient').find('first-name').text for _ in range(count)]
   
    PATIENT_LAST_NAME= PATIENT_LAST_NAME + [root.find('.//patient').find('last-name').text for _ in range(count)]
   
    PATIENT_DOB= PATIENT_DOB + [root.find('.//patient').find('date-of-birth').text for _ in range(count)]

    PATIENT_SEX= PATIENT_SEX + [root.find('.//case-info').find('sex').text for _ in range(count)]

    PATIENT_STATUS= PATIENT_STATUS + [root.find('.//case-info').find('in-out-status').text for _ in range(count)]
   
    REQUESTING_PHYSICIAN_FIRST_NAME= REQUESTING_PHYSICIAN_FIRST_NAME + [root.find('.//surgeons').find('surgeon').find('first-name').text for _ in range(count)]
   
    REQUESTING_PHYSICIAN_LAST_NAME=REQUESTING_PHYSICIAN_LAST_NAME+[root.find('.//surgeons').find('surgeon').find('last-name').text for _ in range(count)]
   
    PHYSICIAN_NID=PHYSICIAN_NID+[root.find('.//surgeons').find('surgeon').find('national-provider-id').text for _ in range(count)]
   
    EVENT_TIME_STAMP=EVENT_TIME_STAMP+[root.find('.//case-info').find('scheduled-at').text for _ in range(count)]

   #REFORMAT TIMESTAMP TO REPLACE THE 'T' and '6AM' TIME STAMP
    stam = len(EVENT_TIME_STAMP)
    i= 0
    while i < stam:
     f =''.join(EVENT_TIME_STAMP [i])
     f = f.replace('T', ' ').replace('-06:00', '')
     EVENT_TIME_STAMP [i] = f
     i +=1

    VISIT_NUMBER=VISIT_NUMBER+[root.find('.//case-info').find('case-number').text for _ in range(count)]
   
   
    print("------------------------------")
    #print(scheduled_date)
    print("------------------------------")
    print("AMOUNT UNIT : ",AMOUNT_UNIT)
    #print(DRUG_LABEL,'\n',SCHEDULE_DRUG)
   
  PATIENT_ID = [str(x) for x in PATIENT_ID]  
  lists=[
          EVENT_TIME_STAMP,
          USER_FIRST_NAME,
          USER_LAST_NAME,
          PROVIDER_TYPE,
          #PROVIDER_NUMBER,
          #PRIMARY_CREDENTIAL,
          NATIONAL_PROVIDER_ID,
          VISIT_NUMBER,
          PATIENT_ID,
          PATIENT_FIRST_NAME,
          PATIENT_LAST_NAME,
          PATIENT_SEX,
          PATIENT_DOB,
          PATIENT_STATUS,
          DRUG_LABEL,
          #DRUG NAME
          SCHEDULE_DRUG,
          SCHEDULE_DRUG_CLASS,
          TOTAL_ADMINISTERED_AMOUNT,
          TOTAL_WASTAGE_AMOUNT,
          #ADMINISTERED_AMOUNT,
          AMOUNT_UNIT,
          #ADMINISTERED_DATE,
          SCHEDULED_DATE,        
          REQUESTING_PHYSICIAN_FIRST_NAME,
          REQUESTING_PHYSICIAN_LAST_NAME,
          PHYSICIAN_NID]
   
  print('----------')
  print("Lists = ", lists)
  print('----------')
  print('----------')

#TRANSPOSE 2D ARRAYS

  list_transpose=[]

  for cols in [col for col in range(len(lists[0]))]:
    temp=[]
    for index,item in enumerate(lists):
      temp.append(item[cols])
      #print(temp)
    list_transpose.append(temp)
   
  print('----------')
  print("Lists Transpose = ", list_transpose)
  print('----------')

#LOADING INTO DATAFRAME AFTER TRANSPOSING
 
  MedAx_df = pd.DataFrame(data=list_transpose, columns=['Event_Time_Stamp', 'User_First_Name', 'User_Last_Name', 'Provider_Type', 'National_Provider_ID', 'Visit_Number', 'Patient_ID', 'Patient_First_Name', 'Patient_Last_Name', 'Patient_Sex', 'Patient_DOB', 'Patient_Status', 'Drug_Label', 'Schedule_Drug', 'Schedule_Drug_Class', 'Total_Administered_Amount', 'Total_Wastage_Amount', 'Amount_Unit', 'Scheduled_Date', 'Req_Physician_FName', 'Req_Physician_LName', 'Req_Physician_NPI' ])


  MedAx_df['Patient_ID'] = MedAx_df['Patient_ID'].astype('str')
  MedAx_df['User_ID'] = MedAx_df.User_First_Name.replace(" ", "", regex =     True).str.cat(MedAx_df.User_Last_Name).str.lower()


  mid = MedAx_df['User_ID']
  MedAx_df.drop(labels=['User_ID'], axis=1, inplace = True)
  MedAx_df.insert(1, 'User_ID', mid)

 

   
  print(MedAx_df)
  MedAx_df.to_csv("comanche_medaxion_"+str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))+".txt", sep ='|', index = False)


#INSERT APPROPRAITE SFTP CREDENTIALS
Hostname = "00.00.00.00"
port = 22
Username = "username"
Password = "password"

keydata = b"""AAAAB3NzaC1yc2EAAAADAQABAAABAQDhYu2Mvf5tRJZDUAyzhGo8OKtKLSsC7EuLuhbXRtZGl6Fd66uMzp7ddd6lz5N8X2yRZpV7MKXQuqjuk4kY3kHyvAJIO6wUyIDAzxwltHeqHAxteQH/cz3qeJz/vUkKnyYdqukdANUPl759N9r4IITFBfVnifVKENbAmar9AjTM55e+WgCxA6ek08mMx4bF29NPYCbZvdnNMMPCbnQwkYVuTJSQoLVKWuNAdgTZuQqLUutbIu52qRDl9SG86fVsDw9ZmDpfcP/r0l4101HFKNvzl9rJhuoVHeeAnlVXt5HMJArbxhOfL0/dd6g+YWE+l0vaEVJrTbHgRCgw4N6703Tr"""
key = paramiko.RSAKey(data=decodebytes(keydata))
cnopts = pysftp.CnOpts()
cnopts.hostkeys.add('00.00.00.00', 'ssh-rsa', key)

files = []

with pysftp.Connection(host=Hostname, port=port, username=Username, password=Password, cnopts=cnopts) as sftp:
    print("Connection successfully established ... ")
    file_no = 0

 

    for filename in sftp.listdir():
      name_length=len(filename)
      year, month, day = [int(x) for x in filename[name_length-23:name_length-13].split(".")]
      #print(filename[name_length-23:name_length-13].split("."))
      #print(year,month,day)
      file_date = date(year,month,day)
      todays_date = date.today()
      diff = todays_date-file_date
      #if diff.days == 7:
      #and file_no < 5
      if diff.days ==0:    #Changed from <=1 to ==0
        file_no = file_no + 1
        print(filename)
        sftp.get(filename)
        print(join(os.getcwd(),str(filename)), " " ,join(os.getcwd(),"files",str(filename)))
        sh.move(join(os.getcwd(),str(filename)), join(os.getcwd(),"files",str(filename)))
        new_file_path = join(os.getcwd(),"files",str(filename))
        files.append(str(new_file_path))
      #print(year,month,day,todays_date)
print(files)
xml_to_csv(files)


# Database connection details
db_host = 'host'
db_user = 'admin'
db_password = 'password'
db_name = 'MedAx'
table_name = 'MedAxCases'
identity_column = 'patientID'

# Create database connection
db_url = f'mysql://{db_user}:{db_password}@{db_host}/{db_name}'
engine = create_engine(db_url)

# Check for existing patient IDs in the database
existing_ids_query = f"SELECT {identity_column} FROM {table_name}"
existing_ids = pd.read_sql(existing_ids_query, engine)

# Filter the DataFrame to exclude existing patient IDs
new_data = MEdAX_df[~MEdAX_df[identity_column].isin(existing_ids[identity_column])]

# Write the filtered DataFrame to the database table
new_data.to_sql(table_name, engine, index=False, if_exists='append')
