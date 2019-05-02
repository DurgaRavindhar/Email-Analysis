#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: DURGA RAVINDHAR
DATE:APR 19 2019
PURPOSE: Query the enron emaillog dimensional model to find insights    
"""
import pymysql
import os
import csv
import pandas as pd


class EmaillogInsights(object):
  def __init__(self):
    self.path=os.getcwd()
    self.config_file=self.path+'/enron_load.ini'
 
  def read_config(self):
      with open(self.config_file,"r+") as configfile:
        config=csv.reader(configfile,delimiter=':')
        for line in config:
            if line[0]=="mysql_password":
               self.sql_pwd=line[1]   
            if line[0]=="mysql_host":
               self.sql_host=line[1]
            if line[0]=="mysql_db":
               self.sql_db=line[1]
            if line[0]=="mysql_user":
               self.sql_user=line[1] 
      self.conn=pymysql.connect(self.sql_host,self.sql_user,self.sql_pwd)

  def recipient_with_more_emails(self):
    try: 
      print("1. Who received the most emails on which day? Please list the top 20 sorted by volume")   
      print("**********************************************************************************")
      mycursor = self.conn.cursor()
      sel_sql="select recp.email_address as recipient_email_address,         date(dt.log_timestamp) as log_timestamp,         count(*) as email_count  from  enron_db.fct_email_logs fct  left join enron_db.dim_date dt on(dt.log_timestamp_key=fct.log_timestamp_key)  left join  enron_db.dim_email_address_book recp on(recp.email_address_key=fct.recipient_email_address_key)  where recp.email_address_key<>-1  group by date(dt.log_timestamp),           recp.email_address  order by 3 desc  limit 20"
      mycursor.execute(sel_sql)
      raw_emaillist=mycursor.fetchall()
      print(pd.DataFrame(list(raw_emaillist),columns=['Email Address','Date','Count']) )    
          
    except pymysql.Error as e:
       print("error in function recipient_with_more_emails"+str(e))   
    finally:        
      self.conn.commit()
      mycursor.close() 
      
  def high_broadcasts_and_directs(self):
    try:  
      print("2.a Let's label an email as ""direct"" if there is exactly one recipient and ""broadcast"" if it has multiple recipients. Identify the top five (5) people who received the largest number of direct emails")
      print("************************************************************************************************************************************************************************************************************")
      mycursor = self.conn.cursor()
      sel_sql="select recp.email_address,count(*) as emails_received  from  enron_db.fct_email_logs fct  left join enron_db.dim_email_address_book recp  on(recp.email_address_key=fct.recipient_email_address_key)  where fct.broadcast_ind=0  and recipient_email_address_key<>-1  group by recp.email_address  order by 2 desc  limit 5;"
      mycursor.execute(sel_sql)
      raw_emaillist=mycursor.fetchall()
      print(pd.DataFrame(list(raw_emaillist),columns=['Recipient Email Address','Direct Email Count'])) 
      print("2.b Let's label an email as ""direct"" if there is exactly one recipient and ""broadcast"" if it has multiple recipients. Identify the top five (5) people who sent the largest number of broadcast emails")
      print("************************************************************************************************************************************************************************************************************")
      sel_sql="select send.email_address,count(*) as emails_sent  from  enron_db.fct_email_logs fct  left join enron_db.dim_email_address_book send  on(send.email_address_key=fct.sender_email_address_key)  where fct.broadcast_ind=1  and sender_email_address_key<>-1  group by send.email_address  order by 2 desc  limit 5;  "
      mycursor.execute(sel_sql)
      raw_emaillist=mycursor.fetchall()
      print(pd.DataFrame(list(raw_emaillist),columns=['Sender Email Address','Broadcast Email Count'])) 
    except pymysql.Error as e:
       print("error in function high_broadcasts_and_directs"+str(e))   
    finally:        
      self.conn.commit()
      mycursor.close() 

  def fast_response(self):
    try: 
      print("3. Find the five (5) emails with the fastest response times (In Seconds). ")   
      print("**********************************************************************************")
      mycursor = self.conn.cursor()
      sel_sql="select dt.log_timestamp,em.message_id,f_addr.email_address,r_addr.email_address,response_time from enron_db.fct_email_logs f left join enron_db.dim_date dt on(dt.log_timestamp_key=f.log_timestamp_key) left join enron_db.dim_email_address_book f_addr on(f.sender_email_address_key=f_addr.email_address_key) left join enron_db.dim_email_address_book r_addr on(f.sender_email_address_key=r_addr.email_address_key) left join enron_db.dim_email_message em on(f.email_message_key=em.email_message_key) where response_time is not null order by response_time asc limit 5;"
      mycursor.execute(sel_sql)
      raw_emaillist=mycursor.fetchall()
      pd.set_option('display.max_columns',5)
      pd.set_option('display.width',1000)
      print(pd.DataFrame(list(raw_emaillist),columns=['Response Email Time Stamp','Response Message Id','Sender Email Address','Recipient Email Address','Response time(sec)'])) 
    except pymysql.Error as e:
       print("error in function fast_response"+str(e))   
    finally:        
      self.conn.commit()
      mycursor.close() 
      
      
if __name__ == "__main__":
    
    insght=EmaillogInsights()
    insght.read_config()
    insght.recipient_with_more_emails()
    insght.high_broadcasts_and_directs()    
    insght.fast_response()