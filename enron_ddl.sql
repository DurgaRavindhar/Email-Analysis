
#Author: Durga Ravindhar
#File:enron_ddl.sql
#Purpose: DDL scripts for enron_email analysis. Please run this before running load.py

drop database enron_db;

create database enron_db;

create table enron_db.stg_email_log
(
message_id varchar(500) PRIMARY KEY,
log_timestamp varchar(100),
from_address TEXT,
to_address TEXT,
cc_address TEXT,
bcc_address TEXT,
reply_to TEXT,
sender TEXT,
email_subject TEXT,
file_name varchar(500),
mime_type varchar(100),
disposition_type varchar(100),
email_subject_prefix varchar(10),
original_email_subject TEXT
);

create table enron_db.stg_pivot_recpt_emaillist
(
message_id varchar(500),
recipient_email_address varchar(100)
);

CREATE TABLE enron_db.dim_date (
    log_timestamp_key int AUTO_INCREMENT PRIMARY KEY,
    log_timestamp timestamp,
    etl_updated_at timestamp
);

insert into enron_db.dim_date values
(-1,null,current_date());

CREATE TABLE enron_db.dim_email_message (
    email_message_key int AUTO_INCREMENT PRIMARY KEY,
    message_id varchar(500),
    email_subject text,
    email_subject_prefix varchar(10),
    attached_file_name varchar(500),
    mime_type varchar(100),
    disposition_type varchar(100),
    etl_updated_at timestamp
);

insert into enron_db.dim_email_message values
(-1,null,null,null,null,null,null,current_date());


CREATE TABLE enron_db.dim_email_address_book (
    email_address_key int AUTO_INCREMENT PRIMARY KEY ,
    email_address varchar(500) ,
    user_name varchar(100) ,
    domain varchar(100) ,
    internal_flag int,
    etl_updated_at timestamp
);

insert into enron_db.dim_email_address_book values
(-1,null,null,null,null,current_date());


CREATE TABLE enron_db.fct_email_logs (
    log_timestamp_key int ,
    email_message_key int ,
    sender_email_address_key int ,
    recipient_email_address_key int ,
    response_ind int ,
    broadcast_ind int,
    response_time int,
    etl_updated_at timestamp,
    CONSTRAINT fct_email_logs_pk PRIMARY KEY (log_timestamp_key,email_message_key,sender_email_address_key,recipient_email_address_key),
    CONSTRAINT fct_email_logs_fk1 FOREIGN KEY (log_timestamp_key) REFERENCES enron_db.dim_date(log_timestamp_key),
    CONSTRAINT fct_email_logs_fk3 FOREIGN KEY (email_message_key) REFERENCES enron_db.dim_email_message(email_message_key),
    CONSTRAINT fct_email_logs_fk4 FOREIGN KEY (sender_email_address_key) REFERENCES enron_db.dim_email_address_book(email_address_key),
    CONSTRAINT fct_email_logs_fk5 FOREIGN KEY (recipient_email_address_key) REFERENCES enron_db.dim_email_address_book(email_address_key)
);
