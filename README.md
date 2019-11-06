# dataIngestion
Scripts for ingesting data into Teralytic DBs and Services

1) DOWNLOAD Manufactuing data

Access manufacturing jig data from Dropbox:
https://www.dropbox.com/home/Teralytic%20Team%20Folder/engineering/product%20-%20v3.2/v3.2%20manufacturing/mfg%20jig%20csv%20files

Stored and ordered by date in the Microart and Nextpcb directories


2) INGEST LoRa devices into LoRa Server API
To ingest lora modulesm, in command line run : 'python lora-module.py lora-modules.csv'

3) INGEST offset data from either Microart or Nextpcb directories into 'microart_offets' table in global_soils Postgres DB
with 'python offsets.py' 
