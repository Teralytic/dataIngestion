import psycopg2 as pg
import csv

conn = pg.connect(database="global_soils", user="soil_admin", password="4MnFcgLYLH", host="teralytic-dev-cluster2.cluster-ctbamupmugqj.us-east-1.rds.amazonaws.com", port="5432")

print("Global Soils DB Connected")

cur = conn.cursor()

###WRITING DATA#############

with open('calibration-data.csv', 'r') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
		# skip row if data missing from cell 
		if not row['ID'] or not row['time'] or not row['devEUI'] or not row['deviceName'] or not row['in6'] or not row['in18'] or not row['in36'] or not row['moist_open_6'] or not row['moist_open_18'] or not row['moist_open_36'] or not row['moist_sub_6'] or not row['moist_sub_18'] or not row['moist_sub_36']:
			print("Missing a cell, skipping row...")
			continue
		else:	
			ID = row['ID']
			time = row['time']
			devEUI = row['devEUI']
			deviceName = row['deviceName']
			in6 = row['in6']
			in18 = row['in18']
			in36 = row['in36']
			moist_open_6 = row['moist_open_6']
			moist_open_18 = row['moist_open_18']
			moist_open_36 = row['moist_open_36']
			moist_sub_6 = row['moist_sub_6']
			moist_sub_18 = row['moist_sub_18']
			moist_sub_36 = row['moist_sub_36']
			
		# create sql table if it doesn't exist yet
		sql = """CREATE TABLE IF NOT EXISTS microart_offsets
			(id text PRIMARY KEY,
			time double precision,
			deveui text,
			devicename text,
			in6 text,
			in18 text,
			in36 text,
			moist_open_6 double precision,
			moist_open_18 double precision,
			moist_open_36 double precision,
			moist_sub_6 double precision,
			moist_sub_18 double precision,
			moist_sub_36 double precision);"""

		cur.execute(sql)
		conn.commit()
		
		### check if uuid already exists 
		####
		SQL = "SELECT EXISTS(SELECT * FROM microart_offsets WHERE id="+"'"+str(ID)+"'"+")"
		cur.execute(SQL)
		offsetExists = cur.fetchall()
		offsetExists = [item for items in offsetExists for item in items]
		offsetExists = offsetExists[0]
		#print(offsetExists)
		# skip header 
		if offsetExists == False:
			SQL = "INSERT INTO microart_offsets (id, time, deveui, devicename, in6, in18, in36, moist_open_6, moist_open_18, moist_open_36, moist_sub_6, moist_sub_18, moist_sub_36) VALUES("+"'"+str(ID)+"'"+","+time+","+"'"+str(devEUI)+"'"+","+"'"+str(deviceName)+"'"+","+"'"+str(in6)+"'"+","+"'"+str(in18)+"'"+","+"'"+str(in36)+"'"+","+moist_open_6+","+moist_open_18+","+moist_open_36+","+moist_sub_6+","+moist_sub_18+","+moist_sub_36+")"
			cur.execute(SQL)
			conn.commit()
			print("New offsets added into microart_offsets.")
			
cur.close()
conn.close()

print("Calibration ingestion complete.")





