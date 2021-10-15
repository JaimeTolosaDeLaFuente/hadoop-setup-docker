#!/usr/bin/python
# Originally based on https://github.infra.cloudera.com/EDU/bda-vm/blob/master/maintenance/datacreation/writedevicestatus.py

import peewee
from peewee import *
import datetime
import os
import random
#from common import *
from dateutil.relativedelta import relativedelta
import sys

if len(sys.argv) != 2:
	quit('Specify the base output directory with a trailing slash')

outputdirectory = sys.argv[1]

if not os.path.exists(outputdirectory):
	os.mkdir(outputdirectory)

startingDate = datetime.datetime(2018, 3, 15, 10, 10, 20)

devicePatterns = {
	'Sorrento' : '{time},{name},{deviceid},{devicetemp},{ambienttemp},{batterypercent},{gpsstatus},{bluetoothstatus},{wifistatus},{signalpercent},{cpuload},{ramusage},{latitude},{longitude}\n',
	'iFruit' : '{time},{name},{deviceid},{devicetemp},{ambienttemp},{batterypercent},{gpsstatus},{bluetoothstatus},{wifistatus},{signalpercent},{cpuload},{ramusage},{latitude},{longitude}\n',
	'Ronin' : '{time},{name},{deviceid},{devicetemp},{ambienttemp},{batterypercent},{gpsstatus},{bluetoothstatus},{wifistatus},{signalpercent},{cpuload},{ramusage},{latitude},{longitude}\n',
	'MeeToo' : '{time},{name},{deviceid},{devicetemp},{ambienttemp},{batterypercent},{gpsstatus},{bluetoothstatus},{wifistatus},{signalpercent},{cpuload},{ramusage},{latitude},{longitude}\n',
	'Titanic' : '{time},{name},{deviceid},{devicetemp},{ambienttemp},{batterypercent},{gpsstatus},{bluetoothstatus},{wifistatus},{signalpercent},{cpuload},{ramusage},{latitude},{longitude}\n',
}

def createRowString(currentTimeString, pattern, name, deviceid, zipcode):
	ambienttemp = random.randrange(10,35)

	if name == "Sorrento F41L":
		# battery_level: % typical range: 20.0 - 80.0; values for Sorrento F41L phone should drop more quickly and be much lower on average)
		batterypercent = str(random.randrange(20, 75))

		# cpu_usage: % (typical range: 10% - 70%); F41L phone should be ~ 20% higher than for any other model.
		cpuload = str(random.randrange(40,100))
		
		# device_temp: celsius (typical range: ambient_temp times ~ 1.07; Sorrento F41L phone should be ambient_temp times ~ 1.4)
		devicetemp = int(float(ambienttemp) * random.uniform(1.2, 1.4))
	else:
		batterypercent = str(random.randrange(20, 100))

		# cpu_usage: % (typical range: 10% - 70%); F41L phone should be ~ 20% higher than for any other model.
		cpuload = str(random.randrange(0,70))
		
		# device_temp: celsius (typical range: ambient_temp times ~ 1.07; Sorrento F41L phone should be ambient_temp times ~ 1.4)
		devicetemp = int(float(ambienttemp) * random.uniform(1.0, 1.07))

	signalpercent = str(random.randrange(30,100))
	ramusage = str(random.randrange(0,70))

	# Make the device temp a delta
	devicetemp = devicetemp - ambienttemp

	# WiFi status: one of [disabled / enabled / connected]; should always be disabled for 10% of devices, and for the other 90% of devices, should be disabled 1% of the time.
	# If not disabled, there should be a 40% chance that the state is connected at any given moment.
	if random.randrange(0,9) == 0:
		wifistatus = "disabled"
	else:
		randomChoice = random.randrange(0, 99)

		if randomChoice < 1:
			wifistatus = "disabled"
		elif randomChoice < 40:
			wifistatus = "connected"
		else:
			wifistatus = "enabled"

	# Bluetooth status: one of [disabled / enabled / connected].  Should always be disabled for 20% of devices, and for the other 80% of devices, should be disabled 1% of the time.
	# If not disabled, there should be a 25% chance that the state is connected at any given moment.
	if random.randrange(0,9) < 2:
		bluetoothstatus = "disabled"
	else:
		randomChoice = random.randrange(0, 99)

		if randomChoice < 1:
			bluetoothstatus = "disabled"
		elif randomChoice < 25:
			bluetoothstatus = "connected"
		else:
			bluetoothstatus = "enabled"
	
	# GPS status: one of [disabled / enabled ]; should always be disabled for 5% of devices, and for the other 95% of devices, should be disabled 1% of the time.
	if random.randrange(0,99) < 5:
		gpsstatus = "disabled"
	else:
		randomChoice = random.randrange(0, 99)

		if randomChoice < 1:
			gpsstatus = "disabled"
		else:
			gpsstatus = "enabled"

	latitude = 0
	longitude = 0

	# Randomize the GPS location based on the account's zip code
	if gpsstatus == "enabled":
		zipbase = zipcode[:3]
		zipbaseAsInt = int(zipbase)

		if zipbase in zipCodeToLatLong:
			latitude, longitude = zipCodeToLatLong[zipbase]
		# Try one zipcode higher
		elif str(zipbaseAsInt + 1) in zipCodeToLatLong:
			latitude, longitude = zipCodeToLatLong[str(zipbaseAsInt + 1)]
		# Try one zipcode lower
		elif str(zipbaseAsInt - 1) in zipCodeToLatLong:
			latitude, longitude = zipCodeToLatLong[str(zipbaseAsInt - 1)]
		else:
			print "Couldn't find zipcode for " + zipbase

		latitude = float(latitude) + random.uniform(0.0, 0.5)
		longitude = float(longitude) + random.uniform(0.0, 0.5)

	return pattern.format(time=currentTimeString,name=name,ambienttemp=ambienttemp,batterypercent=batterypercent,signalpercent=signalpercent,cpuload=cpuload,deviceid=deviceid,devicetemp=devicetemp,
		ramusage=ramusage,latitude=latitude,longitude=longitude,wifistatus=wifistatus,bluetoothstatus=bluetoothstatus,gpsstatus=gpsstatus)

numberOfDevices = 0
numberOfRows = 0

def createRows(accountDevices):
	global startingDate, numberOfDevices, numberOfRows

	i = 0
	currentTimeString = startingDate.strftime('%Y-%m-%d:%H:%M:%S')

	for accountDevice in accountDevices:
		devicePattern = None
		
		deviceManufacturer = accountDevice.device.device_name.partition(' ')[0]
		devicePattern = devicePatterns[deviceManufacturer]

		if devicePattern is None:
			print "Device pattern not found for " + accountDevice.device.device_name

		row = createRowString(currentTimeString, devicePattern, accountDevice.device.device_name, accountDevice.account_device_id, accountDevice.account.zipcode)

		devicestatusfile.write(row)

		i += 1

		if i % UPDATES_PER_SECOND == 0:
			startingDate = startingDate + datetime.timedelta(seconds=1)
			currentTimeString = startingDate.strftime('%Y-%m-%d:%H:%M:%S')

		numberOfDevices += 1
		numberOfRows += 1

		if numberOfRows % 100000 == 0:
			print "Output " + str(numberOfRows) + " rows so far for time " + startingDate.strftime('%Y-%m-%d:%H:%M:%S')

zipCodeToLatLong = {}

def readBaseStations():
	#BaseStation.drop_table(fail_silently=True)
	#BaseStation.create_table()

	devicestatusfile = open(outputdirectory + "base_stations.tsv", "r")
	
	for line in devicestatusfile:
		station_num, zipcode, city, state, latitude, longitude = line.strip().split('\t')

		zipcodeBase = zipcode[:3]

		zipCodeToLatLong.update({zipcodeBase : [latitude, longitude]})

		station_num = int(station_num)
		latitude = float(latitude)
		longitude = float(longitude)

		baseStation = BaseStation(station_num=station_num,zipcode=zipcode,city=city,state=state,latitude=latitude,longitude=longitude)
		baseStation.save(force_insert=True)

	devicestatusfile.close()

#MostActiveStation.drop_table(fail_silently=True)
#MostActiveStation.create_table()

print "Writing out base stations"

readBaseStations()

print "Writing out device status"

devicestatusfile = open(outputdirectory + "devicestatus.txt", "w")

accountDevices = AccountDevice.select().join(Account).where(
		(Account.acct_close_dt >> None) &
		((fn.Mod(Account.acct_num, 100) == 99) | (fn.Mod(Account.acct_num, 100) == 98) | (Account.acct_num == demoAccount))
	).order_by(fn.Rand())

for x in range(0, (60 * 3)):
	numberOfDevices = 0

	createRows(accountDevices)

devicestatusfile.close()

print "Created " + str(numberOfRows) + " for " + str(numberOfDevices) + " devices"