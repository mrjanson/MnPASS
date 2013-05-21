from trafficreader import TrafficReader
from datetime import *
import csv
import numpy as np
import psycopg2

#This program finds the number of paying SOV users vs total MnPASS lane users

#Convert into Hours, Minutes and Seconds from Seconds
def GetInHMS(seconds):
    hours = seconds / 3600
    seconds -= 3600*hours
    minutes = seconds / 60
    seconds -= 60*minutes
    if hours == 0:
        return "%02d:%02d" % (minutes, seconds)
    return "%02d:%02d:%02d" % (hours, minutes, seconds)

#Connect with database
conn = psycopg2.connect("dbname=MnPASS host='134.84.148.233' user=postgres") 
cur = conn.cursor()

#Read in Detector CSV Filer
detector_info = []
with open('394EBhovdetectors.csv', 'rU') as csvfile:
    d = csv.DictReader(csvfile)
    for row in d:
        detector_info.append(row)

#Location of Traffic Files
location = '../../traffic/'

#Hours (120 = 60 minutes/hour x 2 thirty second intervals/min)
beginning_time = 6 * 120
end_time = 10 * 120

#Start Date
date_1=date(2011,10,3)
#End Date
date_2=date(2011,10,28)

#Read in detectors from csv into arrays
gp_detectors = [[0 for x in xrange(0)] for x in xrange(2)]
hot_detectors = [[0 for x in xrange(0)] for x in xrange(2)]

n=0
while n < len(detector_info):
    group = int(detector_info[n].get('Group'))
    
    if (detector_info[n].get('GP/HOT') == "HOT"):
        hot_detectors[group].append(detector_info[n].get('Detector')[1:])
    n+=1

#Time initialization
times = []
secondtimes = []
j=beginning_time
for j in range(beginning_time/6,end_time/6):
    secondtimes.append(j*180)
    times.append(GetInHMS(j*180))

secondtimes.append((j+1)*180)

#Raw data files
with open('volume all 394 hot data.csv', 'wb') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
    csv_writer.writerow(times)


good=0
bad=0
g = 1
#Loop over number of detector groups (=1 for disaggregate)
while g <= 1:
    
    volumes_gp_detector = []
    hot_volumes = []
    date_change = date_1
    n=0

    while date_change <= date_2:
        if (date_change.weekday() < 5):
            
            volumes_hot_detector = []

            #Read in loop detecor data  
            tr = TrafficReader()
            tr.loadfile(location +date_change.strftime('%Y%m%d') + '.traffic')

            #Loop through detectors in parallel (usually just 1 for HOT lane)
            m=0
            while m < len(hot_detectors[g]):
                volumes_hot_detector.append(tr.volumes_for_detector(525))
                m+=1

            #Add up volumes of all detectors in parallel and add to array    
            hot_volumes.append(np.nansum(volumes_hot_detector, axis=0))

        n+=1
        
        date_change+=timedelta(days=1)

    #Add volumes for all days at each time
    hot = np.nansum(hot_volumes, axis=0)

    #Group volume by 3 minute intervals
    k=beginning_time
    hot_volume = []
    while k < end_time:
        hot_volume.append(sum(hot[k:k+6]))
        k+=6

   #Write data to file
    with open('volume all 394 hot data.csv', 'ab') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
        csv_writer.writerow(hot_volume)

    g+=1

start_date = date_1
end_date = date_2


#Retrieve paid SOV trips from database for specified dates and corridor
cur.execute("""Select start_plaza, time_stamp, price, end_plaza from trip_records where
extract(year from time_stamp)=2011 and ((extract(month from time_stamp)=10 and
extract(day from time_stamp)>=3) and (extract(month from time_stamp)=10 and
extract(day from time_stamp)<=28) and extract(dow from time_stamp)<6 and
extract(hour from time_stamp)>=6 and
extract(hour from time_stamp)<10 and start_plaza < 2000""")

trips = cur.fetchall()

n=0
target_time_seconds = []
while n < len(trips):
   target_time_seconds.append(trips[n][1].hour * 3600 + trips[n][1].minute * 60 + trips[n][1].second)
   n+=1


#Loop through trips and determine which trips used which plazas
n=0
sov_volumes_1 = []
sov_volumes_2 = []
sov_volumes_3 = []
sov_volumes_4 = []
sov_volumes_5 = []
while n < len(secondtimes)-1:
    j=0
    count_1=0
    count_2=0
    count_3=0
    count_4=0
    count_5=0
    while j < len(target_time_seconds):
        if (target_time_seconds[j] >= secondtimes[n] and target_time_seconds[j] < secondtimes[n+1]):
            if (trips[j][0]==1001):
                count_1 += 1
                if (trips[j][3]==1005):
                    count_5 += 1
                    count_4 += 1
                    count_3 += 1
                    count_2 += 1
                elif (trips[j][3]==1004):
                    count_4 += 1
                    count_3 += 1
                    count_2 += 1
                elif (trips[j][3]==1003):
                    count_3 += 1
                    count_2 += 1
                elif (trips[j][3]==1002):
                    count_2 += 1
            elif (trips[j][0]==1002):
                count_2 += 1
                if (trips[j][3]==1005):
                    count_5 += 1
                    count_4 += 1
                    count_3 += 1
                elif (trips[j][3]==1004):
                    count_4 += 1
                    count_3 += 1
                elif (trips[j][3]==1003):
                    count_3 += 1
            elif (trips[j][0]==1003):
                count_3 += 1
                if (trips[j][3]==1005):
                    count_5 += 1
                    count_4 += 1
                elif (trips[j][3]==1004):
                    count_4 += 1
            elif (trips[j][0]==1004):
                count_4 += 1
                if (trips[j][3]==1005):
                    count_5 += 1
            elif (trips[j][0]==1005):
                count_5 += 1
        j+=1
    sov_volumes_1.append(count_1)
    sov_volumes_2.append(count_2)
    sov_volumes_3.append(count_3)
    sov_volumes_4.append(count_4)
    sov_volumes_5.append(count_5)
    n+=1

#Write trips from one or more plazas (plaza 1003 in this case)
with open('volume all 394 hot data.csv', 'ab') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
    csv_writer.writerow('sov')
    csv_writer.writerow(sov_volumes_3)




