from trafficreader import TrafficReader
from datetime import *
import csv
import numpy as np
import scipy.stats.stats as sci

#This program calculates the percentage of flow in the HOT lane versus the GP lanes

#Convert into Hours, Minutes and Seconds from Seconds
def GetInHMS(seconds):
    hours = seconds / 3600
    seconds -= 3600*hours
    minutes = seconds / 60
    seconds -= 60*minutes
    if hours == 0:
        return "%02d:%02d" % (minutes, seconds)
    return "%02d:%02d:%02d" % (hours, minutes, seconds)


#Read in Detector CSV Filer
detector_info = []
with open('394WBdetectors.csv', 'rU') as csvfile:
    d = csv.DictReader(csvfile)
    for row in d:
        detector_info.append(row)

#Location of Traffic Files
location = '../../traffic/'

#Hours (120 = 60 minutes/hour x 2 thirty second intervals/min)
beginning_time = 14 * 120
end_time = 19 * 120

#Start Date
date_1=date(2011,1,1)

#Read in detectors from csv into arrays
gp_detectors = [[0 for x in xrange(0)] for x in xrange(2)]
hot_detectors = [[0 for x in xrange(0)] for x in xrange(2)]

n=0
while n < len(detector_info):
    group = int(detector_info[n].get('Group'))
    
    if (detector_info[n].get('GP/HOT') == "GP"):
        gp_detectors[group].append(detector_info[n].get('Detector')[1:])
    else:
        hot_detectors[group].append(detector_info[n].get('Detector')[1:])
    n+=1

volumes_hot_detector = []
volumes_gp_detector = []

hot_total_volume = []
gp_total_volume = []

#Write titles for output file
with open('volume 394.csv', 'wb') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
    csv_writer.writerow(['Time','Average HOT Volume', 'HOT Std Dev','Average GP Volume',
                         'GP Std Dev','HOT Lane %'])

#Time initialization
times = []
j=beginning_time
for j in range(beginning_time,end_time):
    times.append(GetInHMS(j*30))

#Raw data files
with open('volume all 394 hot data.csv', 'wb') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
    csv_writer.writerow(times)

with open('volume all 394 gp data.csv', 'wb') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
    csv_writer.writerow(times)

good=0
bad=0
#End date
while (date_1 <= date(2012,12,31)):
    #Various date constraints including only weekdays. 
    if (date_1.weekday() < 5 and date_1!=date(2011,11,25)):

        #Read in loop detector data
        tr = TrafficReader()
        tr.loadfile(location + date_1.strftime('%Y%m%d') + '.traffic')
        
        volumes_hot_detector = []
        volumes_gp_detector = []

        #Number of detector groups (= 1 for disaggregate)
        g = 1
        while g <= 1:

            #Loop through detectors in parallel
            m=0
            while m < len(hot_detectors[g]):
                volumes_hot_detector.append(tr.volumes_for_detector(int(hot_detectors[g][m])))
                m+=1
            
            j=0
            while j < len(gp_detectors[g]):
                volumes_gp_detector.append(tr.volumes_for_detector(int(gp_detectors[g][j])))
                j+=1
                
            g+=1

        #Add up volumes of all detectors in parallel
        hot = np.nansum(volumes_hot_detector,axis=0)
        gp = np.nansum(volumes_gp_detector,axis=0)

        #Write out raw data
        with open('volume all 394 hot data.csv', 'ab') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
            csv_writer.writerow(hot[beginning_time:end_time])

        with open('volume all 394 gp data.csv', 'ab') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
            csv_writer.writerow(gp[beginning_time:end_time])

        #Append data    
        hot_total_volume.append(hot[beginning_time:end_time])
        gp_total_volume.append(gp[beginning_time:end_time])

    date_1 += timedelta(days=1)


#Average volume at each time across all days
average_hot_volume = sci.nanmean(hot_total_volume,axis=0)
variance_hot_volume = sci.nanstd(hot_total_volume,axis=0)
average_gp_volume = sci.nanmean(gp_total_volume,axis=0)
variance_gp_volume = sci.nanstd(gp_total_volume,axis=0)

#Group into 3 minute increments (6 x 30 seconds)
k=0
resolution = 6
while k in range(0,len(average_hot_volume)):
    average_hot_volume[k:k+resolution] = sum(average_hot_volume[k:k+resolution])/resolution
    variance_hot_volume[k:k+resolution] = sum(variance_hot_volume[k:k+resolution])/resolution
    average_gp_volume[k:k+resolution] = sum(average_gp_volume[k:k+resolution])/resolution
    variance_gp_volume[k:k+resolution] = sum(variance_gp_volume[k:k+resolution])/resolution
    k+=resolution

#Write out averaged 3 minute data
with open('volume 394.csv', 'ab') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
    i=0
    while i in range(0,len(average_hot_volume)):
        csv_writer.writerow([times[i],average_hot_volume[i],variance_hot_volume[i],average_gp_volume[i],
                            variance_gp_volume[i],average_hot_volume[i]/
                             (average_hot_volume[i]+average_gp_volume[i])*100])
        i+=resolution





