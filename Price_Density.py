import psycopg2
from datetime import *
import matplotlib
import matplotlib.pyplot as plt
from pylab import figure
import csv
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

#This program retrieves prices and densities form the database and formats into desired time
#intervals in CSV file

#Connect to database
conn = psycopg2.connect("dbname=MnPASS host='134.84.148.233' user=postgres")
cur = conn.cursor()

all_pricing_data = []

#Retrieve records from database
cur.execute("""SELECT rev_sec_rate,traffic_density,plaza,updated_on,dia_sec_rate from price_records
            where extract(hour from updated_on) <10 and extract(hour from updated_on) >=6 and plaza =1003
            and (extract(year from updated_on)=2011 or extract(year from updated_on)=2012) order by
            extract(hour from updated_on),extract(minute from updated_on)""")

all_pricing_data = cur.fetchall()


all_plaza = []
all_date_time = []
all_rate = []
all_density = []
all_times = []
all_rate_2 = []

i=0
while i < len(all_pricing_data):
    
    all_plaza.append(all_pricing_data[i][2])
    all_date_time.append(all_pricing_data[i][3])
    all_rate.append(all_pricing_data[i][0])
    all_density.append(all_pricing_data[i][1])
    all_rate_2.append(all_pricing_data[i][4])
    i+=1

k=0
while k < len(all_rate):
    if (all_rate[k] + all_rate_2[k]>8):
        all_rate[k]=8
        all_rate_2[k]=0
        print '8'
    k+=1

times = []
prices = []
price_std_dev = []
prices_2 = []
price_std_dev_2 = []
density = []
density_std_dev = []
#Minute Intervals
resolution = 3

#Convert to seconds and break into 3 minute intervals
j=0
while j < len(all_date_time):
    k=j
    while (k< len(all_date_time) and (all_date_time[k].hour*100 + all_date_time[k].minute)
           < (all_date_time[j].hour*100 + all_date_time[j].minute + resolution)):
        k+=1

    prices.append(np.mean(all_rate[j:k]))
    price_std_dev.append(np.std(all_rate[j:k]))
    prices_2.append(np.mean(all_rate_2[j:k]))
    price_std_dev_2.append(np.std(all_rate_2[j:k]))
    times.append(all_date_time[j])
    density.append(np.mean(all_density[j:k]))
    density_std_dev.append(np.std(all_density[j:k]))
    j=k

#Write data to CSV file
with open('Price_Density_394_Morning.csv', 'wb') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=',',quotechar='|')
    csv_writer.writerow(['Time','Price','Std Dev Price','Density','Density Std Dev'])
    for i in range(0,len(times)):
        csv_writer.writerow([times[i].time(),prices[i],price_std_dev[i],prices_2[i],price_std_dev_2[i],
                             density[i],density_std_dev[i]])






