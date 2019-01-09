import os
import sys

os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python2.7'

sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.9-src.zip')
os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages com.databricks:spark-csv_2.10:1.5.0 pyspark-shell')
os.environ['JAVA_TOOL_OPTIONS'] = "-Dhttps.protocols=TLSv1.2"

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

conf = SparkConf()
sc = SparkContext(conf= conf)
sql= HiveContext(sc)

stations = sql.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("CTA.csv")
stations.registerTempTable("Stations")

stops = sql.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("stops.csv")
stops.registerTempTable("Stops")

# station id = map_id in other  dataset


# print("All")
# all  = sql.sql("""SELECT * from Stops Join Stations on station_id = MAP_ID""")


print("New Years Day")
nyears =  sql.sql ("""SELECT * from Stops Join Stations ON station_id = MAP_ID WHERE date like '01/01/%' """)
#nye.coalesce(1).write.format("com.databricks.spark.csv").option('header','true').save("file:///home/cloudera/Desktop/nye.csv")

nye = sql.sql ("""SELECT * from Stops Join Stations ON station_id = MAP_ID WHERE date like '12/31/%' """)

print("4th of July")
independence = sql.sql ("""SELECT * from Stops Join Stations ON station_id = MAP_ID WHERE date like '07/04/%' """)

print("Xmas")
xmas = sql.sql ("""SELECT * from Stops Join Stations ON station_id = MAP_ID WHERE date like '12/25/%' """)

print("Weekday Totals")
weekday = sql.sql ("""SELECT * from Stops Join Stations ON station_id = MAP_ID WHERE daytype = 'W' """)

print("Saturday Totals")
saturday = sql.sql ("""SELECT * from Stops Join Stations ON station_id = MAP_ID WHERE daytype = 'A' """)

print("Top 20 Stations")
top_20_stations = sql.sql (""" SELECT stationname, SUM(rides) as rides_count from Stops Join Stations
                            ON station_id = MAP_ID GROUP BY stationname
                            Order by SUM(rides) DESC LIMIT 20 """)

print("Top 20 dates")
top_20_dates = sql.sql ("""SELECT date, SUM(rides) from Stops Join Stations
            ON station_id = MAP_ID GROUP BY date
            ORDER BY sum(rides) DESC LIMIT 20""")

print("ADA Station Totals")
ada = sql.sql ("""SELECT stationname, SUM(rides) as Rides from Stops Join Stations
            ON station_id = MAP_ID 
            WHERE ADA = false
            Group By stationname
            ORDER BY sum(rides) DESC LIMIT 20""")


print("Red line totals")
red = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE RED = true
            GROUP BY stationname """)

print("Blue line totals")
blue = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE BLUE = true
            GROUP BY stationname""")

print("Green line totals")
green = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE G = true
            GROUP BY stationname """)

print("Brown line totals")
brown = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE BRN = true
            GROUP BY stationname""")

print("Purple line totals")
purple = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE P = true
            GROUP BY stationname  """)

print("Purple line Express totals")
express = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE Pexp = true
            GROUP BY stationname """)

print("Yellow line totals")
yellow = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE Y = true
            GROUP BY stationname """)

print("Pink line totals")
pink = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE Pnk = true
            GROUP BY stationname""")

print("Orange line totals")
orange = sql.sql ("""SELECT stationname, SUM(rides) as Rides,AVG(rides) as average_rides from Stops Join Stations
            ON station_id = MAP_ID
            WHERE O = true
            GROUP BY stationname """)

print("ALL station totals")



all = sql.sql("""
            SELECT "Red" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE RED = true
            UNION ALL
            SELECT "Blue" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE BLUE = true
            UNION ALL
            SELECT "Green" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE G = true
            UNION ALL
            SELECT "Brown" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE BRN = true
            UNION ALL
            SELECT "Purple" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE P = true
            UNION ALL
            SELECT "Purple Express" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE Pexp = true
            UNION ALL
            SELECT "Yellow" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE Y = true
            UNION ALL
            SELECT "Pink" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE Pnk = true
            UNION ALL
            SELECT "Orange" as Line,SUM(rides) as Rides, AVG(rides) as average_rides FROM Stops Join Stations on station_id = MAP_ID
            WHERE O = true
             """)
results = [stations,stops,nyears,nye,independence,xmas,weekday,saturday,top_20_stations,top_20_dates,ada, red,blue,
           green, brown,purple,express,yellow,pink, orange,all]

results_names =  ["stations", "stops", "nyears","nye", "independence","xmas", "weekday", "saturday", "top_20_stations",
               "top_20_dates","ada", "red","blue", "green","brown", "purple", "express", "yellow", "pink", "orange", "all"]

for x in range(len(results)):

    results[x].coalesce(1).write.format("com.databricks.spark.csv").\
    option('header','true').save("file:///home/cloudera/Desktop/results/{0}.csv".format(results_names[x]))


# holiday totals -nye, memorial day, 4th, labor day, thanksgiving, xmas
# weekday totals
# top 20 dates
# top 20 stations
# ADA station totals
#totals for train lines
