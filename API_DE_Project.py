import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path = "/Volume/workspace/default/cricket_api_project"

API_KEY = 'dd4dd816-1fa4-4b6b-9457-06a18b611072'
api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"

response = request.get(api_url)
response.raise_for_status()

api_data = response.json()
print(api_data.keys())

print(json.dumps(api_data,indent = 2)[:2000]

raw_file_path = f"{base_path}/current_current_matches.raw.json"

with open(raw_file_path, 'w') as file:
        json.dump(api_data, file)

print("RAW API data is save at the : " raw_file_path)

#create bronze layer dataframe

bronze_data = [{
"source_api" : api_url,
"raw_json" : json.dumps(api_data), 
"ingestion_time" : None }]

bronze_schema = StructType([
StructField("source_api", StringType(), True),
StructField("raw_json", StringType(), True),
StructField("ingestion_time", TimestampType(), True)
])

bronze_df = spark.createDataFrame(bronze_data, bronze_schema)\
                      .withColumn("ingestion_time", current_timestamp())

display(bronze_df)

bronze_df.write.format('delta').mode('overwrite').saveAsTable("workspace.default.cricket_bronze_current_matches")

print("Bronze table create successfully")

#Silver

%sql
select * from workspace.default.cricket_bronze_current_matches

import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

bronze_df = spark.table('workspace.default.cricket_bronze_current_matches')

raw_json = bronze_df.select("raw_json").collect()[0]['raw_json']
api_data = json.loads(raw_json)

print("Total Mathces Found: ", len(matches))
print(matches[0] if len(matches) > 0 else "NO MATCHES FOUND")

silver_rows = [ ]

for match in matches:
      teams = match.get("team", [])
      score = match.get("score", [])

      team_1 = teams[0] if len(teams) > 0 else None
      team_2 = team[1] if len(teams) > 1 else None

      score_1 = None
      score_2 = None

      if len(score) > 0:
         score_1 = score[0]
         #Formating iut in real score display example 180/5 in 20 overs
          score_1 = f"{s1.get('r', 0)}/{s1.get('w', 0)} in {s1.get('o', 0)} overs"

       if len(score) > 1:
          s2 = score[1]
          score_2 = f"{s2.get('r', 0)}/{s2.get('w', 0)} in {s2.get('o', 0)} overs"

       silver_rows.append( {
            "match_id" : match.get("id"),
            "match_name" : match.get("name"),
            "match_type" : match.get("matchType"),
            "status" : match.get("status"),
            "venue" : match.get("venue"),
            "match_date" : match.get("date"),
            "date_time_gmt" : match.get("dateTimeGMT"),
            "team_1" : team_1,
            "team_2" : team_2,
            "score_1" : score_1,
            "score_2" : score_2,
            "match_started" : match.get("matchStarted"),
            "match_ended" : match.get("matchStarted") })

print(" Silver rows prepared: " len(silver_rows))

silver_schema = StructType([
      StructField("match_id" , StringType(), True),
      StructField("match_name",  StringType(), True),
      StructField("match_type",  StringType(), True),
      StructField("status",  StringType(), True),
      StructField("venue",  StringType(), True),
      StructField("match_date" ,  StringType(), True),
      StructField("date_time_gmt" ,  StringType(), True),
      StructField("team_1" ,  StringType(), True),
      StructField("team_2" ,  StringType(), True),
      StructField("score_1" , StringType(), True),
      StructField("score_2" ,  StringType(), True),
      StructField("match_started" ,  StringType(), True),
      StructField("match_ended" ,  StringType(), True) ])

silver_df = spark.createDataFrame(silver_rows, silver_schema)\
                  .withColumn("match_date", to_date(col("match_date")))\
                  .withColumn("loaded_at", current_timestamp())

display(silver_df)

silver_df.write\
       .format('delta')\
       .mode('overwrite')\
       .option("overwriteSchema", "true")\
       .saveAsTable('workspace.default.cricket_silver_current_matches')

print("Silver Table created Successfully !!!! ")

%sql
select * from workspace.default.cricket_silver_current_matches')

gold_match_type_df = silver_df.groupBy('match_type').agg(count('*').alias('Total_Matches'))
display(gold_match_type_df)

gold_venue_df = silver_df.groupBy('venue').agg(count('*').alias('Total_Matches'))
display(gold_venue_df)

team_1_df = silver_df.select(col("team_1").alias("team"))
team_2_df = silver_df.select(col("team_2").alias("team"))

all_teams_df = team_1_df.union(team_2_df)
gold_team_df = all_teams_df.groupBy('team').agg(count('*').alias("matches_played"))
display(gold_team_df)

%sql
display(spark.sql(" " "
select count(*) as total_matches, count(distinct match_type) as total_match_types, count(distinct venue) as total_venues from workspace.default.cricket_silver_current_matches" " ")






