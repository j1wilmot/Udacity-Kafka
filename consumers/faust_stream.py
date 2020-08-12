"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Directions:
#
# 1. Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#    places it into a new topic with only the necessary information.
#
# 2. Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
#
# 3. Define the output Kafka Topic
#
# 4. Define a Faust Table

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.legacy_postgres.stations", value_type=Station, partitions=1)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
table = app.Table(
    "stations_table",
    partitions=1,
    changelog_topic=out_topic,
)

@app.agent(topic)
async def transform_stations(stations):
    async for station in stations:
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=get_line_color(station)
        )
        table[station.station_id] = transformed_station.asdict()

def get_line_color(station):
    if station.red is True:
        return 'red'
    elif station.blue is True:
        return 'blue'
    elif station.green is True:
        return 'green'

if __name__ == "__main__":
    app.main()
