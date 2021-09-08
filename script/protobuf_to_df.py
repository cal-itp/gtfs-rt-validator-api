from datetime import datetime
from google.transit import gtfs_realtime_pb2
import urllib
import urllib.request
import requests
import pandas as pd

def get_stop_states(entity, timestamp):
    """ Returns all stop time updates as a df to be appended to the entire one
    """
    stop_time_updates = pd.DataFrame()
    for stu in entity.trip_update.stop_time_update:
        entity_id       = entity.id
        trip_id         = entity.trip_update.trip.trip_id
        sched_relship   = entity.trip_update.trip.schedule_relationship
        arrival_delay   = stu.arrival.delay
        arrival_time    = stu.arrival.time
        arrival_uncert  = stu.arrival.uncertainty
        departure_delay = stu.departure.delay
        departure_time  = stu.departure.time
        depart_uncert   = stu.departure.uncertainty
        stop_id         = stu.stop_id
        route_id        = entity.trip_update.trip.route_id
        stop_time_updates = stop_time_updates.append(
            {   'server_timestamp':datetime.fromtimestamp(timestamp),
                'entity_id':str(entity_id),
                'trip_id':int(trip_id),
                'route_id': str(route_id),
                'arrival_delay':int(arrival_delay),
                'arrival_uncertainty': int(arrival_uncert),
                'arrival_time':datetime.fromtimestamp(arrival_time),
                'departure_delay':int(departure_delay),
                'departure_time':datetime.fromtimestamp(departure_time),
                'departure_uncertainty': int(depart_uncert),
                'stop_id':str(stop_id),
                'schedule_relationsip': str(sched_relship)
            }, ignore_index=True)

    return stop_time_updates

def get_rt_Gtfs_TripUpdates(url = None, fname = None):
    """ Access the GTFS-RT at the url and return them as a dataframe.
    """

    if fname:
        content = open(fname, "rb").read()
    else:
        response = requests.get(url, allow_redirects=True)
        content = response.content
        
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)

    timestamp = feed.header.timestamp
    tripupdate = pd.DataFrame()

    for entity in feed.entity:
        tripupdate = tripupdate.append(get_stop_states(entity, timestamp))

    return tripupdate
