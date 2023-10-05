import requests
import json
import pandas as pd
import time
import datetime as dt
from datetime import datetime
import numpy as np
import pickle
import sys
import operator
import polyline
import warnings
warnings.filterwarnings("ignore")

# API key of google maps project
YOUR_API_KEY = "YOUR_API_KEY" 

# defining names of files to be used
directory = "C:\\SampleDirectory"
waypoint_filename = "Waypoints_Sample"
junction_filename = "Junctions_Sample"
roads_pickle_filename = "Roads Sample Google Maps"
junctions_pickle_filename = "Junctions Sample Google Maps"
log_filename = "GoogleMaps Sample Log.txt"
error_log_filename = "GoogleMaps Sample Error Log.txt"
critical_log_filename = "GoogleMaps Sample Critical Log.txt"
request_counter = 0
request_counter_upto = 15456

# list of times during the day at which the requests will be sent
times = ["08:05:00", "08:35:00", "09:05:00", "09:35:00", "10:05:00", "10:35:00", "12:05:00", "14:05:00", "16:35:00", "17:35:00", "18:05:00", "18:35:00", "19:05:00", "20:05:00", "21:05:00", "22:05:00"]


# =============================================================================
# Methods
# =============================================================================

def get_roads_waypoints_list(waypoint_df):
    '''returns dictionary of waypoints that are to be sent as a part of the POST request for straights'''
    waypoints = list()
    for l in range(len(waypoint_df)):
        waypoints.append({
          "location":{
            "latLng":{
              "latitude": waypoint_df["loc_latitude"].iloc[l],
              "longitude": waypoint_df["loc_longitude"].iloc[l]
            },
          },
        }
        )
    return waypoints

def get_junctions_waypoints_list(waypoint_df):
    '''returns dictionary of waypoints that are to be sent as a part of the POST request for left and right turns'''
    waypoints = list()
    for l in range(len(waypoint_df)):
        waypoints.append({
          "location":{
            "latLng":{
              "latitude": 
                  waypoint_df["loc_latitude_from"].iloc[l],
              "longitude": 
                  waypoint_df["loc_longitude_from"].iloc[l]
            },
          },
        }
        )
        waypoints.append({
              "location":{
                "latLng":{
                  "latitude": 
                      waypoint_df["loc_latitude_to"].iloc[l],
                  "longitude": 
                      waypoint_df["loc_longitude_to"].iloc[l]
                },
              },
            }
            )
            
    return waypoints
  
def get_response(waypoints_list):
    '''sends the POST request and returns the response'''
    global request_counter
    intermediates = waypoints_list[1:-1]
    headers = {
           'Content-Type': 'application/json',
           'X-Goog-Api-Key': f'{YOUR_API_KEY}',
           'X-Goog-FieldMask': 'routes.duration,routes.staticDuration,routes.distanceMeters,routes.legs,routes.polyline.encodedPolyline,routes.travelAdvisory,routes.legs.travelAdvisory',
       }
       
    json_data = {
        'origin': waypoints_list[0],
        'destination': waypoints_list[-1],
        "intermediates": intermediates,
        'travelMode': 'DRIVE',
        "extraComputations": ["TRAFFIC_ON_POLYLINE"],
        'routingPreference': 'TRAFFIC_AWARE',
        'computeAlternativeRoutes': False,
        'routeModifiers': {
            'avoidTolls': False,
            'avoidHighways': False,
            'avoidFerries': False,
        },
        'languageCode': 'en-US',
        'units': 'METRIC',
    }
    response = requests.post('https://routes.googleapis.com/directions/v2:computeRoutes', headers=headers, json=json_data)
    request_counter+=1
    res = response.json()
    # storing response as a json string
    obj = json.dumps(res)
    return obj
def get_roads_df(waypoints_df,response,usertime,poly_line):
    '''returns a row that is to be appeneded to the Roads_Responses dataframe'''
    
    # converts response from json string to a dictionary
    data = json.loads(response)
    response_df = pd.DataFrame(columns = ["DateTime","User Time","Road","Travel Direction", 
                                      "Polyline", "start_lat", "start_lng", "end_lat",
                                      "end_lng", "Intermediates", "Distance", "Duration", "Response"],index = [0])

    
    response_df["DateTime"].loc[0] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    response_df["User Time"].loc[0] = usertime
    response_df["Road"].loc[0] = waypoints_df["loc_road"].iloc[i]
    response_df["Travel Direction"].loc[0] = waypoints_df["travel_direction"].iloc[i]
    response_df["Polyline"].loc[0] = poly_line
    response_df["start_lat"].loc[0] = waypoints_df["loc_latitude"].iloc[0]
    response_df["start_lng"].loc[0] = waypoints_df["loc_longitude"].iloc[0]
    response_df["end_lat"].loc[0] = waypoints_df["loc_latitude"].iloc[-1]
    response_df["end_lng"].loc[0] = waypoints_df["loc_longitude"].iloc[-1]
    
    intermediates_list = list()
    for a in range(len(waypoints_df)):
        intermediates_list.append({"lat":waypoints_df['loc_latitude'].iloc[a],"lng":waypoints_df['loc_longitude'].iloc[a],
                                   "junc name":waypoints_df['intersection_next_localname'].iloc[a],
                                   "junc no":waypoints_df['intersection_next_junction_no'].iloc[a]})
    
    #stores all the interemdiates or waypoints used in the request
    response_df["Intermediates"].loc[0] = intermediates_list
    response_df["Distance"].loc[0] = data['routes'][0]['distanceMeters']
    response_df["Duration"].loc[0] = data['routes'][0]['duration']
    response_df["Response"].loc[0] = response 
    
    return response_df

def get_error_roads_df(waypoints_df,response,usertime,poly_line):
    '''returns a row that is to be appeneded to the Roads_Responses dataframe incase there is an error in the POST request'''

    response_df = pd.DataFrame(columns = ["DateTime","User Time","Road","Travel Direction", 
                                      "Polyline", "start_lat", "start_lng", "end_lat",
                                      "end_lng", "Intermediates", "Distance", "Duration", "Response"],index = [0])


    response_df["DateTime"].loc[0] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    response_df["User Time"].loc[0] = usertime
    response_df["Road"].loc[0] = waypoints_df["loc_road"].iloc[i]
    response_df["Travel Direction"].loc[0] = waypoints_df["travel_direction"].iloc[i]
    response_df["Polyline"].loc[0] = poly_line
    response_df["start_lat"].loc[0] = waypoints_df["loc_latitude"].iloc[0]
    response_df["start_lng"].loc[0] = waypoints_df["loc_longitude"].iloc[0]
    response_df["end_lat"].loc[0] = waypoints_df["loc_latitude"].iloc[-1]
    response_df["end_lng"].loc[0] = waypoints_df["loc_longitude"].iloc[-1]
    
    intermediates_list = list()
    for a in range(len(waypoints_df)):
        intermediates_list.append({"lat":waypoints_df['loc_latitude'].iloc[a],"lng":waypoints_df['loc_longitude'].iloc[a],
                                   "junc name":waypoints_df['intersection_next_localname'].iloc[a],
                                   "junc no":waypoints_df['intersection_next_junction_no'].iloc[a]})
    
    response_df["Intermediates"].loc[0] = intermediates_list
    response_df["Distance"].loc[0] = np.nan
    response_df["Duration"].loc[0] = None
    response_df["Response"].loc[0] = response 
    
    return response_df

def get_junctions_df(waypoints_df, response, usertime, poly_line):
    '''returns a row that is to be appeneded to the Junctions_Responses dataframe'''

    # converts response from json string to a dictionary
    data = json.loads(response)
    response_df = pd.DataFrame(columns = ['DateTime', 'User Time', 'Junction Name', 'Junction no',
                                      'No of Turns', 'Polyline', 'start_lat', 'start_lng', 'end_lat',
                                      'end_lng', 'Intermediates', 'Distance', 'Duration', 'Response'],
                               index = [0])
    
    response_df["DateTime"].loc[0] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    response_df["User Time"].loc[0] = usertime
    response_df["Junction Name"].loc[0] = waypoints_df["intersection_next_localname"].iloc[0]
    response_df["Junction no"].loc[0] = waypoints_df["intersection_next_junction_no"].iloc[0]
    response_df["No of Turns"].loc[0] = len(waypoints_df)
    response_df["Polyline"].loc[0] = poly_line
    response_df["start_lat"].loc[0] = waypoints_df["loc_latitude_from"].iloc[0]
    response_df["start_lng"].loc[0] = waypoints_df["loc_longitude_from"].iloc[0]
    response_df["end_lat"].loc[0] = waypoints_df["loc_latitude_to"].iloc[-1]
    response_df["end_lng"].loc[0] = waypoints_df["loc_longitude_to"].iloc[-1]
    intermediates_list = []
    
    for i in range(len(waypoints_df)):
        intermediates_list.append(waypoints_df.iloc[i][['loc_latitude_from', 'loc_longitude_from',
           'loc_road_from', 'travel_direction_from', 'loc_description_from', 'loc_latitude_to', 'loc_longitude_to',
           'loc_road_to', 'travel_direction_to',
           'loc_description_to', 'Turn']].to_dict())
    
    #stores all the interemdiates or waypoints used in the request
    response_df["Intermediates"].loc[0] = intermediates_list
    response_df["Distance"].loc[0] = data['routes'][0]['distanceMeters']
    response_df["Duration"].loc[0] = data['routes'][0]['duration']
    response_df["Response"].loc[0] = response 
    return response_df

def get_error_junctions_df(waypoints_df, response, usertime, poly_line):
    '''returns a row that is to be appeneded to the Junctions_Responses dataframe incase there is an error in the POST request'''

    response_df = pd.DataFrame(columns = ['DateTime', 'User Time', 'Junction Name', 'Junction no',
                                      'No of Turns', 'Polyline', 'start_lat', 'start_lng', 'end_lat',
                                      'end_lng', 'Intermediates', 'Distance', 'Duration', 'Response'],
                               index = [0])
    
    response_df["DateTime"].loc[0] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    response_df["User Time"].loc[0] = usertime
    response_df["Junction Name"].loc[0] = waypoints_df["intersection_next_localname"].iloc[0]
    response_df["Junction no"].loc[0] = waypoints_df["intersection_next_junction_no"].iloc[0]
    response_df["No of Turns"].loc[0] = len(waypoints_df)
    response_df["Polyline"].loc[0] = poly_line
    response_df["start_lat"].loc[0] = waypoints_df["loc_latitude_from"].iloc[0]
    response_df["start_lng"].loc[0] = waypoints_df["loc_longitude_from"].iloc[0]
    response_df["end_lat"].loc[0] = waypoints_df["loc_latitude_to"].iloc[-1]
    response_df["end_lng"].loc[0] = waypoints_df["loc_longitude_to"].iloc[-1]
    intermediates_list = []
    
    for i in range(len(waypoints_df)):
        intermediates_list.append(waypoints_df.iloc[i][['loc_latitude_from', 'loc_longitude_from',
           'loc_road_from', 'travel_direction_from', 'loc_description_from', 'loc_latitude_to', 'loc_longitude_to',
           'loc_road_to', 'travel_direction_to',
           'loc_description_to', 'Turn']].to_dict())
        
    response_df["Intermediates"].loc[0] = intermediates_list
    response_df["Distance"].loc[0] = np.nan
    response_df["Duration"].loc[0] = None
    response_df["Response"].loc[0] = response 
    return response_df

def calc_sleep_time(times_list):
    
    ''' function to halt execution of program until the next time it is supposed to run'''
    
    # getting current date and time
    now = datetime.now()
    year = now.year
    month = now.month 
    day = now.day
    current_time = now.time()
    current_time = current_time.replace(microsecond=0)
    
    current_date = datetime.strptime(f"{year}-{month}-{day} {current_time}","%Y-%m-%d %H:%M:%S")
    
    flag = False
    for i in times_list:
        # if there exists a time in the times_list which is greater than current time, it halts until that time
        if i>current_time:
            next_time = i
            flag = True
            break
        
    # if no time is found to be greater than the current time in times_list
    # the date is shifted to the next day, and the time is reset to the first time in times_list.
    if(flag == False):
            next_time = times_list[0]
            date = now + dt.timedelta(days = 1)
            year = date.year
            month = date.month
            day = date.day
            
            
    next_date = datetime.strptime(f"{year}-{month}-{day} {next_time}","%Y-%m-%d %H:%M:%S")
    print(f"current time = {current_date}, next time = {next_date} ")
    #finds difference from current time until the next time
    diff = datetime.timestamp(next_date)-datetime.timestamp(current_date)
    print(f"diff = {diff}")
    current_time = next_time
    # halts execution until next time
    time.sleep(diff)
    return next_time



# =============================================================================
# Code
# =============================================================================


waypoints = pd.read_csv(f"{directory}{waypoint_filename}.csv")        

# dataframe containing all start and end points of each turn at a junction
destinations = waypoints[['loc_latitude', 'loc_longitude', 'loc_address', 'loc_road',
       'travel_direction', 'loc_description', 'Bearing', 'intersection_next_latitude',
       'intersection_next_longitude','intersection_next_localname', 'intersection_next_junction_no']].merge(waypoints[['loc_latitude', 'loc_longitude', 'loc_address', 'loc_road',
       'travel_direction', 'loc_description', 'Bearing', 'intersection_prev_latitude',
       'intersection_prev_longitude', 'intersection_prev_localname',
       'intersection_prev_junction_no']], how = "inner",
                               left_on = 'intersection_next_junction_no',
                               right_on = 'intersection_prev_junction_no', 
                               suffixes =  ('_from',"_to"))

# will be used for straight road requests                                                                                                                                                                       
waypoint_groups = waypoints.groupby("loc_road")
                                                


#contains seleccted junctions
junctions = pd.read_csv(f'{directory}{junction_filename}.csv')
junctions["intersection_junction_no"] = junctions["intersection_junction_no"].astype(str)

#removes unselected junctions from destinations dataframe
destinations = destinations.loc[destinations["intersection_next_junction_no"].isin( junctions["intersection_junction_no"])]
destinations = destinations.merge(junctions[["intersection_junction_no","U-turn"]],how="left",
                                left_on  = "intersection_next_junction_no",
                                right_on = "intersection_junction_no")

destinations.drop("intersection_junction_no",axis = 1,  inplace = True)
destinations.reset_index(drop=True, inplace = True)

# following code is to get the turn (straight, right, left or U turn) related to the start and end point in destinations
destinations["turn_angle"] = ((destinations["Bearing_to"]-destinations["Bearing_from"])+360)%360

directions_dict = {"turn_angle":[0,90,180,270], "Turn":["Straight", "Right", "U-turn", "Left"]}
manoeuvres = pd.DataFrame.from_dict(directions_dict)
destinations = destinations.merge(manoeuvres, how = "left", on = "turn_angle")

# destinations["U-turn"] is a boolean column containing whether a U turn is allowed at the specific junction or not
drop_rows =  destinations[(destinations["Turn"]=="U-turn")&(destinations["U-turn"]==False)]
destinations = destinations[list(map(operator.not_,(destinations["Turn"]=="U-turn")&(destinations["U-turn"]==False)))]
destinations.reset_index(drop=True, inplace = True)


# following code is to sort the rows in order of how the turn requests will be sent 
destinations_right = destinations[(destinations['Turn']=="Right")]
destinations_left = destinations[(destinations['Turn']=="Left")]

# for right turns in the request first right turn is NW to SW, second is SW to SE and so on
sorter_right = ['NW', 'SW', 'SE', 'NE']
# Similarly for left turns in the request first left turn is NW to NE, second is NE to SE and so on
sorter_left = ['NW', 'NE', 'SE', 'SW']

#travel direction from stores the direction of the road that the turn start from
destinations_right["travel_direction_from"] = destinations_right["travel_direction_from"].astype("category")
destinations_left["travel_direction_from"] = destinations_left["travel_direction_from"].astype("category")

# setting the category on the basis of which travel directions will be sorted
destinations_right["travel_direction_from"] =  destinations_right["travel_direction_from"].cat.set_categories(sorter_right)
destinations_left["travel_direction_from"] =  destinations_left["travel_direction_from"].cat.set_categories(sorter_left)

destinations_right.sort_values(["travel_direction_from"], inplace = True)  
destinations_left.sort_values(["travel_direction_from"], inplace = True)  

#joining left and rights df
destinations_right_left = pd.concat([destinations_right,destinations_left])
destination_groups = destinations_right_left.groupby('intersection_next_localname')


try:
    Road_Responses = pd.read_pickle(f"{directory}{roads_pickle_filename}.pkl")
except FileNotFoundError:
    Road_Responses = pd.DataFrame(columns = ["DateTime","User Time","Road","Travel Direction", 
                                      "Polyline", "start_lat", "start_lng", "end_lat",
                                      "end_lng", "Intermediates", "Distance", "Duration", "Response"])
    
try:
    Junction_Responses = pd.read_pickle(f"{directory}{junctions_pickle_filename}.pkl")
except FileNotFoundError:
    Junction_Responses = pd.DataFrame(columns = ['DateTime', 'User Time', 'Junction Name', 'Junction no',
                                      'No of Turns', 'Polyline', 'start_lat', 'start_lng', 'end_lat',
                                      'end_lng', 'Intermediates', 'Distance', 'Duration', 'Response'])
  



polyline_list = list()
j = 0

times = pd.to_datetime(pd.Series(times), format='%H:%M:%S').sort_values().dt.time #sorts list and converts to datetime

# =============================================================================
# loop to be run continously
# =============================================================================
x=0
while True:
    critical_errors = 0
    # stops programming if free requests are used up
    if ((request_counter_upto - (waypoint_groups.ngroups+destination_groups.ngroups)) < request_counter - 1):
        sys.exit("Request limit reached")
        
    # halts execution of program, stores current time from the times list
    user_time = calc_sleep_time(times)
    
    # loop to send straights requests
    for road_name,roads_group_df in waypoint_groups:
        #skips if only one or two waypoints are given
        if(len(roads_group_df)<=2):
           continue
        print(road_name)
        
        # road heading north
        road_north = roads_group_df[(roads_group_df['travel_direction']=='NE') | (roads_group_df['travel_direction']=='NW')]
        # same road heading opposite direction (south)
        road_south = roads_group_df[(roads_group_df['travel_direction']=='SE') | (roads_group_df['travel_direction']=='SW')]

        # while going north, sort on latitude in ascending order, to have correct order of waypoints to form a straight
        road_north.sort_values('loc_latitude', ascending = True, inplace = True)
        if(road_name == "Madhya Marg"):
            road_north.sort_values('loc_longitude', ascending = False, inplace = True)
 
        # while going south, sort on latitude in descending order, to have correct order of waypoints to form a straight
        road_south.sort_values('loc_latitude', ascending = False, inplace = True)
        if(road_name == "Madhya Marg"):
            road_south.sort_values('loc_longitude', ascending = True, inplace = True)
            
       
        # stores sorted north south waypoints in group_df
        roads_group_df = pd.concat([road_north,road_south])
        # polyline list stores all the longitudes and latitudes of the waypoints
        roads_polyline_list = list()
        for i in range(len(roads_group_df)):
            roads_polyline_list.append((roads_group_df["loc_latitude"].iloc[i],roads_group_df["loc_longitude"].iloc[i]))
        
        # stores plolyline
        road_poly = polyline.encode(roads_polyline_list,5)
        
        roads_group_df.reset_index(drop=True,inplace=True)
        errors=0
        road_waypoints = get_roads_waypoints_list(roads_group_df)
        while True:
            try:
                json_response = get_response(road_waypoints)
                
                #logging
                f = open(directory+log_filename, 'a')
                f.write(f"{datetime.now()} - INFO - {roads_group_df['loc_road'].iloc[0]}, {roads_group_df['travel_direction'].iloc[0]}- Request:{request_counter} \n")
                f.close()
                
                current_roads_response= get_roads_df(roads_group_df,json_response,user_time,road_poly)
                critical_errors=0
                break
            except:
                print("ERROR")
                #logging
                f = open(directory+error_log_filename, 'a')
                f.write(f"{datetime.now()} - ERROR - {road_name} {json_response} - Request:{request_counter} \n")
                f.close()
                errors+=1
               
                # if response shows error five times
                if errors>=5:
                    current_roads_response= get_error_roads_df(roads_group_df,json_response,user_time,road_poly)
                    critical_errors +=1
                    break
        
        # storing reponse row in main Road_Responses dataframe
        Road_Responses = pd.concat([Road_Responses,current_roads_response])    
        
        # terminates code if 2 critical errors occur
        if(critical_errors>=2):
            #logging
            f = open(directory+critical_log_filename, 'a')
            f.write(f"\n{datetime.now()} - CRITICAL - Paused \n\n")
            f.close()
            break
    
    # storing response df in a pickled file after all straights have been run
    outfile = open(f"{directory}{roads_pickle_filename}.pkl", 'wb')
    pickle.dump(Road_Responses, outfile)
    outfile.close()
    
    #==============================================================
    # JUNCTIONS
    #==============================================================
    
    for junction_name,junctions_group_df in destination_groups:
        print(junction_name)
        
        # polyline list stores all the longitudes and latitudes of the waypoints
        junctions_polyline_list = []
        for i in range(len(junctions_group_df)):
            junctions_polyline_list.append((junctions_group_df['loc_latitude_from'].iloc[i],junctions_group_df['loc_longitude_from'].iloc[i]))
            junctions_polyline_list.append((junctions_group_df['loc_latitude_to'].iloc[i],junctions_group_df['loc_longitude_to'].iloc[i]))
        
        junction_poly = polyline.encode(junctions_polyline_list)
        junctions_group_df.reset_index(drop=True,inplace=True)
        errors=0
        junction_waypoints = get_junctions_waypoints_list(junctions_group_df)
        while True:
            try:
                json_response = get_response(junction_waypoints)
                #logging
                f = open(directory+log_filename, 'a')
                f.write(f"{datetime.now()} - INFO - {junctions_group_df['intersection_next_localname'].iloc[0]}, {junctions_group_df['intersection_next_junction_no'].iloc[0]}- Request:{request_counter} \n")
                f.close()
                
                current_junctions_response= get_junctions_df(junctions_group_df,json_response,user_time,junction_poly)
                critical_errors=0
                break
            except:
                print("ERROR")
                #logging
                f = open(directory+error_log_filename, 'a')
                f.write(f"{datetime.now()} - ERROR - {junction_name} {json_response} - Request:{request_counter} \n")
                f.close()
                errors+=1

                # if response shows error five times
                if errors>=5:
                    current_junctions_response = get_error_junctions_df(junctions_group_df,json_response,user_time,junction_poly)
                    critical_errors +=1
                    break

        Junction_Responses = pd.concat([Junction_Responses,current_junctions_response])   
        
        # terminates code if 2 critical errors occur        
        if(critical_errors>=2):
            #logging
            f = open(directory+critical_log_filename, 'a')
            f.write(f"\n{datetime.now()} - CRITICAL - Paused \n\n")
            f.close()
            break

    # storing response df in a pickled file after all turns at junctions have been run
    outfile = open(f"{directory}{junctions_pickle_filename}.pkl", 'wb')
    pickle.dump(Junction_Responses, outfile)
    outfile.close()
    

    
