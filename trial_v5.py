import time
import json
from sensor import io
from ups import INA219
from logging_data import log
from helper import *
from gpiozero import Button
from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import logging
from logging.handlers import TimedRotatingFileHandler

log_the_data=log(
            unpub="/home/UbiqCM4/mqtt_data/datalogs",       #file to save unpublished data into json
            back_up="/home/UbiqCM4/mqtt_data/All_datalogs", #file to save all data into CSV format
            logged_data="/home/UbiqCM4/logged_data"
            )

unpub_file = log_the_data.unpub_data() #this creates the directory for unpublished file
back_up = log_the_data.backup_data()   #this creates the directory for all the data
logged_data=log_the_data.log_data()

# Initialize logger
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
log_file = os.path.join(logged_data, 'debug.log')

timed_handler = TimedRotatingFileHandler(
    log_file,    # Separate file for time-based rotation
    when='midnight',        # Rotate at midnight
    interval=1,            # Rotate every day
    backupCount=15         # Keep 15 backup files
)


# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
timed_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(timed_handler)
logger.info(f'Logger initialized. Log files: {log_file}')

#CLASS io Object
try:
    me31=io(port='/dev/ttyAMA3', window_size=12)
    me31.connect()
    me31.write_relay(0,False)
    logger.info("IO initialization successful")
except Exception as e:
    logger.error(f"Failed to initialize IO: {str(e)}")

#CLASS INA219 Object
try:
    ina219 = INA219(i2c_bus=10, addr=0x43)
    logger.info("INA219 initialization successful")
except Exception as e:
    logger.error(f"Failed to initialize INA219: {str(e)}")

#CLASS HELPER object
helpin = HELPER(None)

file_path = "/home/UbiqCM4/access_token.json"
access_token = ""

try:
    with open(file_path, 'r') as file:
        data = json.load(file)
        access_token = data['token']
        logger.info(f"Access token read successfully : {access_token}")
except FileNotFoundError:
    logger.error(f"JSON file not found: {file_path}")
except KeyError:
    logger.error("Token key not found in the JSON file.")
except json.JSONDecodeError:
    logger.error("Invalid JSON format in the file.")

try:
    client = TBDeviceMqttClient("samasth.io",username=access_token)
    client.connect()
except Exception as e:
    logger.error("Connection timeout error: %s", str(e))

def Publish_data(telemetry_with_ts):
    try:
        #client = TBDeviceMqttClient("samasth.io",username=access_token)
        client.connect()
        client.send_telemetry(telemetry_with_ts)
    except Exception as e:
        logger.error("Connection timeout error: %s", str(e))
        log_the_data.save_data_locally(telemetry_with_ts,unpub_file)
        client.disconnect()


BUTTON_PIN = 22
button = Button(BUTTON_PIN, pull_up=True)
MANUAL_DOSE=False

# Variables to store state
press_start_time = time.time()
ignore_events = False          # When Auto Dosing starts, manual detection of pump should be ignored

def button_pressed():
    global press_start_time
    # Only process the event if we're not ignoring events
    if not ignore_events:
        press_start_time = time.time()
        logger.info(f"Manual Dosing Started")

def button_released():
    # Only process the event if we're not ignoring events
    global MANUAL_DOSE
    if not ignore_events:
        duration = time.time() - press_start_time
        if duration > 3:
            manual_dosing_pub(duration)
            MANUAL_DOSE = False
        logger.info(f"Manual Dosing Stopped")
        logger.info(f"Manual Dosing was done for {duration:.2f} seconds")

# Assign both callbacks
button.when_pressed = button_pressed
button.when_released = button_released

def manual_dosing_pub(sec):
    manual_dose=(sec*0.5)/8
    telemetry_with_ts={"ts": int(round(time.time() * 1000)), "values": {"Manual_dosing_amount":round(manual_dose,2)}}
    log_the_data.save_data_locally_csv(telemetry_with_ts, back_up)    #saving current pressure every 900 secs
    Publish_data(telemetry_with_ts)

#All timers declarations
AVG_TIMER = time.time()             #Timer Used for Setting timer every 5 sec and updating average values
PUBLISH_TIMER=time.time()           #Publish timer used for setting every 65 sec after sending payload
IDLE_TIMER=time.time()              #idle timer used for setting device to idle every 5 mins
NON_ZERO_TIMER=time.time()          #Non zero timer is used when non zero pressure is constant till 10 mins and trip declaration
DOSING_PUMP_TIMER=time.time()       #used for setting pump off every 30 second in auto dosing
TRANSIT_TIMER=time.time()           #tansit timer is used for setting cuurent pressure every 900 sec
UNLOAD_TIMER=time.time()            #timer used for setting device to unloading when trip and loading has happened
READ_RPM_TIME=time.time()           #timer used to read rpm every 61 secs
MASTER_PRESSURE_TIMER=time.time()   #Master pressure timer used to set master pressure once when non zero timer has passed and declaring event to transit
PRINT_TELEMERTY_TIMER=time.time()   #timer used for fetching all parameters every two second
MASTER_SET_TIMER=time.time()        #timer used for setting master pressure after a certain time limit
CURRENT_SET_TIMER=time.time()       #timer used for setting current pressure after a certain time limit
UPS_TIMER_PUBLISH=time.time()       #timer used for publishing external supply after a certain time limit
STOP_DOSING_TIMER=time.time()       #timer used for to stop dosing after 3hours of master pressure set
DOSING_DECIDE_TIMER=time.time()     #timer used to verify if any of the intefernce is occured during CP


#All Flags Declarations
CONCRETE_LOADING_FLAG=False         #As soon concrete loading starts the CONCRETE LOADING FLAG is set
CONCRETE_TRANSIT_FLAG=False         #As soon as Trip starts CONCRETE TRANSIT FLAG is set
CONCRETE_UNLOADING_FLAG=False       #As soon as concrete unloading starts the CONCRETE UNLOADING FLAG is set
MASTER_PRESSURE_SET_FLAG=0          #Set Master Pressure After trip starts(Note:Master Pressure is set once)
NON_ZERO_FLAG=False                 #This flag is use set the Non zero timer once for pressure
AVG_TIMER_FLAG=False                #This Flag is use to set the Average timer once for pressure and tank level
PUBLISH_TIMER_FLAG=False            #this Flag is use to set the publish timer once
IDLE_TIMER_FLAG=False               #This Flag is use to set IDLE_TIMER once
DOSING_EVENT_FLAG=False             #This Flag is use to set dosing event once if cuurent pressure sets
MASTER_PRESSURE_TIMER_FLAG=False    #This Flag is use to trigger Master_pressure_timer to start
UNLOADING_TIMER_FLAG=False          #This Flag is use to trigger the Unload_timer to start
PRINT_TELEMETRY_FLAG=False          #This Flag is used to print the debugging statements every 2 sec on console
UPDATE_TRIP_FLAG=False
MASTER_SET_COUNT_FLAG=False
CURRENT_SET_COUNT_FLAG=False
DOSING_AMOUNT_TRIP_FLAG=False       #flag to set the Dosing amount per trip once during a trip
UPS_PUBLISH_FLAG=False              #flag to
First_CP_FLAG=False                 #This flag is used to skip dosing for 1st Current Pressure
Current_Pressure_Set=False          #This flag is used
AUTO_LOGIC=True                     #This flag is used for skiping or opting for auto dosing
CHECK_AUTO_DOSING=False             #

running_pressure=me31.instant_pressure()
tank_level =me31.tank_level
flow_meter_total = me31.read_pulse_count()
pump_status = me31.relay_status()
prev_pump_state=False
digital_inputs =me31.read_digital_inputs
Mixer_direction=me31.check_direction()
toggle_state=me31.read_digital_inputs()

count=0 #using for printing values evry 2 sec

#Variable for accumulating amount of dosing count
Dosing_Count=0
Dosing_Amount=0
total_dosing=0
dosing_time=0

#Variables used for measuring RPM
Mixer_Rotation_Prev=0
Mixer_Rotation_Now=0
rpm=0
Master_Set_Count=0 #to set the stabilized master pressure.
Current_Set_Count=0
Unloading_time_sec=0
Unload_time=0


running_average_pressure=0
tank_level_average=0
Master_Pressure_Avg=0
Mixer_direction=None
Event=None
prev_pulse_count=0
Desired_dosing=0
gps_flag=False
Total_trip=0
Total_trip_count=0
Dosing_Amount_Per_Trip=0
Dosing=0
rpm_pulse=0
prev_pulse=0
Percent_Change=0

#reading initial value of flowmeter
if flow_meter_total != 0:
    prev_pulse=flow_meter_total[2]

Mixer_Rotation_Prev = me31.read_pulse_count()

if Mixer_Rotation_Prev !=0:
    pulse_count = Mixer_Rotation_Prev[1]

    Mixer_Rotation_Prev = pulse_count

    Mixer_Rotation_Now = pulse_count

#Counting RPM (Revolution per minute)
def read_rpm_every_65s():
    """
    This function reads count of pulses on DI module every 60 seconds to return count of rotations
    """
    pulse_count_all = me31.read_pulse_count()
    if pulse_count_all != 0:
        global Mixer_Rotation_Prev
        global rpm
        Mixer_Rotation_Now = pulse_count_all[1]
        if Mixer_Rotation_Now >= Mixer_Rotation_Prev:
            temp = Mixer_Rotation_Now
            #uncomment below linw for debugging
            #logger.info(f"rpm recorded: {Mixer_Rotation_Now - Mixer_Rotation_Prev}")
            rpm = Mixer_Rotation_Now - Mixer_Rotation_Prev
            Mixer_Rotation_Prev = temp
            return rpm
        else:
            Mixer_Rotation_Prev=Mixer_Rotation_Now
    else:
        logger.error("Error reading count of pulses")


def start_pump_and_pump_timer():
    """
    This function starts pump when dosing is required followed by pump timer
    """
    global prev_pulse_count,DOSING_PUMP_TIMER,DOSING_EVENT_FLAG,ignore_events,pump_status

    ignore_events = True
    prev_pulse_count=flow_meter_total[2]                            #Storing current flow in previous pulse count for dosing
    me31.write_relay(0,True)
    me31.write_relay(1,True)
    DOSING_PUMP_TIMER=time.time()                                   #Start Pump timer when demanded
    DOSING_EVENT_FLAG=True                                          #setting this flag for dosing
    pump_status = me31.relay_status()
    logger.info(f"Turning Pump On and Pump status: {pump_status}")

def print_telemetry():
    """
    fetching realtime datas every 2 second and function for debug using print statement below
    """
    global count,Event, Mixer_direction, rpm,rpm_pulse, flow_meter_total, running_average_pressure, tank_level, pump_status,total_dosing,toggle_state,Total_trip,digital_inputs,CONCRETE_UNLOADING_FLAG,CONCRETE_TRANSIT_FLAG,CONCRETE_LOADING_FLAG,prev_pulse
    count+=1

    rpm_pulse=me31.read_pulse_count()
    running_pressure = me31.instant_pressure()
    tank_level = me31.tank_level()
    pump_status = me31.relay_status()
    digital_inputs = me31.read_digital_inputs()
    Mixer_direction=me31.check_direction()
    toggle_state=me31.read_digital_inputs()
    total_dosing=helpin.get_total_flow()
    Total_trip=helpin.get_total_trip()
    flow_meter_total = me31.read_pulse_count()

    if flow_meter_total[2] > prev_pulse:
        """
        if flowmeter total is updating then every 60 sec it should update in to total flow in litres
        """
        flow_dif=flow_meter_total[2]-prev_pulse
        helpin.write_total_flow(flow_dif)
        prev_pulse=flow_meter_total[2]

    if running_average_pressure <= 5:
        logging.info(f"[{count}].......[Event:{Event}]......")
        print("")

    elif running_average_pressure >5 and MASTER_PRESSURE_SET_FLAG == 1:

        logging.info(f"[{count}]..[Event:{Event}]....[Non_Zero_Timer:{(time.time()-NON_ZERO_TIMER):.1f}]...[Mast_Press:{Master_Pressure}]...[Current Timer:{(time.time()-TRANSIT_TIMER):1f}]....[Average_Pressure:{round(running_average_pressure,2)}]....[RPM:{(rpm)}]")
        print("")

    elif running_average_pressure >1 and DOSING_EVENT_FLAG == False:
        logging.info(f"[{count}]..[Event:{Event}]....[Non_Zero_Timer:{(time.time()-NON_ZERO_TIMER):.1f}].....[Time to set Master Pressure:{(time.time()-MASTER_PRESSURE_TIMER):.1f}]....[Average_Pressure:{round(running_average_pressure,2)}]........[RPM:{(rpm)}")
        print("")

while True:
    try:
        time.sleep(0.01)
        unpub_file = log_the_data.unpub_data()                      # this creates the directory for unpublished file
        back_up = log_the_data.backup_data()                        # this creates the directory for all the data
        logged_data = log_the_data.log_data()

        external_supply = ina219.check_external_supply()            # check state of external supply

        if DOSING_EVENT_FLAG:  #
            """
            This conditions checks the pump state through DOSING_PUMP_TIMER
            if pump remains ON beyond DOSING_PUMP_TIMER, we switch OFF the Pump
            """

            if (time.time() - DOSING_PUMP_TIMER) > dosing_time:
                me31.write_relay(0,False)
                me31.write_relay(1,False)
                pump_status = me31.relay_status()
                logger.info(f"Turning Pump OFF after dosing and Pump status: {pump_status}")
                Dosing_Count+=1
                Dosing_Amount=Desired_dosing
                telemetry_with_ts={"ts": int(round(time.time() * 1000)), "values": {"Pump_Status":pump_status,"Dosing_count":round(Dosing_Count,2),"Dosing_amount":round(Dosing_Amount,2)}}
                log_the_data.save_data_locally_csv(telemetry_with_ts, back_up)    #saving master pressure once for each trip
                Publish_data(telemetry_with_ts)
                DOSING_EVENT_FLAG=False
                ignore_events = False

        if external_supply:
            """
            Detects whether external power supply is connected and proceeds with further execution of algorithm.
            """

            if pump_status!=prev_pump_state:                                    # Detect the change of state of pump and updates on portal
                prev_pump_state=pump_status
                telemetry_with_ts={"ts": int(round(time.time() * 1000)), "values": {"Pump_Status":pump_status}}
                Publish_data(telemetry_with_ts)


            if (time.time()-READ_RPM_TIME) > 31 :                               # Counts the rotation of concrete mixer through rpm sensor
                READ_RPM_TIME=time.time()
                read_rpm_every_65s()

            if PRINT_TELEMETRY_FLAG!=True:                                      # Resets the PRINT_TELEMERTY_TIMER every 5 sec
                PRINT_TELEMERTY_TIMER=time.time()
                PRINT_TELEMETRY_FLAG=True

            if (time.time()-PRINT_TELEMERTY_TIMER) > 2:                         # Reading all the sensors and relevant parameters every 5 sec.
                #print("reading every 2 sec datas")
                print_telemetry()
                PRINT_TELEMERTY_TIMER=time.time()
                PRINT_TELEMETRY_FLAG=False

            if AVG_TIMER_FLAG!=True:                                            # Resets the AVG_TIMER every 5 sec
                AVG_TIMER = time.time()
                AVG_TIMER_FLAG=True

            if (time.time()-AVG_TIMER) > 5:                                     # Reads the average(rms) values of pressure and tank level
                running_average_pressure=me31.running_pressure_average()
                tank_level_average=me31.tank_level_average()
                AVG_TIMER_FLAG=False

            if PUBLISH_TIMER_FLAG!=True:                                        # Resets the PUBLISH_TIMER every 65 sec
                PUBLISH_TIMER=time.time()
                PUBLISH_TIMER_FLAG=True

            #to do: make a provison to publish data on configurable time window (through attributes)

            if (time.time()-PUBLISH_TIMER) > 35:                                # Publishes data on portal every 65 sec
                PUBLISH_TIMER=time.time()
                PUBLISH_TIMER_FLAG=False
                running_pressure=me31.instant_pressure()

                telemetry_with_ts = {"ts": int(round(time.time() * 1000)), "values": {"Event":Event,"rpm_pulse":rpm_pulse[1],"Flowmeter_Total":round(total_dosing,2),"Direction": Mixer_direction,"Average_pressure":round(running_average_pressure,2),"Running_Pressure":running_pressure[0],"Averag_Tank_level":round(tank_level_average,2),"RPM":rpm,"Pump_Status":pump_status,"Total_Trip":Total_trip,"Toggle_State":toggle_state[3],"External_Supply":external_supply}}
                Publish_data(telemetry_with_ts)

            if running_average_pressure < 5:                                    #Idle Condition
                """
                This Checks for ILDE condition if running_average_pressure is less than 5 bars
                Here we reset all the timers and flags
                Also declaration of end of trip, update dosing amount per trip and Trip count
                """

                Dosing_Amount=0
                Dosing_Count=0
                Event="IDLE"

                if IDLE_TIMER_FLAG!=True:
                    IDLE_TIMER=time.time()
                    IDLE_TIMER_FLAG=True

                if time.time() - IDLE_TIMER > 100:#we are cheking zero values for at least 5 mins

                    logger.info(f"Event is {Event}, No pressure is observed, Pressure is {round(running_average_pressure,2)}")

                    helpin.reset_flowcount()
                    IDLE_TIMER=time.time()
                    IDLE_TIMER_FLAG=False
                    NON_ZERO_TIMER=time.time()
                    NON_ZERO_FLAG=False
                    MASTER_PRESSURE_SET_FLAG=0
                    MASTER_PRESSURE_TIMER_FLAG=False

                    #If only CONCRETE UNLOAD FLAG and CONCRETE LOADING FLAG and CONCRETE TRANSIT FLAG are True declare a trip is valid
                    if UPDATE_TRIP_FLAG!=True:
                        if CONCRETE_UNLOADING_FLAG and CONCRETE_LOADING_FLAG and CONCRETE_TRANSIT_FLAG:
                                CONCRETE_UNLOADING_FLAG,CONCRETE_LOADING_FLAG,CONCRETE_TRANSIT_FLAG=False,False,False #Reset all the flag and declare trip end
                                print("Trip is valid and ended")
                                logger.info("Trip Ended, Reseting all the Flags and wait for next trip")
                                Unloading_time_sec=(time.time()-UNLOAD_TIMER)
                                Unload_time=helpin.time_string(Unloading_time_sec)
                                Total_trip_count+=1
                                helpin.write_total_trip(1)
                                telemetry_with_ts={"ts": int(round(time.time() * 1000)), "values": {"Dosing_amount_per_trip":round(Dosing_Amount_Per_Trip,2),"Unloading_Time":Unload_time}}
                                Publish_data(telemetry_with_ts)
                                log_the_data.save_data_locally_csv(telemetry_with_ts,back_up)
                                UPDATE_TRIP_FLAG=True
                                DOSING_AMOUNT_TRIP_FLAG=False
                                Master_Set_Count=0
                                Current_Set_Count=0
                                CURRENT_SET_COUNT_FLAG=False
                                dosing_time=0

            elif running_average_pressure > 5:
                """
                Here we observe Non Zero pressure and Non Zero timer(Trip timer) starts
                During this period we observe entire trip cycle of truck from loading to unloading

                """

                if NON_ZERO_FLAG!=True and me31.check_direction()==1:                       # This flag ensure that a Non zero timer set once and reset again after next trip
                    NON_ZERO_TIMER=time.time()
                    NON_ZERO_FLAG=True
                    logger.info(f"Non Zero Timer Started, RPM is observed {rpm}")
                    logger.info(f"Non Zero Pressure Observed, Pressure is observed {round(running_average_pressure,2)} bars, RPM is observed {rpm}")

                if (time.time()-NON_ZERO_TIMER)> 120 and me31.check_direction()==1 :       # Here After 20mins of continous Non Zero pressure observation we declare truck in "Transit" State
                    IDLE_TIMER=time.time()
                    CONCRETE_LOADING_FLAG=True
                    if Event != "TRANSIT":
                        Event="TRANSIT"
                        #logger.info(f"Event {Event} is Observed, RPM is Observed {rpm}")

                    """
                    After 20 mins of continous non zero pressure, event is been declared as transit and after 25 minutes master pressure sets.
                    The master pressure is the rms value of pressure

                    """

                    if MASTER_PRESSURE_TIMER_FLAG!=True:
                        MASTER_PRESSURE_TIMER=time.time()
                        MASTER_PRESSURE_TIMER_FLAG=True
                        logger.info(f"Master Pressure timer started, RPM is observed {rpm}")

                    if (time.time()-MASTER_PRESSURE_TIMER) > 90 and MASTER_PRESSURE_SET_FLAG !=1 and 0 <= rpm <= 2:

                        if MASTER_SET_COUNT_FLAG!=True:
                            MASTER_SET_TIMER=time.time()
                            MASTER_SET_COUNT_FLAG=True

                        if (time.time()-MASTER_SET_TIMER) > 6:
                            Master_Set_Count+=1
                            print("master pressure count",Master_Set_Count)
                            if Master_Set_Count > 5:
                                Master_Pressure_Avg=round(me31.master_pressure_average(),2)
                            MASTER_SET_TIMER=time.time()

                        if Master_Set_Count >= 10:
                            Master_Pressure =  Master_Pressure_Avg  #set master pressure to a value after 420 seconds
                            Master_Set_Count=0
                            telemetry_with_ts={"ts": int(round(time.time() * 1000)), "values": {"Master_Pressure":round(Master_Pressure,2)}}
                            log_the_data.save_data_locally_csv(telemetry_with_ts, back_up)#saving master pressure once for each trip
                            Publish_data(telemetry_with_ts)
                            #Log this master pressure to backup data
                            logger.info(f"Master Pressure is Set to {round(Master_Pressure,2)}, RPM is Observed as {rpm}")
                            MASTER_PRESSURE_SET_FLAG = 1
                            TRANSIT_TIMER=time.time()
                            logger.info(f"Transit Timer is started, Pressure is observed as {round(running_average_pressure,2)}")
                            First_CP_FLAG = True
                            CONCRETE_TRANSIT_FLAG=True  #indicating truck has started moving
                            MASTER_SET_COUNT_FLAG=False
                            STOP_DOSING_TIMER=time.time() #Rename variable
                            MANUAL_DOSE = True

                if me31.check_direction() == 1 and 3 <= rpm <= 7 :                          # Loading State occurs when rpm of concrete mixer is observed around 3 to 7.
                    me31.write_relay(0,False)
                    me31.write_relay(1,False)
                    Master_Set_Count=0
                    if Event != "LOADING":
                        Event="LOADING"
                        #logger.info(f"Event {Event} is Observed, RPM is Observed {rpm}")

                if me31.check_direction()==0 and MASTER_PRESSURE_SET_FLAG == 1:
                    AUTO_LOGIC = False


                if (time.time() -TRANSIT_TIMER) > 60 and MASTER_PRESSURE_SET_FLAG == 1 :
                    """
                    Here we wait for 10mins, after every 60sec for 5 count we average(rms) the pressure to set Stabilized Current Pressure.
                    """

                    if CURRENT_SET_COUNT_FLAG!=True:                           # Reset the CURRENT_SET_TIMER every 60sec
                        CURRENT_SET_TIMER=time.time()
                        CURRENT_SET_COUNT_FLAG=True

                    if (time.time()-CURRENT_SET_TIMER) > 6:                   # Every 60sec we average(rms) the 1min pressure and also ensure if rpm exceed more than 3 no auto dosing happens
                        Current_Set_Count+=1
                        if rpm >= 3 or me31.check_direction()==0:
                            """
                            Even if once rpm exceeds more than 2 rpm dosing logic skip
                            """
                            AUTO_LOGIC=False

                        print("count in current set count",Current_Set_Count)
                        Current_Pressure_Avg = round(me31.current_pressure_average(),2)
                        CURRENT_SET_TIMER=time.time()

                    if Current_Set_Count > 5 and Current_Pressure_Set != True:                                  # Set Current pressure after 5min and calculate the percent change

                        #Get the CURRENT PRESSURE every 900 sec
                        Current_Pressure = Current_Pressure_Avg
                        logger.info(f"Current Pressure is Set to {Current_Pressure}, RPM is Observed as {rpm}")
                        logger.info(f"Transit Timer is started, Pressure is observed as {round(running_average_pressure,2)}")
                        telemetry_with_ts={"ts": int(round(time.time() * 1000)), "values": {"Current_Pressure":round(Current_Pressure,2)}}
                        log_the_data.save_data_locally_csv(telemetry_with_ts, back_up)#saving current pressure every 900 secs
                        Publish_data(telemetry_with_ts)
                        #check whether Current Pressure is greater than Master Pressure
                        try: #handle divide by zero error
                            Percent_Change =  ((Current_Pressure - Master_Pressure )/Master_Pressure)*100 # set percent_change variable
                            logger.info(f"Percent Change calculated is {Percent_Change}, Current pressure is {Current_Pressure} bars, Master Pressure is {round(Master_Pressure,2)} bars")
                        except:
                            logger.info("Master Pressure is zero")
                            Percent_Change = Current_Pressure * 100

                        Pressure_Difference = Current_Pressure - Master_Pressure # set Pressure difference
                        Current_Pressure_Set = True
                        DOSING_DECIDE_TIMER=time.time()

                    if me31.check_direction()==0 or rpm >= 3 and Current_Pressure_Set != False :      # Check if even once rpm exceeds more than 3 rpm and direction change ensure no auto dosing
                        AUTO_LOGIC = False


                    if (time.time() - DOSING_DECIDE_TIMER) >= 30 and Current_Pressure_Set != False:  # Wait 5 mins after current pressure sets
                        TRANSIT_TIMER = time.time()
                        DOSING_DECIDE_TIMER = time.time()
                        CHECK_AUTO_DOSING = True
                        Current_Pressure_Set = False
                        Current_Set_Count=0

                if me31.check_direction() == 1 and 0 <= rpm <= 2 and CHECK_AUTO_DOSING != False:
                    """
                    Entire AutoLogic is implemented when truck is in transit state and mixer direction clockwise
                    """
                    CHECK_AUTO_DOSING = False

                    if DOSING_AMOUNT_TRIP_FLAG!=True:
                        Dosing_Amount_Per_Trip=0
                        DOSING_AMOUNT_TRIP_FLAG=True

                    if MANUAL_DOSE:

                        if AUTO_LOGIC:

                            if (time.time()-STOP_DOSING_TIMER) <= 1800:

                                if First_CP_FLAG != True:

                                    if Dosing_Amount_Per_Trip <= 4:

                                        if tank_level_average >= 5:

                                            if Percent_Change <= 20:
                                            #setting pump off by writing to register 0
                                                me31.write_relay(0, False)

                                            elif Percent_Change > 20 :
                                                logger.info("pressure difference between 20 and 30")

                                                MP=Master_Pressure

                                                if  Pressure_Difference >= 0.2*MP and Pressure_Difference <= 0.25*MP:
                                                    start_pump_and_pump_timer()
                                                    dosing_time=8
                                                    Desired_dosing=0.5  #here 50 indicates 0.5L of solution
                                                    Dosing_Amount_Per_Trip+=Desired_dosing

                                                elif Pressure_Difference >= 0.25*MP and Pressure_Difference <= 0.35*MP:
                                                    start_pump_and_pump_timer()
                                                    dosing_time=16
                                                    Desired_dosing=1.0 #here 100 indicates 1L of solution
                                                    Dosing_Amount_Per_Trip+=Desired_dosing

                                                elif Pressure_Difference >= 0.35*MP and Pressure_Difference <= 0.45*MP:
                                                    start_pump_and_pump_timer()
                                                    dosing_time=24
                                                    Desired_dosing=1.5#here 150 indicates 1.5L of solution
                                                    Dosing_Amount_Per_Trip+=Desired_dosing

                                                elif Pressure_Difference >= 0.45*MP :
                                                    start_pump_and_pump_timer()
                                                    dosing_time=30
                                                    Desired_dosing=2.0  #here 200 indicates 2L of solution
                                                    Dosing_Amount_Per_Trip+=Desired_dosing

                                                logger.info(f"Dosing time is {dosing_time}sec, Dosing amount is {Desired_dosing}L")
                                        else:
                                            logger.info("Tank level is less than 5 litre no dosing")
                                    else:
                                        logger.info("Dosing Amount per trip has been exceeded...")
                                else :
                                    First_CP_FLAG =False
                                    logger.info("first Auto dosing was observed, no dosing")
                            else:
                                logger.info("3 Hours have been exceeded since Master pressure set, No dosing")
                        else:
                            logger.info("RPM is disturbed or Mixer State Change, No Auto Dosing should happen")
                            AUTO_LOGIC = True
                    else:
                        logger.info("Manual Dosing was detected, No Auto Dosing Happens during whole trip")

                elif me31.check_direction()==0:

                    Master_Set_Count=0
                    me31.write_relay(0,False)
                    me31.write_relay(1,False)

                    Event="UNLOADING"

                    if UNLOADING_TIMER_FLAG!=True and CONCRETE_TRANSIT_FLAG:
                        UNLOAD_TIMER=time.time()
                        UNLOADING_TIMER_FLAG=True

                    if (time.time()-UNLOAD_TIMER) > 90 and CONCRETE_TRANSIT_FLAG:        # checking whether transit has happened or not
                        UNLOADING_TIMER_FLAG=False
                        logger.info(f"Event {Event} is observed, RPM {rpm}rpm is observed")
                        Event="UNLOADING"
                        CONCRETE_UNLOADING_FLAG=True
                        IDLE_TIMER_FLAG=False
                        UPDATE_TRIP_FLAG=False
        else:
            if UPS_PUBLISH_FLAG!=True:
                UPS_TIMER_PUBLISH=time.time()
                UPS_PUBLISH_FLAG=True

            if (time.time()-UPS_TIMER_PUBLISH) > 65:
                telemetry_with_ts={"ts": int(round(time.time() * 1000)), "values": {"External_Supply":external_supply}}
                log_the_data.save_data_locally_csv(telemetry_with_ts,back_up)
                Publish_data(telemetry_with_ts)
                UPS_TIMER_PUBLISH=time.time()
                UPS_PUBLISH_FLAG=False

    except Exception as e:
        print("exception",e)
        me31.write_relay(0,False)