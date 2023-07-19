import pandas as pd
from datetime import datetime
import inputs
import re
import os
import PySimpleGUI as sg
import statistics
import data_treatment


def read_daily_alarm_report(alarm_report_path, irradiance_file_path, event_tracker_path, previous_dmr_path):
    dir = os.path.dirname(alarm_report_path)
    basename = os.path.basename(alarm_report_path)
    date_finder = re.search(r'\d\d\d\d-\d\d-\d\d', alarm_report_path)
    date = date_finder.group()
    geography_report_match = re.search(r'\w+?_', basename)
    geography_report = geography_report_match.group()[:-1]
    print(geography_report)

    day = date[-2:]     #so naive please change this later
    month = date[-5:-3]
    year = date[:4]

    df_all = pd.read_excel(alarm_report_path, engine="openpyxl")
    df_all['InSolar Check'] = ""
    df_all['Curtailment Event'] = ""
    df_all['Tracker'] = ''
    df_all['Comments'] = ''
    df_all_columns = df_all.columns

    irradiance_data = pd.read_excel(irradiance_file_path, engine="openpyxl")

    try:
        all_prev_active_events = pd.read_excel(previous_dmr_path,
                                               sheet_name=["Active incidents", "Active tracker incidents"],
                                               engine="openpyxl")
        #all_prev_active_events = pd.concat([all_prev_active_events['Active Events'], all_prev_active_events['Active tracker incidents']])
        #df_all = pd.concat([df_all, all_prev_active_events['Active Events'], all_prev_active_events['Active tracker incidents']])[df_all_columns]

        prev_active_events = all_prev_active_events['Active incidents']
        prev_active_tracker_events = all_prev_active_events['Active tracker incidents']

        prev_active_events['InSolar Check'] = "x"
        prev_active_tracker_events['InSolar Check'] = "x"

        #print(df_all.columns)
    except FileNotFoundError:
        print("Previous Daily Monitoring Report not found.")
        try:
            all_prev_active_events = pd.read_excel(event_tracker_path,
                                                   sheet_name=["Active Events", "Active tracker incidents"],
                                                   engine="openpyxl")

            prev_active_events = all_prev_active_events['Active Events']
            prev_active_tracker_events = all_prev_active_events['Active tracker incidents']

        except FileNotFoundError:
            print("Event Tracker not found.")

    newfile = dir + '/Incidents' + str(day) + '-' + str(month) + str(geography_report) + '.xlsx'
    newtrackerfile = dir + '/Tracker_Incidents' + str(day) + '-' + str(month) + str(geography_report) + '.xlsx'

    return df_all, newfile, newtrackerfile, irradiance_data, prev_active_events, prev_active_tracker_events


def read_general_info(general_info_path):

    df_general_info = pd.read_excel(general_info_path, sheet_name='Site Info', engine="openpyxl")
    df_general_info_calc = pd.read_excel(general_info_path, sheet_name='Site Info', index_col=0, engine="openpyxl")
    all_component_data = pd.read_excel(general_info_path, sheet_name='Component Code', index_col=0, engine="openpyxl")


    return df_general_info, df_general_info_calc, all_component_data


def read_time_of_operation_old(irradiance_df, Report_template_path, withmean: bool = False):

    df_info_capacity = pd.read_excel(Report_template_path, sheet_name='Info', engine="openpyxl")

    irradiance_df = irradiance_df.loc[:, ~irradiance_df.columns.str.contains('^Unnamed')]
    irradiance_df = irradiance_df[:-1]    # removes last timestamp of dataframe, since it's always the next day

    irradiance_df['day'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').day for timestamp in
                                   irradiance_df['Timestamp']]
    only_days = irradiance_df['day'].drop_duplicates().tolist()

    """try:
        del df_all
        print('Existing variable deleted. All good now, starting compilation')
    except NameError:
        print('All good, starting compilation')"""

    irradiance_file_data_curated = irradiance_df.loc[:, irradiance_df.columns.str.contains('curated')]

    irradiance_file_data_notcurated = irradiance_df.loc[:, ~irradiance_df.columns.str.contains('curated')]
    irradiance_file_data_poaaverage = irradiance_file_data_notcurated.loc[:,
                                      irradiance_file_data_notcurated.columns.str.contains('Average')]

    irradiance_file_data_meteostation = irradiance_df.loc[:, irradiance_df.columns.str.contains('Meteo')]

    for column in irradiance_file_data_poaaverage.columns:
        dict_timeofops = {}
        dict_timeofops_seconds = {}

        only_name_site = re.search(r'\[.+\]', column).group().replace('[', "").replace(']', "")
        only_name_site = data_treatment.correct_site_name(only_name_site)

        print(only_name_site)

        capacity = float(df_info_capacity.loc[df_info_capacity['Site'] == only_name_site]['Capacity'])

        # Get curated data
        try:
            curated_column = irradiance_file_data_curated.loc[:, irradiance_file_data_curated.columns.str.contains(only_name_site)].columns[0]
        except IndexError:
            curated_column = column

        if not column == 'Timestamp' and not column == 'day':
            data = irradiance_df[['Timestamp', curated_column, 'day']]
        else:
            continue

        backup_data = irradiance_df[['Timestamp', column, 'day']]

        # print(name_site)
        dict_timeofops['Site'] = only_name_site
        print(dict_timeofops)
        for day in only_days:
            print("Day under analysis: " + str(day))
            data_day = data.loc[data['day'] == day].reset_index()
            entire_day = data['Timestamp'][0]
            entire_day = datetime.strptime(str(entire_day), '%Y-%m-%d %H:%M:%S').date()

            #print(entire_day)

            try:
                stime_index = next(i for i, v in enumerate(data_day[curated_column]) if v > 20)
                etime_index = next(i for i, v in reversed(list(enumerate(data_day[curated_column]))) if v > 20)

                stime = data_day['Timestamp'][stime_index]
                etime = data_day['Timestamp'][etime_index]

                # Verify Hours read------------------------------
                stime, etime = data_treatment.verify_read_time_of_operation(only_name_site, entire_day, stime, etime)

                # -------------------------------------------------

            except StopIteration:
                print('No data on the ' + str(entire_day))
                try:
                    stime_index = next(i for i, v in enumerate(backup_data[column]) if v > 20)
                    etime_index = next(i for i, v in reversed(list(enumerate(backup_data[column]))) if v > 20)

                    stime = data_day['Timestamp'][stime_index]
                    #print(stime)
                    etime = data_day['Timestamp'][etime_index]
                    #print(etime)

                    #print('Verify 2')

                    stime, etime = data_treatment.verify_read_time_of_operation(only_name_site, entire_day, stime, etime)

                except StopIteration:
                    print('No backup data on the ' + str(entire_day))
                    stime, etime = inputs.input_time_operation_site(only_name_site, str(entire_day))

            if type(stime) == str:
                stime = datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')
            if type(etime) == str:
                etime = datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')

            dict_timeofops['Capacity'] = [capacity]
            dict_timeofops['Time of operation start'] = [stime]
            dict_timeofops['Time of operation end'] = [etime]

            df_timeofops = pd.DataFrame.from_dict(dict_timeofops)
            # df_timeofops = df_timeofops.set_index('Site')

            try:
                df_all = df_all.append(df_timeofops)
            except (UnboundLocalError, NameError):
                df_all = df_timeofops

    df_info_sunlight = df_all.reset_index(drop=True)
    print(df_info_sunlight)

    if withmean is True:
        df_all = df_all.set_index('Site')
        stime_columns = df_all.columns[df_all.columns.str.contains('sunrise')].tolist()
        etime_columns = df_all.columns[df_all.columns.str.contains('sunset')].tolist()

        stime_data = df_all.loc[:, stime_columns]
        etime_data = df_all.loc[:, etime_columns]

        for index, row in stime_data.iterrows():
            timestamps = row.tolist()
            timestamps_datetime = [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') for timestamp in timestamps if
                                   timestamp != 'No data']
            in_seconds = [(i.hour * 3600 + i.minute * 60 + i.second) for i in timestamps_datetime if i != 'No data']
            average_in_seconds = int(statistics.mean(in_seconds))
            average_in_hours = datetime.fromtimestamp(average_in_seconds - 3600).strftime("%H:%M:%S")

            df_all.loc[index, 'Mean Start Time'] = average_in_hours

        for index, row in etime_data.iterrows():
            timestamps = row.tolist()
            timestamps_datetime = [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') for timestamp in timestamps if
                                   timestamp != 'No data']
            in_seconds = [(i.hour * 3600 + i.minute * 60 + i.second) for i in timestamps_datetime if i != 'No data']
            average_in_seconds = int(statistics.mean(in_seconds))
            average_in_hours = datetime.fromtimestamp(average_in_seconds - 3600).strftime("%H:%M:%S")

            df_all.loc[index, 'Mean End Time'] = average_in_hours
        df_info_sunlight = df_all

    return df_info_sunlight, irradiance_file_data_notcurated


def read_time_of_operation_new(irradiance_df, site_list, df_general_info, withmean: bool = False):

    irradiance_df = irradiance_df.loc[:, ~irradiance_df.columns.str.contains('^Unnamed')]
    irradiance_df = irradiance_df[:-1]    # removes last timestamp of dataframe, since it's always the next day

    irradiance_df['day'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').day
                            for timestamp in irradiance_df['Timestamp']]
    only_days = irradiance_df['day'].drop_duplicates().tolist()

    irradiance_file_data_curated = irradiance_df.loc[:, irradiance_df.columns.str.contains('curated')]

    irradiance_file_data_notcurated = irradiance_df.loc[:, ~irradiance_df.columns.str.contains('curated')]
    irradiance_file_data_poaaverage = irradiance_file_data_notcurated.loc[:,
                                      irradiance_file_data_notcurated.columns.str.contains('Average')]

    # irradiance_file_data_meteostation = irradiance_df.loc[:, irradiance_df.columns.str.contains('Meteo')]

    for column in irradiance_file_data_poaaverage.columns:
        dict_timeofops = {}
        dict_timeofops_seconds = {}

        only_name_site = re.search(r'\[.+\]', column).group().replace('[', "").replace(']', "")
        only_name_site = data_treatment.correct_site_name(only_name_site)

        print(only_name_site)

        capacity = float(df_general_info.loc[df_general_info['Site'] == only_name_site]['Nominal Power DC'])

        # Get curated data
        if only_name_site in site_list:
            try:
                curated_column = irradiance_file_data_curated.loc[:,
                                 irradiance_file_data_curated.columns.str.contains(only_name_site)].columns[0]
            except IndexError:
                curated_column = column

            if not column == 'Timestamp' and not column == 'day':
                data = irradiance_df[['Timestamp', curated_column, 'day']]
            else:
                continue

            backup_data = irradiance_df[['Timestamp', column, 'day']]
            dict_timeofops['Site'] = only_name_site

            for day in only_days:
                print("Day under analysis: " + str(day))
                data_day = data.loc[data['day'] == day].reset_index()
                entire_day = data['Timestamp'][0]
                entire_day = datetime.strptime(str(entire_day), '%Y-%m-%d %H:%M:%S').date()

                #print(entire_day)

                try:
                    stime_index = next(i for i, v in enumerate(data_day[curated_column]) if v > 20)
                    etime_index = next(i for i, v in reversed(list(enumerate(data_day[curated_column]))) if v > 20)

                    stime = data_day['Timestamp'][stime_index]
                    etime = data_day['Timestamp'][etime_index]

                    # Verify Hours read------------------------------
                    stime, etime = data_treatment.verify_read_time_of_operation(only_name_site, entire_day, stime, etime)

                    # -------------------------------------------------

                except StopIteration:
                    print('No data on the ' + str(entire_day))
                    try:
                        stime_index = next(i for i, v in enumerate(backup_data[column]) if v > 20)
                        etime_index = next(i for i, v in reversed(list(enumerate(backup_data[column]))) if v > 20)

                        stime = data_day['Timestamp'][stime_index]
                        #print(stime)
                        etime = data_day['Timestamp'][etime_index]
                        #print(etime)

                        #print('Verify 2')

                        stime, etime = data_treatment.verify_read_time_of_operation(only_name_site, entire_day, stime, etime)

                    except StopIteration:
                        print('No backup data on the ' + str(entire_day))
                        stime, etime = inputs.input_time_operation_site(only_name_site, str(entire_day))

                if type(stime) == str:
                    stime = datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')
                if type(etime) == str:
                    etime = datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')

                dict_timeofops['Capacity'] = [capacity]
                dict_timeofops['Time of operation start'] = [stime]
                dict_timeofops['Time of operation end'] = [etime]

                df_timeofops = pd.DataFrame.from_dict(dict_timeofops)
                # df_timeofops = df_timeofops.set_index('Site')

                try:
                    df_all = df_all.append(df_timeofops)
                except (UnboundLocalError, NameError):
                    df_all = df_timeofops

    df_info_sunlight = df_all.reset_index(drop=True)
    print(df_info_sunlight)

    if withmean is True:
        df_all = df_all.set_index('Site')
        stime_columns = df_all.columns[df_all.columns.str.contains('sunrise')].tolist()
        etime_columns = df_all.columns[df_all.columns.str.contains('sunset')].tolist()

        stime_data = df_all.loc[:, stime_columns]
        etime_data = df_all.loc[:, etime_columns]

        for index, row in stime_data.iterrows():
            timestamps = row.tolist()
            timestamps_datetime = [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') for timestamp in timestamps if
                                   timestamp != 'No data']
            in_seconds = [(i.hour * 3600 + i.minute * 60 + i.second) for i in timestamps_datetime if i != 'No data']
            average_in_seconds = int(statistics.mean(in_seconds))
            average_in_hours = datetime.fromtimestamp(average_in_seconds - 3600).strftime("%H:%M:%S")

            df_all.loc[index, 'Mean Start Time'] = average_in_hours

        for index, row in etime_data.iterrows():
            timestamps = row.tolist()
            timestamps_datetime = [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') for timestamp in timestamps if
                                   timestamp != 'No data']
            in_seconds = [(i.hour * 3600 + i.minute * 60 + i.second) for i in timestamps_datetime if i != 'No data']
            average_in_seconds = int(statistics.mean(in_seconds))
            average_in_hours = datetime.fromtimestamp(average_in_seconds - 3600).strftime("%H:%M:%S")

            df_all.loc[index, 'Mean End Time'] = average_in_hours
        df_info_sunlight = df_all

    return df_info_sunlight, irradiance_file_data_notcurated


def get_filename_folder():
    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.

    layout = [[sg.Text('Choose file', pad=((2, 10), (2, 5)))],
              [sg.FileBrowse(target='-FILE-'),
               sg.In(key='-FILE-', text_color='black', size=(20, 1), enable_events=True, readonly=True,
                     visible=True)],
              [sg.Button('Submit'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Choose file', layout)

    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read(timeout=100)

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            "file_path = filename = folder = 0"
            break

        if event == 'Submit':
            file_path = values['-FILE-']
            print(file_path)
            if file_path == "":
                sg.popup('No file chosen, try again or exit')
                continue
            filepath_split = os.path.split(file_path)
            folder = filepath_split[0] + "/"
            filename = filepath_split[1].split(".")[0]
            extension = "." + filepath_split[1].split(".")[1]

            break

    window.close()

    return file_path, filename, folder, extension

def read_irradiance_export(irradiance_file_path, export_file_path):
    
    irradiance_df = pd.read_excel(irradiance_file_path, engine="openpyxl")
    #irradiance_df["Timestamp"] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S') for timestamp in irradiance_df["Timestamp"]]
    irradiance_df['Timestamp'] = pd.to_datetime(irradiance_df['Timestamp'])

    export_df = pd.read_excel(export_file_path, engine="openpyxl")
    #export_df["Timestamp"] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S') for timestamp in export_df["Timestamp"]]
    export_df['Timestamp'] = pd.to_datetime(export_df['Timestamp'])

    return irradiance_df, export_df 

def read_approved_incidents(incidents_file, site_list, roundto: int = 15):
    for site in site_list:

        if "LSBP - " in site or "LSBP – " in site:
            onlysite = site[7:]
        else:
            onlysite = site

        if onlysite[-1:] == " ":
            onlysite = onlysite[:len(onlysite)-1]
        active_sheet_name = onlysite + ' Active'
        closed_sheet_name = onlysite

        active_df = pd.read_excel(incidents_file, sheet_name=active_sheet_name, engine="openpyxl")
        active_df = active_df.loc[active_df['InSolar Check'] == 'x']  # filter for checked events
        active_df = active_df.reset_index(None, drop=True)
        active_df = data_treatment.remove_milliseconds(active_df)
        active_df = data_treatment.rounddatesactive_15m(site, active_df, freq=roundto)

        closed_df = pd.read_excel(incidents_file, sheet_name=closed_sheet_name, engine="openpyxl")
        closed_df = closed_df.loc[closed_df['InSolar Check'] == 'x']  # filter for checked events
        closed_df = closed_df.reset_index(None, drop=True)
        closed_df = data_treatment.remove_milliseconds(closed_df, end_time=True)
        closed_df = data_treatment.rounddatesclosed_15m(site, closed_df, freq=roundto)
        closed_df = data_treatment.correct_duration_of_event(closed_df)

        try:
            if site not in df_list_active.keys():
                df_list_active[site] = active_df
        except NameError:
            df_list_active = {site: active_df}

        try:
            if site not in df_list_closed.keys():
                df_list_closed[site] = closed_df
        except NameError:
            df_list_closed = {site: closed_df}

    return df_list_active, df_list_closed

def read_approved_tracker_inc(tracker_file, roundto: int = 15):
    df_info_trackers = pd.read_excel(tracker_file, sheet_name='Trackers info', engine="openpyxl")

    df_tracker_active = pd.read_excel(tracker_file, sheet_name='Active tracker incidents', engine="openpyxl")
    df_tracker_active = df_tracker_active.loc[df_tracker_active['InSolar Check'] == 'x']  # filter for checked events
    df_tracker_active = df_tracker_active.reset_index(None, drop=True)
    df_tracker_active = data_treatment.remove_milliseconds(df_tracker_active)
    df_tracker_active = data_treatment.rounddatesactive_15m('Trackers', df_tracker_active, freq=roundto)

    df_tracker_closed = pd.read_excel(tracker_file, sheet_name='Closed tracker incidents', engine="openpyxl")
    df_tracker_closed = df_tracker_closed.loc[df_tracker_closed['InSolar Check'] == 'x']  # filter for checked events
    df_tracker_closed = df_tracker_closed.reset_index(None, drop=True)
    df_tracker_closed = data_treatment.remove_milliseconds(df_tracker_closed, end_time=True)
    df_tracker_closed = data_treatment.rounddatesclosed_15m('Trackers', df_tracker_closed, freq=roundto)

    return df_tracker_active, df_tracker_closed

def read_curtailment_dataframes(source_folder, geography, geopgraphy_folder, site_selection, period,
                                irradiance_threshold: int = 20):

    # <editor-fold desc="turn all of this into a data acquistion function!!!">
    date_start_str, date_end_str = inputs.choose_period_of_analysis(period)
    date_start = datetime.strptime(date_start_str, '%Y-%m-%d')
    date_end = datetime.strptime(date_end_str, '%Y-%m-%d')

    year = date_start.year
    dest_file_suffix = date_start.strftime("%y-%b-%d") + "to" + date_end.strftime("%y-%b-%d")

    curtailment_folder = geopgraphy_folder + "/Curtailment - File Misc/Curtailment Input/"
    info_folder = geopgraphy_folder + "/Info&Templates/"
    dest_folder = geopgraphy_folder + "/Curtailment - File Misc/"
    dest_file = dest_folder + "Curtailment Results/Curtailment_" + geography + "_" + dest_file_suffix + ".xlsx"

    irradiance_file_path = geopgraphy_folder + "/Irradiance " + geography + "/Irradiance_corrected_" + geography + \
                          "_1m_" + str(year) + ".xlsx"  # will be picked by use
    export_file_path = geopgraphy_folder + "/Exported Energy " + geography + "/All_Power_Exported_" + geography + \
                     "_1m_" + str(year) + ".xlsx"

    general_info_path = info_folder + "General Info " + geography + ".xlsx"
    event_tracker_file_path = geopgraphy_folder + '/Event Tracker/Event Tracker ' + geography +  '.xlsx'

    active_power_setpoint_file_path= curtailment_folder + "PPC - Active Power setpoint_" + str(year) + ".xlsx"

    # </editor-fold>

    # <editor-fold desc="Read irradiance, export and setpoint data">
    print("Reading irradiance and export data...")
    df_all_irradiance, df_all_power = read_irradiance_export(irradiance_file_path, export_file_path)
    df_irradiance_period = df_all_irradiance[(df_all_irradiance["Timestamp"] > date_start) &
                                             (df_all_irradiance["Timestamp"] < date_end)]
    df_power_period = df_all_power[(df_all_power["Timestamp"] > date_start) &
                                        (df_all_power["Timestamp"] < date_end)]

    print("Reading active power setpoint data...")
    active_power_setpoint_df = pd.read_excel(active_power_setpoint_file_path, engine = 'openpyxl')
    active_power_setpoint_df['Timestamp'] = pd.to_datetime(active_power_setpoint_df['Timestamp'])
    active_power_setpoint_period = active_power_setpoint_df[(active_power_setpoint_df["Timestamp"] > date_start) &
                                                            (active_power_setpoint_df["Timestamp"] < date_end)]

    print("Reading general info data...")
    component_data, tracker_data, fmeca_data, site_capacities, fleet_capacity, budget_irradiance, budget_pr, \
    budget_export, all_site_info = get_general_info_dataframes(general_info_path)

    # </editor-fold>

    # <editor-fold desc="Read Event Tracker incidents">
    print("Reading incidents data...")
    df_eventtracker_all = pd.read_excel(event_tracker_file_path,
                                        sheet_name=['Active Events', 'Closed Events', 'Active tracker incidents',
                                                    'Closed tracker incidents', 'FMECA'], engine='openpyxl')

    df_active_eventtracker = df_eventtracker_all['Active Events']
    df_closed_eventtracker = df_eventtracker_all['Closed Events']

    # Create all component incidents df
    incidents = pd.concat([df_eventtracker_all['Active Events'], df_eventtracker_all['Closed Events']])

    # </editor-fold>

    print("Data acquisition complete")

    return df_irradiance_period, df_power_period, active_power_setpoint_period, component_data, tracker_data, fmeca_data, site_capacities, fleet_capacity, \
           budget_irradiance, budget_pr, budget_export, all_site_info, incidents, dest_file

def read_clipping_dataframes(source_folder, geography, geopgraphy_folder, site_selection, period,
                                irradiance_threshold: int = 20):

    # <editor-fold desc="turn all of this into a data acquistion function!!!">
    date_start_str, date_end_str = inputs.choose_period_of_analysis(period)
    date_start = datetime.strptime(date_start_str, '%Y-%m-%d')
    date_end = datetime.strptime(date_end_str, '%Y-%m-%d')

    year = date_start.year
    dest_file_suffix = date_start.strftime("%y-%b-%d") + "to" + date_end.strftime("%y-%b-%d")

    clipping_folder = geopgraphy_folder + "/Event Tracker/Clipping Analysis/"
    folder_img = clipping_folder + "images/"
    info_folder = geopgraphy_folder + "/Info&Templates/"

    dest_file = clipping_folder + "Clipping_Analysis_" + geography + "_" + dest_file_suffix + ".xlsx"


    irradiance_file_path = geopgraphy_folder + "/Irradiance " + geography + "/Irradiance_corrected_" + geography + \
                          "_1m_" + str(year) + ".xlsx"  # will be picked by use
    export_file_path = geopgraphy_folder + "/Exported Energy " + geography + "/All_Power_Exported_" + geography + \
                     "_1m_" + str(year) + ".xlsx"

    general_info_path = info_folder + "General Info " + geography + ".xlsx"
    event_tracker_file_path = geopgraphy_folder + '/Event Tracker/Event Tracker ' + geography +  '.xlsx'


    # </editor-fold>

    # <editor-fold desc="Read irradiance, export and setpoint data">
    print("Reading irradiance and export data...")
    df_all_irradiance, df_all_power = read_irradiance_export(irradiance_file_path, export_file_path)
    df_irradiance_period = df_all_irradiance[(df_all_irradiance["Timestamp"] > date_start) &
                                             (df_all_irradiance["Timestamp"] < date_end)]
    df_power_period = df_all_power[(df_all_power["Timestamp"] > date_start) &
                                        (df_all_power["Timestamp"] < date_end)]



    print("Reading general info data...")
    component_data, tracker_data, fmeca_data, site_capacities, fleet_capacity, budget_irradiance, budget_pr, \
    budget_export, all_site_info = get_general_info_dataframes(general_info_path)

    # </editor-fold>

    # <editor-fold desc="Read Event Tracker incidents">
    print("Reading incidents data...")
    df_eventtracker_all = pd.read_excel(event_tracker_file_path,
                                        sheet_name=['Active Events', 'Closed Events', 'Active tracker incidents',
                                                    'Closed tracker incidents', 'FMECA'], engine='openpyxl')

    df_active_eventtracker = df_eventtracker_all['Active Events']
    df_closed_eventtracker = df_eventtracker_all['Closed Events']

    # Create all component incidents df
    incidents = pd.concat([df_eventtracker_all['Active Events'], df_eventtracker_all['Closed Events']])

    # </editor-fold>

    print("Data acquisition complete")

    return df_irradiance_period, df_power_period, component_data, tracker_data, fmeca_data, site_capacities, fleet_capacity, \
           budget_irradiance, budget_pr, budget_export, all_site_info, incidents, dest_file, folder_img

# <editor-fold desc="ET Functions">

def get_files_to_add(date_start, date_end, dmr_folder, geography, no_update: bool = False):
    if no_update == False:
        if date_end == None:
            date_list = pd.date_range(date_start, date_start, freq='d')
        else:
            date_list = pd.date_range(date_start, date_end, freq='d')

        report_files_dict = {}
        irradiance_dict = {}
        export_dict = {}

        irradiance_folder = dmr_folder + "/Irradiance " + geography
        export_folder = dmr_folder + "/Exported Energy " + geography
        folder_content = os.listdir(dmr_folder)

        for date in date_list:
            #Get date info for name
            month = str("0" + str(date.month)) if date.month < 10 else str(date.month)
            day = str("0" + str(date.day)) if date.day < 10 else str(date.day)
            year = str(date.year)

            #Get each of the files to be used in each day
            report_file_prefix = 'Reporting_' + geography + '_Sites_' + str(date.date()).replace("-","")
            report_file_list = [dmr_folder + '/' + file for file in folder_content if report_file_prefix in file]

            irradiance_file = irradiance_folder + '/Irradiance_' + geography + '_Curated&Average-' + year + month + str(day) + '.xlsx'  # will be picked by user
            export_file = export_folder + '/Energy_Exported_' + geography + '_' + year + month + str(day) + '.xlsx'  # will be picked by user

            index_file = 1
            for file in report_file_list:
                report_files_dict[str(date) + str(index_file)] = file
                index_file += 1

            irradiance_dict[date] = irradiance_file
            export_dict[date] = export_file

        report_files = list(report_files_dict.values())
        irradiance_files = list(irradiance_dict.values())
        export_files = list(export_dict.values())

        all_irradiance_file = irradiance_folder + '/All_Irradiance_' + geography + '.xlsx'  # will be picked by use
        all_export_file = export_folder + '/All_Energy_Exported_' + geography + '.xlsx'  # will be picked by use
        general_info_path = dmr_folder + '/Info&Templates/General Info ' + geography + '.xlsx'  # will be picked by script

        return report_files, irradiance_files, export_files, all_irradiance_file, all_export_file, general_info_path

    else:
        irradiance_folder = dmr_folder + "/Irradiance " + geography
        export_folder = dmr_folder + "/Exported Energy " + geography

        all_irradiance_file = irradiance_folder + '/All_Irradiance_' + geography + '.xlsx'  # will be picked by use
        all_export_file = export_folder + '/All_Energy_Exported_' + geography + '.xlsx'  # will be picked by use
        general_info_path = dmr_folder + '/Info&Templates/General Info ' + geography + '.xlsx'  # will be picked by script

        return all_irradiance_file, all_export_file, general_info_path


def get_general_info_dataframes(general_info_path):
    # Read general info file
    general_info = pd.read_excel(general_info_path, sheet_name=['Site Info', 'Component Code'], engine='openpyxl')

    general_info_budgets = pd.read_excel(general_info_path, sheet_name=['Budget Irradiance', 'Budget PR',
                                                                        'Budget Export'], index_col=0, engine='openpyxl')

    all_component_data = general_info['Component Code']
    all_site_info = general_info['Site Info'].set_index('Site')

    budget_irradiance = general_info_budgets['Budget Irradiance']
    budget_pr = general_info_budgets['Budget PR']
    budget_export = general_info_budgets['Budget Export']


    # Separate data
    component_data = all_component_data.loc[
        (all_component_data['Component Type'] != 'Tracker') & (all_component_data['Component Type'] != 'Tracker Group')]
    tracker_data = all_component_data.loc[
        (all_component_data['Component Type'] == 'Tracker') | (all_component_data['Component Type'] == 'Tracker Group')]
    fmeca_data = pd.read_excel(general_info_path, sheet_name='FMECA', engine='openpyxl')

    site_capacities = component_data.loc[component_data['Component Type'] == 'Site'][
        ['Component', 'Nominal Power DC']].set_index('Component')
    fleet_capacity = site_capacities['Nominal Power DC'].sum()


    return component_data, tracker_data, fmeca_data, site_capacities, fleet_capacity, budget_irradiance, budget_pr, budget_export, all_site_info


def get_dataframes_to_add_to_EventTracker(report_files,event_tracker_file_path, fmeca_data,
                                          component_data, tracker_data):
    '''From Event Tracker & files, gets all dataframes to add separated by dictionaries.
    Returns: df_to_add - dict with new dfs to add
             df_event_tracker - dict with existing dfs in tracker
             fmeca_data - Corrected for Unnamed columns and incomplete entries'''

    #Dataframes from Event Tracker

    df_all = pd.read_excel(event_tracker_file_path,
                           sheet_name=['Active Events', 'Closed Events', 'Active tracker incidents',
                                       'Closed tracker incidents'], engine='openpyxl')

    df_active_eventtracker = df_all['Active Events']
    df_closed_eventtracker = df_all['Closed Events']
    df_active_eventtracker_trackers = df_all['Active tracker incidents']
    df_closed_eventtracker_trackers = df_all['Closed tracker incidents']

    #Dataframes to add
    for report_path in report_files:
        try:
            df_active_to_add_report = pd.read_excel(report_path, sheet_name='Active incidents', engine='openpyxl')
            df_closed_to_add_report = pd.read_excel(report_path, sheet_name='Closed incidents', engine='openpyxl')
        except ValueError:
            df_active_to_add_report = pd.read_excel(report_path, sheet_name='Active Events', engine='openpyxl')
            df_closed_to_add_report = pd.read_excel(report_path, sheet_name='Closed Events', engine='openpyxl')

        df_active_to_add_trackers_report = pd.read_excel(report_path, sheet_name='Active tracker incidents',
                                                         engine='openpyxl')
        df_closed_to_add_trackers_report = pd.read_excel(report_path, sheet_name='Closed tracker incidents',
                                                         engine='openpyxl')

        try:
            df_active_reports = df_active_reports.append(df_active_to_add_report)
        except NameError:
            df_active_reports = df_active_to_add_report

        try:
            df_closed_reports = df_closed_reports.append(df_closed_to_add_report)
        except NameError:
            df_closed_reports = df_closed_to_add_report

        try:
            df_active_reports_trackers = df_active_reports_trackers.append(df_active_to_add_trackers_report)
        except NameError:
            df_active_reports_trackers = df_active_to_add_trackers_report

        try:
            df_closed_reports_trackers = df_closed_reports_trackers.append(df_closed_to_add_trackers_report)
        except NameError:
            df_closed_reports_trackers = df_closed_to_add_trackers_report

    # Reset Index
    df_active_reports.reset_index(drop=True, inplace=True)
    df_closed_reports.reset_index(drop=True, inplace=True)
    df_active_reports_trackers.reset_index(drop=True, inplace=True)
    df_closed_reports_trackers.reset_index(drop=True, inplace=True)

    # Dicts with dataframes
    df_to_add = {'Closed Events': df_closed_reports,
                 'Closed tracker incidents': df_closed_reports_trackers,
                 'Active Events': df_active_reports,
                 'Active tracker incidents': df_active_reports_trackers}

    df_event_tracker = {'Closed Events': df_closed_eventtracker,
                        'Closed tracker incidents': df_closed_eventtracker_trackers,
                        'Active Events': df_active_eventtracker,
                        'Active tracker incidents': df_active_eventtracker_trackers}

    # Correct any unnamed columns
    fmeca_data = fmeca_data.loc[:, ~fmeca_data.columns.str.contains('^Unnamed')]
    fmeca_data = fmeca_data.dropna(thresh=8)

    # Correct unnamed columns
    for sheet in df_event_tracker:
        df = df_event_tracker[sheet]
        corrected_df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df_event_tracker[sheet] = corrected_df

    #Correct structure of df_to_add dfs to match Event Tracker
    for df_name_pair in df_to_add.items():

        df_data = df_name_pair[1]
        df_name = df_name_pair[0]
        print(df_name)
        #print(df_data)
        if "Closed" in df_name:
            active = False
        else:
            active = True

        if not "tracker" in df_name:
            component_data_effective = component_data
            tracker_check = False
        else:
            component_data_effective = tracker_data
            tracker_check = True

        # print(df_name, " corrected")
        df_corrected = data_treatment.match_df_to_event_tracker(df_data, component_data_effective, fmeca_data, active=active,
                                                    tracker = tracker_check)
        

        df_to_add[df_name] = df_corrected



    return df_to_add, df_event_tracker, fmeca_data

def get_final_dataframes_to_add_to_EventTracker(df_to_add, df_event_tracker, fmeca_data):
    '''From the different dataframe dictionaries available (New Reports to Add, Event Tracker info and FMECA data,
    creates dict with final dataframes to add.
    Events are verified, excludes from new additions any incident already on Event Tracker and removes from
    active sheet any closed incident.
    Returns: df_final_to_add'''

    final_df_to_add = {}
    # Choose which incidents to add to event tracker
    for sheet in df_to_add.keys():
        print("Joining new df to event tracker df - " , sheet)
        if "Closed" in sheet:
            df_toadd_id = df_to_add[sheet]['ID'].to_list()  # Get dataframe to add - Closed
            df_ET_id = df_event_tracker[sheet]['ID'].to_list()  # Get dataframe event tracker - Closed

            set_df_ET_id = set(df_ET_id)
            df_toadd_id_tokeep = [x for x in df_toadd_id if x not in set_df_ET_id]

            df_to_add[sheet] = df_to_add[sheet][df_to_add[sheet]['ID'].isin(df_toadd_id_tokeep)].reset_index(drop=True)

            # df_to_add_id = list(set(df_id) - set(df_ET_id))

        else:
            other_sheet = sheet.replace('Active', 'Closed')

            df_toadd_id = df_to_add[sheet]['ID'].to_list()  # Get active dataframe to add and all others

            df_closed_id = df_to_add[other_sheet]['ID'].to_list()
            df_ET_id = df_event_tracker[sheet]['ID'].to_list()
            df_ET_closed_id = df_event_tracker[other_sheet]['ID'].to_list()
            all_ids = df_closed_id + df_ET_id + df_ET_closed_id

            set_all_ids = set(all_ids)
            df_toadd_id_tokeep = [x for x in df_toadd_id if x not in set_all_ids]

            df_to_add[sheet] = df_to_add[sheet][df_to_add[sheet]['ID'].isin(df_toadd_id_tokeep)].reset_index(drop=True)

        # Join new events with events from event tracker and sort them
        if not df_to_add[sheet].empty:
            new_df = pd.concat([df_event_tracker[sheet], df_to_add[sheet]]).sort_values(
                by=['Site Name', 'Event Start Time', 'Related Component'], ascending=[True, False, False],
                ignore_index=True)
        else:
            new_df = df_event_tracker[sheet]
        final_df_to_add[sheet] = new_df

    # Correct final active lists to exclude already closed events
    for sheet in final_df_to_add.keys():
        print("Correcting final dfs to add - ", sheet)
        if "Active" in sheet:
            other_sheet = sheet.replace('Active', 'Closed')
            df_active = final_df_to_add[sheet]
            df_active_id = final_df_to_add[sheet]['ID'].to_list()

            df_closed = final_df_to_add[other_sheet]
            df_closed_id = final_df_to_add[other_sheet]['ID'].to_list()

            set_closed_ids = set(df_closed_id)
            df_tokeep_id = [x for x in df_active_id if x not in set_closed_ids]
            df_toremove_id = [x for x in df_active_id if x in set_closed_ids]

            for id_incident in df_toremove_id:
                index_closed = int(df_closed.loc[df_closed['ID'] == id_incident].index.values)
                index_active = int(df_active.loc[df_active['ID'] == id_incident].index.values)
                df_closed.loc[index_closed, 'Remediation'] = df_active.loc[index_active, 'Remediation']
                df_closed.loc[index_closed, 'Fault'] = df_active.loc[index_active, 'Fault']
                df_closed.loc[index_closed, 'Fault Component'] = df_active.loc[index_active, 'Fault Component']
                df_closed.loc[index_closed, 'Failure Mode'] = df_active.loc[index_active, 'Failure Mode']
                df_closed.loc[index_closed, 'Failure Mechanism'] = df_active.loc[index_active, 'Failure Mechanism']
                df_closed.loc[index_closed, 'Category'] = df_active.loc[index_active, 'Category']
                df_closed.loc[index_closed, 'Subcategory'] = df_active.loc[index_active, 'Subcategory']
                df_closed.loc[index_closed, 'Resolution Category'] = df_active.loc[index_active, 'Resolution Category']

            final_df_to_add[sheet] = final_df_to_add[sheet][
                final_df_to_add[sheet]['ID'].isin(df_tokeep_id)].reset_index(drop=True)
            final_df_to_add[other_sheet] = df_closed
        else:
            pass

        # final_df_to_add[sheet]
    for sheet, df in final_df_to_add.items():
        print("Correcting timestamps on final dfs to add - ", sheet)
        if "Active" in sheet:
            df['Event Start Time'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S') for timestamp in
                                      df['Event Start Time']]
            df.sort_values(by = ['ID', 'Event Start Time'], inplace = True,ascending=[True, False],ignore_index=True)
        else:
            df['Event Start Time'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S') for timestamp in
                                      df['Event Start Time']]
            df['Event End Time'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S') for timestamp in
                                    df['Event End Time']]
            df.sort_values(by=['Event Start Time', 'ID'], inplace=True, ascending=[False, False], ignore_index=True)
        final_df_to_add[sheet] = df

    final_df_to_add['FMECA'] = fmeca_data
    final_df_to_add = dict(sorted(final_df_to_add.items()))

    return final_df_to_add


# </editor-fold>

