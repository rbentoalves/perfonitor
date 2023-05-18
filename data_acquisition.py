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
                                               sheet_name=["Active Events", "Active tracker incidents"],
                                               engine="openpyxl")
        #all_prev_active_events = pd.concat([all_prev_active_events['Active Events'], all_prev_active_events['Active tracker incidents']])
        #df_all = pd.concat([df_all, all_prev_active_events['Active Events'], all_prev_active_events['Active tracker incidents']])[df_all_columns]

        prev_active_events = all_prev_active_events['Active Events']
        prev_active_tracker_events = all_prev_active_events['Active tracker incidents']

        print(df_all.columns)
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


def read_time_of_operation(irradiance_df, Report_template_path, withmean: bool = False):

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
