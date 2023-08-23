import pandas as pd
from datetime import datetime
import datetime as dt
import re
import os
import PySimpleGUI as sg


def input_date(startend: str = "start"):

    hour = [*range(24)]
    for i in range(0, len(hour), 1):
        hour[i] = str(f'{hour[i]:02}')

    minutes = [*range(0, 46, 15)]
    for i in range(0, len(minutes), 1):
        minutes[i] = str(f'{minutes[i]:02}')

    # Create interface
    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.
    layout = [[sg.Text('Enter date of report you want to analyse')],
              [sg.CalendarButton('Choose ' + startend + ' date', target='-CAL-', format="%Y-%m-%d"),
               sg.In(key='-CAL-', text_color='black', size=(10, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Button('Submit'), sg.Exit()]]
    # Create the Window
    window = sg.Window('Choose date', layout)
    # Event Loop to process "events" and get the "values" of the inputs

    while True:
        event, values = window.read()

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            break
        if event == 'Submit':
            date = values['-CAL-']
            break
    window.close()

    return date

def input_file(desktop_path):

    # Create interface
    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.
    layout = [[sg.Text('Select "General Info" file of geography')],
              [sg.FileBrowse(target='-FILE-', initial_folder = desktop_path),
               sg.In(key='-FILE-', text_color='black', size=(20, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Button('Submit'), sg.Exit()]]
    # Create the Window
    window = sg.Window('Choose date', layout)
    # Event Loop to process "events" and get the "values" of the inputs

    while True:
        event, values = window.read()

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            break
        if event == 'Submit':
            file_path = values['-FILE-']
            return file_path

    window.close()

    return


def input_date_and_time():
    hour = [*range(24)]
    for i in range(0, len(hour), 1):
        hour[i] = str(f'{hour[i]:02}')

    minutes = [*range(0, 46, 15)]
    for i in range(0, len(minutes), 1):
        minutes[i] = str(f'{minutes[i]:02}')

    # Create interface
    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.
    layout = [[sg.Text('Enter date of report you want to analyse')],
              [sg.CalendarButton('Choose date that the event started', target='-CAL-', format="%Y-%m-%d"),
               sg.In(key='-CAL-', text_color='black', size=(10, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Text('Enter start time of event'), sg.Spin(hour, initial_value='07', size=(3, 2), key='-SHOUR-'),
               sg.Spin(minutes, initial_value='00', size=(3, 2), key='-SMIN-')],
              [sg.Button('Submit'), sg.Exit()]]
    # Create the Window
    window = sg.Window('Daily Monitoring Report', layout)
    # Event Loop to process "events" and get the "values" of the inputs

    while True:
        event, values = window.read()

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            break
        if event == 'Submit':
            date = values['-CAL-']

            stime_hour = values['-SHOUR-']
            stime_min = values['-SMIN-']

            stime = date + ' ' + stime_hour + ':' + stime_min + ':00'
            timestamp = datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')

            print(timestamp)
            break
    window.close()

    return timestamp


def input_time_operation_site(site, date):

    hour = [*range(24)]
    for i in range(0, len(hour), 1):
        hour[i] = str(f'{hour[i]:02}')

    minutes = [*range(0, 46, 15)]
    for i in range(0, len(minutes), 1):
        minutes[i] = str(f'{minutes[i]:02}')

    #Create interface
    sg.theme('DarkAmber')  # Add a touch of color

    # All the stuff inside your window.
    layout = [[sg.Text('Enter sunrise and sunset time for ' + site)],
              [sg.HorizontalSeparator(pad=((10, 10), (2, 10)))],
              [sg.Text('Enter sunrise hour'), sg.Spin(hour, initial_value='07', size=(3, 2), key='-SHOUR-'),
               sg.Spin(minutes, initial_value='00', size=(3, 2), key='-SMIN-')],
              [sg.Text('Enter sunset hour'), sg.Spin(hour, initial_value='19', size=(3, 2), key='-EHOUR-'),
               sg.Spin(minutes, initial_value='00', size=(3, 2), key='-EMIN-')],
              [sg.Button('Submit'), sg.Exit()]]
    # Create the Window
    window = sg.Window('Daily Monitoring Report', layout)
    # Event Loop to process "events" and get the "values" of the inputs

    while True:
        event, values = window.read()

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            break
        if event == 'Submit':
            stime_hour = values['-SHOUR-']
            stime_min = values['-SMIN-']

            print(date, type(date))

            stime = date + ' ' + stime_hour + ':' + stime_min + ':00'
            stime = datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')

            etime_hour = values['-EHOUR-']
            etime_min = values['-EMIN-']

            etime = date + ' ' + etime_hour + ':' + etime_min + ':00'
            etime = datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')

            print(stime)
            print(etime)
            break
    window.close()

    return stime, etime


def choose_period_of_analysis(granularity_avail, date: str = "", year_analysis: int = 0):
    """ input: option = ["mtd", "ytd", "monthly", "choose"], month_analysis, year_analysis

    output: start_date, end_date
    """

    possible_granularity_avail = ["mtd", "ytd", "monthly", "choose", "day"]
    current_day = (datetime.now()).day
    if not len(date) == 0:
        print(date, " ", type(date))
        date_start_str = date_end_str = str(date)

    else:
        if current_day == 1:
            actual_date = datetime.now() - dt.timedelta(days=1)
            year = actual_date.year
            month = actual_date.month
            day = actual_date.day
        else:
            year = datetime.now().year
            month = datetime.now().month
            day = current_day

        if granularity_avail == "mtd":

            date_start_str = str(year) + "-" + str(month) + "-01"
            if day < 10 and month < 10:
                date_end_str = str(year) + "-0" + str(month) + "-0" + str(day)
            elif day < 10:
                date_end_str = str(year) + "-" + str(month) + "-0" + str(day)
            elif month < 10:
                date_start_str = str(year) + "-0" + str(month) + "-01"
                date_end_str = str(year) + "-0" + str(month) + "-" + str(day)
            else:
                date_end_str = str(year) + "-" + str(month) + "-" + str(day)

        elif granularity_avail == "ytd":

            date_start_str = str(year) + "-01-01"
            if month < 10:
                month = "0" + str(month)

            if day < 10:
                date_end_str = str(year) + "-" + str(month) + "-0" + str(day)
            else:
                date_end_str = str(year) + "-" + str(month) + "-" + str(day)

        elif granularity_avail == "monthly":
            date_start_str = input_date(startend="start")
            date_start = datetime.strptime(date_start_str, '%Y-%m-%d')

            if year_analysis != 0:
                year = year_analysis
            else:
                year = date_start.year

            month = date_start.month
            if month < 12:
                day_end = (dt.date(year, month+1, 1) - dt.timedelta(days=1)).day
            else:
                day_end = 31

            if 10 > month > 0:
                month = "0" + str(month)
            elif 10 <= month <= 12:
                pass
            else:
                print("Month chosen invalid, you chose: " + str(month) + " \n please chose a number between 1 and 12")

            date_start_str = str(year) + "-" + str(month) + "-01"
            date_end_str = str(year) + "-" + str(month) + "-" + str(day_end)

        elif granularity_avail == "choose":
            date_start_str = input_date(startend="start")
            date_end_str = input_date(startend="end")
            # using custom start dates

        elif granularity_avail == "day":
            date_start_str = date_end_str = input_date(startend="start")
            # using custom start dates

        else:
            print("Invalid input from period of availability calculation. You entered: " + str(granularity_avail) +
                  ". \n Please choose one of the following ['mtd', 'ytd', 'custom', 'choose'].")

    return date_start_str, date_end_str


def choose_incidents_files():
    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.
    layout = [[sg.Text('Enter date of report you want to analyse', pad=((2, 10), (2, 5)))],
              [sg.CalendarButton('Choose date', target='-CAL-', format="%Y-%m-%d"),
               sg.In(key='-CAL-', text_color='black', size=(16, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Text('Choose Incidents file', pad=((0, 10), (10, 2)))],
              [sg.FileBrowse(target='-FILE-'),
               sg.In(key='-FILE-', text_color='black', size=(20, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Text('Choose Tracker Incidents file', pad=((0, 10), (10, 2)))],
              [sg.FileBrowse(target='-TFILE-'),
               sg.In(key='-TFILE-', text_color='black', size=(20, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Text('Enter geography ', pad=((0, 10), (10, 2)))],
              [sg.Combo(['AUS', 'ES', 'USA'], size=(4, 3), readonly=True, key='-GEO-', pad=((5, 10), (2, 10)))],
              [sg.Button('Submit'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Daily Monitoring Report', layout, modal=True)
    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read()

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            window.close()
            return "No File", "No File", ["No site list"], "PT", "27-03-1996"

        if event == 'Submit':
            date = values['-CAL-']  # date is string
            date_object = datetime.strptime(date, '%Y-%m-%d')
            day = date_object.day
            month = date_object.month
            year = date_object.year
            if day < 10:
                strday = str(0) + str(day)
            else:
                strday = str(day)

            if month < 10:
                strmonth = str(0) + str(month)
            else:
                strmonth = str(month)

            #stryear = str(year)
            date_to_test = strday + '-' + strmonth

            #Get files name+path
            incidents_file = values['-FILE-']
            tracker_incidents_file = values['-TFILE-']

            # Get file names
            incidents_file_name = os.path.basename(incidents_file)
            tracker_incidents_file_name = os.path.basename(tracker_incidents_file)

            #Get geography and dates
            geography_incidents_file_match = re.search(r'\-\w+\.', incidents_file_name)
            geography_incidents_file = geography_incidents_file_match.group()[3:-1]
            date_incidents_file_match = re.search(r'\d\d\-\d\d', incidents_file_name)
            date_incidents_file = date_incidents_file_match.group()

            geography_tracker_file_match = re.search(r'\-\w+\.', tracker_incidents_file_name)
            geography_tracker_file = geography_tracker_file_match.group()[3:-1]
            date_tracker_incidents_file_match = re.search(r'\d\d\-\d\d', tracker_incidents_file_name)
            date_tracker_incidents_file = date_tracker_incidents_file_match.group()

            geography = values['-GEO-']

            #print(strday)
            #print(strmonth)
            #print(stryear)
            #print(date_incidents_file)
            #print(date_tracker_incidents_file)
            #print(geography)
            #print(geography_incidents_file)
            #print(geography_tracker_file)

            if not date_incidents_file == date_to_test:
                sg.popup('Incidents file is not the correct one, you chose the file from ' + date_incidents_file
                         + ' and the following date: ' + date_to_test)

            elif not date_tracker_incidents_file == date_to_test:
                sg.popup('Incidents file is not the correct one, you chose the file from ' + date_tracker_incidents_file
                         + ' and the following date: ' + date_to_test)

            elif not geography == geography_incidents_file:
                sg.popup('Selected Geography ' + geography + ' does not match geography from file '
                      + geography_incidents_file)

            elif not geography == geography_tracker_file:
                sg.popup('Selected Geography ' + geography + ' does not match geography from tracker file: '
                      + geography_tracker_file)

            elif "Incidents" not in incidents_file or "Tracker_Incidents" not in tracker_incidents_file:
                sg.popup('Files are not correct. \n' + '\n' + incidents_file
                         + ' has to be like: "Incidents01-07USA.xlsx" \n' + '\n' + tracker_incidents_file
                         + ' has to be like: "Tracker_Incidents01-07USA.xlsx"')

            elif "Incidents" in incidents_file and "Tracker_Incidents" in tracker_incidents_file:
                site_list = pd.read_excel(incidents_file, sheet_name='Info', engine="openpyxl")['Site'].tolist()
                sg.popup('Submitted files are correct: \n' + '\nIncidents file: \n' + incidents_file
                         + '\n' + '\nTracker Incidents file: \n' + tracker_incidents_file + '\n' + '\nSite list: \n'
                         + str(site_list), no_titlebar=True)
                break

    window.close()

    return incidents_file, tracker_incidents_file, site_list, geography, date


def set_time_of_operation(reportfiletemplate, site_list, date):

    df_info_sunlight = pd.read_excel(reportfiletemplate, sheet_name='Info', engine="openpyxl")
    ignore_site = ['HV Almochuel']

    for site in site_list:
        if "HV Almochuel" in site:
            print(site + ' was ignored')
            pass
        else:
            index_array = df_info_sunlight[df_info_sunlight['Site'] == site].index.values
            index = int(index_array[0])

            stime, etime = input_time_operation_site(site, date)

            df_info_sunlight.loc[index, 'Time of operation start'] = stime
            df_info_sunlight.loc[index, 'Time of operation end'] = etime

    return df_info_sunlight


# <editor-fold desc="ET Functions">

def collapse(layout, key, visible):
    """
    Helper function that creates a Column that can be later made hidden, thus appearing "collapsed"
    :param layout: The layout for the section
    :param key: Key used to make this section visible / invisible
    :param visible: visible determines if section is rendered visible or invisible on initialization
    :return: A pinned column that can be placed directly into your layout
    :rtype: sg.pin
    """
    return sg.pin(sg.Column(layout, key=key, visible=visible, pad=(0,0)))


def new_event_tracker():
    username = os.getlogin()

    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.

    layout = [[sg.Text('Choose the source of information:', pad=((2, 10), (2, 5)))],
              [sg.Radio('One file', group_id="source", default=True, key="-SRCOF-"),
               sg.Radio('DMR', group_id="source", disabled=True, default=False, key="-SRCDMR-")],
              [sg.Text('Select source of Desktop', pad=((0, 10), (10, 2)))],
              [sg.FolderBrowse(target='-SRCFOLDER-',
                               initial_folder="C:/Users/" + username + "/OneDrive - Lightsource BP/Desktop"),
               sg.In(key='-SRCFOLDER-', text_color='black', size=(20, 1), enable_events=True, readonly=True,
                     visible=True)],
              [sg.Text('Select report', pad=((0, 10), (10, 2)))],
              [sg.FileBrowse(target='-SRCFILE-',
                             initial_folder="C:/Users/" + username + "/OneDrive - Lightsource BP/Desktop"),
               sg.In(key='-SRCFILE-', text_color='black', size=(20, 1), enable_events=True, readonly=True,
                     visible=True)],
              [sg.Text('Enter geography ', pad=((0, 10), (10, 2))), sg.Push()],
              [sg.Combo(['AUS', 'ES', 'USA'], size=(4, 3), readonly=True, key='-GEO-', pad=((5, 10), (2, 10))),
               sg.Push(), sg.Checkbox('Recalculate All', enable_events=True, size=(13, 3), pad=((20, 0), (0, 10)),
                                      key='chk_recalc')],
              [sg.Button('Submit'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Event Tracker', layout)

    # toggle_sec1 = False
    # toggle_updt = True
    toggle_recalc = False

    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read(timeout=100)

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            window.close()
            return "None", "None", "None", "None", "None"
            break

        if event == 'chk_recalc':
            toggle_recalc = not toggle_recalc

        if event == 'Submit':

            source_folder = values['-SRCFOLDER-']
            source = values['-SRCFILE-']
            geography = values['-GEO-']
            geopgraphy_folder = source_folder + "/" + geography
            dest_file = geopgraphy_folder + '/Event Tracker/Event Tracker ' + geography + '.xlsx'
            folder_img = geopgraphy_folder + '/Event Tracker/images'

            for key in values.keys():
                if "SRC" in key and values[key] is True:
                    if "OF" in key:
                        source_type = "one-file"
                    elif "DMR" in key:
                        source_type = "dmr"

            return source_folder, source, geography, geopgraphy_folder, dest_file, folder_img, toggle_recalc
            window.close()


    window.close()

    return


def update_event_tracker():
    username = os.getlogin()
    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.

    end_date_calendar_section = [[sg.Text('Enter end date of period you want to add', pad=((2, 10), (2, 5)))],
                                 [sg.CalendarButton('Choose date', target='-ECAL-', format="%Y-%m-%d"),
                                  sg.In(key='-ECAL-', text_color='black', size=(16, 1), enable_events=True,
                                        readonly=True, visible=True)]]

    layout = [[sg.Text('Enter date of report you want to add', pad=((2, 10), (2, 5)))],
              [sg.CalendarButton('Choose date', target='-SCAL-', format="%Y-%m-%d"),
               sg.In(key='-SCAL-', text_color='black', size=(16, 1), enable_events=True, readonly=True, visible=True),
               sg.Checkbox('Multiple reports', enable_events=True, size=(13, 1), key='chk_multr')],
              [collapse(end_date_calendar_section, '-EXCAL-', False)],
              [sg.Text('Choose source from Desktop', pad=((0, 10), (10, 2)))],
              [sg.FolderBrowse(target='-DMRFOLDER-', initial_folder="C:/Users/" + username +
                                                                    "/OneDrive - Lightsource BP/Desktop"),
               sg.In(key='-DMRFOLDER-', text_color='black', size=(20, 1), enable_events=True, readonly=True,
                     visible=True)],
              [sg.Text('Enter geography ', pad=((0, 10), (10, 2))), sg.Push(),
               sg.Checkbox('Update All Export\n& Irradiance', default=True, enable_events=True, size=(13, 3),
                           pad=((20, 0), (0, 10)), key='chk_updt')],
              [sg.Combo(['AUS', 'ES', 'USA'], size=(4, 3), readonly=True, key='-GEO-', pad=((5, 10), (2, 10))),
               sg.Push(), sg.Checkbox('Recalculate All', enable_events=True, size=(13, 3), pad=((20, 0), (0, 10)),
                                      key='chk_recalc')],
              [sg.Button('Submit'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Update Event Tracker', layout)

    toggle_sec1 = False
    toggle_updt = True
    toggle_recalc = False

    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read(timeout=100)

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            window.close()
            return "None", "None", "None", "None", "None", "None", "None"

        if event == 'chk_multr':
            toggle_sec1 = not toggle_sec1
            window['-EXCAL-'].update(visible=toggle_sec1)

        if event == 'chk_updt':
            toggle_updt = not toggle_updt

        if event == 'chk_recalc':
            toggle_recalc = not toggle_recalc

        if event == 'Submit':
            date_start = values['-SCAL-']  # date is string
            date_end = values['-ECAL-']
            geography = values['-GEO-']
            dmr_folder = values['-DMRFOLDER-'] + "/" + geography
            event_tracker_path = dmr_folder + "/Event Tracker/Event Tracker " + geography + ".xlsx"

            if date_end == "":
                date_end = None
                print('date end value changed to none')
            if toggle_sec1 is False:
                date_end = None

            window.close()
            return date_start, date_end, event_tracker_path, dmr_folder, geography, toggle_updt, toggle_recalc

    # window.close()

    return


def event_tracker():
    username = os.getlogin()

    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.

    layout = [[sg.Text('Choose the source of information:', pad=((2, 10), (2, 5)))],
              [sg.Radio('Database', group_id="source", disabled=True, default=False, key="-SRCDB-"),
               sg.Radio('Event Tracker file', group_id="source", default=True, key="-SRCFILE-")],
              [sg.Text('Select source on Desktop', pad=((0, 10), (10, 2)))],
              [sg.FolderBrowse(target='-SRCFOLDER-',
                               initial_folder="C:/Users/" + username + "/OneDrive - Lightsource BP/Desktop"),
               sg.In(key='-SRCFOLDER-', text_color='black', size=(20, 1), enable_events=True, readonly=True,
                     visible=True)],
              [sg.Text('Enter geography ', pad=((0, 10), (10, 2))), sg.Push()],
              [sg.Combo(['AUS', 'ES', 'USA'], size=(4, 3), readonly=True, key='-GEO-', pad=((5, 10), (2, 10))),
               sg.Push(), sg.Checkbox('Recalculate All', enable_events=True, size=(13, 3), pad=((20, 0), (0, 10)),
                                      key='chk_recalc')],
              [sg.Button('Submit'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Event Tracker', layout)

    #toggle_sec1 = False
    #toggle_updt = True
    toggle_recalc = False

    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read(timeout=100)

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            window.close()
            return "None", "None", "None", "None"

        if event == 'chk_recalc':
            toggle_recalc = not toggle_recalc

        if event == 'Submit':

            source_folder = values['-SRCFOLDER-']
            geography = values['-GEO-']
            geopgraphy_folder = source_folder + "/" + geography

            for key in values.keys():
                if "SRC" in key and values[key] == True:
                    if "FILE" in key:
                        source_type = "file"
                    elif "DB" in key:
                        source_type = "database"

            window.close()
            return source_folder, geography, geopgraphy_folder, toggle_recalc


    window.close()

    return


def underperformance_report(site_list, pre_selection):
    username = os.getlogin()

    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.

    month_calendar_section = [[sg.Text('Enter month of analysis', pad=((2, 10), (2, 5)))],
                              [sg.CalendarButton('Choose date', target='-ECAL-', format="%Y-%m-%d"),
                               sg.In(key='-SCAL-', text_color='black', size=(16, 1), enable_events=True,
                                     readonly=True, visible=True)]]

    custom_calendar_section = [[sg.Text('Enter start date of period you want to analyse', pad=((2, 10), (2, 5)))],
                               [sg.CalendarButton('Choose date', target='-SCAL-', format="%Y-%m-%d"),
                                sg.In(key='-SCAL-', text_color='black', size=(16, 1), enable_events=True,
                                      readonly=True, visible=True)],
                               [sg.Text('Enter end date of period you want to analyse', pad=((2, 10), (2, 5)))],
                               [sg.CalendarButton('Choose date', target='-ECAL-', format="%Y-%m-%d"),
                                sg.In(key='-ECAL-', text_color='black', size=(16, 1), enable_events=True,
                                      readonly=True, visible=True)]]

    layout = [[sg.Text('Choose the source of information:', pad=((2, 10), (2, 5)))],
              [sg.Radio('Month', group_id="period", default=False, key="-PERMON-"),
               sg.Radio('Choose', group_id="period", default=True, key="-PERCHO-")],
              [sg.Text('Choose the period of analysis:', pad=((2, 10), (2, 5)))],
              [sg.Radio('Database', group_id="source", disabled=True, default=False, key="-SRCDB-"),
               sg.Radio('Event Tracker file', group_id="source", default=True, key="-SRCFILE-")],
              [sg.Text('Select source on Desktop', pad=((0, 10), (10, 2)))],
              [sg.FolderBrowse(target='-SRCFOLDER-',
                               initial_folder="C:/Users/" + username + "/OneDrive - Lightsource BP/Desktop"),
               sg.In(key='-SRCFOLDER-', text_color='black', size=(20, 1), enable_events=True, readonly=True,
                     visible=True)],
              [sg.Text('Enter geography ', pad=((0, 10), (10, 2))), sg.Push()],
              [sg.Combo(['AUS', 'ES', 'USA'], size=(4, 3), readonly=True, key='-GEO-', pad=((5, 10), (2, 10))),
               sg.Push(),
               sg.Checkbox('Recalculate All', enable_events=True, size=(13, 3), pad=((20, 0), (0, 10)),
                           key='chk_recalc')],
              [sg.Text('Select level of analysis', pad=((0, 10), (10, 2))), sg.Push(),
               sg.Text('Select Irradiance Threshold', pad=((0, 10), (10, 2))), sg.Push()],
              [sg.Combo(['All', 'Inverter level', 'Inverter only', 'No Inverter Modules'], default_value="All",
                        size=(18, 3), readonly=True,
                        key='-LVL-', pad=((5, 10), (2, 10))),
               sg.Combo([20, 50, 85, 100], default_value=50, size=(11, 3), readonly=True, key='-THR-',
                        pad=((50, 10), (2, 10)))],
              [sg.Button('Submit'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Event Tracker', layout)

    #toggle_sec1 = False
    #toggle_updt = True
    toggle_recalc = False

    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read(timeout=100)

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            break

        if event == 'chk_recalc':
            toggle_recalc = not toggle_recalc

        if event == 'Submit':

            source_folder = values['-SRCFOLDER-']
            level = values['-LVL-']
            irradiance_threshold = values['-THR-']
            geography = values['-GEO-']
            geopgraphy_folder = source_folder + "/" + geography

            for key in values.keys():
                if "SRC" in key and values[key] is True:
                    if "FILE" in key:
                        source_type = "file"
                    elif "DB" in key:
                        source_type = "database"

                elif "PER" in key and values[key] is True:
                    if "CHO" in key:
                        period_list = ["choose"]
                    elif "MON" in key:
                        period_list = ["monthly"]

            return source_folder, geography, geopgraphy_folder, toggle_recalc, period_list, level, irradiance_threshold

    window.close()

    return


def mondaycom_file():
    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.

    end_date_calendar_section = [[sg.Text('Enter end date of period you want to add', pad=((2, 10), (2, 5)))],
                                 [sg.CalendarButton('Choose date', target='-ECAL-', format="%Y-%m-%d"),
                                  sg.In(key='-ECAL-', text_color='black', size=(16, 1), enable_events=True,
                                        readonly=True, visible=True)]]

    layout = [[sg.Text('Enter date of report you want to add', pad=((2, 10), (2, 5)))],
              [sg.CalendarButton('Choose date', target='-SCAL-', format="%Y-%m-%d"),
               sg.In(key='-SCAL-', text_color='black', size=(16, 1), enable_events=True, readonly=True, visible=True),
               sg.Checkbox('Multiple reports', enable_events=True, size=(13, 1), key='chk_multr')],
              [collapse(end_date_calendar_section, '-EXCAL-', False)],
              [sg.Text('Choose location folder of Event Tracker', pad=((0, 10), (10, 2)))],
              [sg.FolderBrowse(target='-ETFOLDER-'),
               sg.In(key='-ETFOLDER-', text_color='black', size=(20, 1), enable_events=True, readonly=True,
                     visible=True)],
              [sg.Text('Enter geography ', pad=((0, 10), (10, 2))), sg.Push()],
              [sg.Combo(['AUS', 'ES', 'USA'], size=(4, 3), readonly=True, key='-GEO-', pad=((5, 10), (2, 10))),
               sg.Push()],
              [sg.Button('Submit'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Monday.com files', layout)

    toggle_sec1 = False

    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read(timeout=100)

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            window.close()
            return "None", "None", "None", "None"
            break
        if event == 'chk_multr':
            toggle_sec1 = not toggle_sec1
            window['-EXCAL-'].update(visible=toggle_sec1)

        if event == 'Submit':
            date_start = values['-SCAL-']  # date is string
            date_end = values['-ECAL-']
            event_tracker_folder = values['-ETFOLDER-']
            geography = values['-GEO-']

            if date_end == "":
                date_end = date_start
            if toggle_sec1 == False:
                date_end = date_start

            return date_start, date_end, event_tracker_folder, geography,

            window.close()

    window.close()

    return

# </editor-fold>
