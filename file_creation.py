import pandas as pd
from append_df_to_excel import append_df_to_excel
from datetime import datetime
import datetime as dt
import os
import re
import PySimpleGUI as sg
import data_acquisition
import inputs
import data_treatment
import data_analysis


#File creation/edit/removal-------------------------------------------------------------------------------------

def add_incidents_to_excel(dest_file, site_list, df_list_active,
                           df_list_closed, df_info_sunlight, final_irradiance_data):

    """USAGE: add_incidents_to_excel(destiny_file,site_list,df_list_active,df_list_closed)"""

    append_df_to_excel(dest_file, df_info_sunlight, sheet_name='Info', startrow=0)
    append_df_to_excel(dest_file, final_irradiance_data, sheet_name='Irradiance', startrow=0)

    for site in site_list:
        if "LSBP - " in site or "LSBP – " in site:
            onlysite = site[7:]
        else:
            onlysite = site
        if onlysite[-1:] == " ":
            active_sheet_name = onlysite + 'Active'
            closed_sheet_name = onlysite[:len(onlysite)-1]
        else:
            active_sheet_name = onlysite + ' Active'
            closed_sheet_name = onlysite

        df_active = df_list_active[site]
        df_closed = df_list_closed[site]

        df_closed['Status of incident'] = 'Closed'
        df_active['Status of incident'] = 'Active'
        df_active['Action required'] = ''

        append_df_to_excel(dest_file, df_closed, sheet_name=closed_sheet_name)

        print('Active events of ' + site + ' added')
        append_df_to_excel(dest_file, df_active, sheet_name=active_sheet_name)
        print('Closed events of ' + site + ' added')

    return


def add_tracker_incidents_to_excel(dest_tracker_file, df_tracker_active, df_tracker_closed, df_tracker_info):

    """USAGE: add_tracker_incidents_to_excel(dest_file, df_tracker_active, df_tracker_closed, df_tracker_info)"""

    append_df_to_excel(dest_tracker_file, df_tracker_info, sheet_name='Trackers info', startrow=0)
    print('Tracker Info added')

    df_tracker_closed['Status of incident'] = 'Closed'
    df_tracker_active['Status of incident'] = 'Active'
    df_tracker_active['Action required'] = ''

    append_df_to_excel(dest_tracker_file, df_tracker_active, sheet_name='Active tracker incidents')
    print('Active tracker incidents added')
    append_df_to_excel(dest_tracker_file, df_tracker_closed, sheet_name='Closed tracker incidents')
    print('Closed tracker incidents added')

    return


def add_events_to_final_report(reportfile, df_list_active, df_list_closed, df_tracker_active, df_tracker_closed):

    final_active_events_list = pd.concat(list(df_list_active.values()))
    final_closed_events_list = pd.concat(list(df_list_closed.values()))

    if not final_active_events_list.empty:
        append_df_to_excel(reportfile, final_active_events_list, sheet_name='Active Events', startrow=0)
        print('Active events added')
    else:
        print('No active events to be added')

    if not final_closed_events_list.empty:
        append_df_to_excel(reportfile, final_closed_events_list, sheet_name='Closed Events', startrow=0)
        print('Closed events added')
    else:
        print('No closed events to be added')

    if not df_tracker_active.empty:
        append_df_to_excel(reportfile, df_tracker_active, sheet_name='Active tracker incidents', startrow=0)
        print('Tracker active events added')
    else:
        print('No tracker active events to be added')

    if not df_tracker_closed.empty:
        append_df_to_excel(reportfile, df_tracker_closed, sheet_name='Closed tracker incidents', startrow=0)
        print('Tracker closed events added')
    else:
        print('No tracker closed events to be added')

    return


def add_analysis_to_reportfile(reportfile, df_incidents_analysis, df_tracker_analysis, df_info_sunlight):

    # Add Info sheet
    append_df_to_excel(reportfile, df_info_sunlight, sheet_name='Info', startrow=0)

    # Add component failure analysis
    append_df_to_excel(reportfile, df_incidents_analysis, sheet_name='Analysis of CE', startrow=0)

    # Add component failure analysis
    append_df_to_excel(reportfile, df_tracker_analysis, sheet_name='Analysis of tracker incidents', startrow=0)

    return


def update_dump_file(irradiance_files, all_irradiance_file, data_type: str = 'Irradiance'):
    df_all_irradiance = pd.read_excel(all_irradiance_file, engine='openpyxl')

    df_irradiance_day_list = [pd.read_excel(file, engine='openpyxl') for file in irradiance_files]
    df_all_irradiance_list = df_irradiance_day_list.append(df_all_irradiance)

    df_all_irradiance_new = pd.concat(df_irradiance_day_list)
    df_all_irradiance_new['Timestamp'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S') for timestamp in
                                          df_all_irradiance_new['Timestamp']]

    df_all_irradiance_new = df_all_irradiance_new.loc[:, ~df_all_irradiance_new.columns.str.contains('^Unnamed')].\
        drop_duplicates(subset=['Timestamp'], keep='first', ignore_index=True).sort_values(
        by=['Timestamp'], ascending=[True], ignore_index=True)

    writer_irr = pd.ExcelWriter(all_irradiance_file, engine='xlsxwriter',
                                engine_kwargs={'options': {'strings_to_numbers': True}})

    workbook_irr = writer_irr.book

    df_all_irradiance_new.to_excel(writer_irr, sheet_name='All ' + str(data_type), index=False)

    writer_irr.save()

    return df_all_irradiance_new


def dmr_create_incidents_files(alarm_report_path, irradiance_file_path, geography, date):
    ar_dir = os.path.dirname(alarm_report_path)
    #print("this is dir: " + dir)

    previous_date = datetime.strptime(date, '%Y-%m-%d') - dt.timedelta(days=1)
    prev_day = str(previous_date.day) if previous_date.day >= 10 else str(0) + str(previous_date.day)
    prev_month = str(previous_date.month) if previous_date.month >= 10 else str(0) + str(previous_date.month)

    report_template_path = ar_dir + '/Info&Templates/Reporting_' + geography + '_Sites_Template.xlsx'
    general_info_path = ar_dir + '/Info&Templates/General Info ' + geography + '.xlsx'
    event_tracker_path = ar_dir + '/Event Tracker/Event Tracker ' + geography + '.xlsx'
    previous_dmr_path = ar_dir + '/Reporting_' + geography + '_Sites_' + prev_day + '-' + prev_month + '.xlsx'

    print(previous_dmr_path)

    #READ FILES AND EXTRACT RAW DATAFRAMES
    print('Reading Daily Alarm Report...')
    df_all, incidents_file, tracker_incidents_file, irradiance_file_data, prev_active_events, \
    prev_active_tracker_events = data_acquisition.read_daily_alarm_report(alarm_report_path,
                                                         irradiance_file_path, event_tracker_path, previous_dmr_path)

    print('Daily Alarm Report read!')
    print('newfile: ' + incidents_file)
    print('newtrackerfile: ' + tracker_incidents_file)
    print(df_all)
    print('Reading trackers info...')
    df_general_info, df_general_info_calc, all_component_data = data_acquisition.read_general_info(general_info_path)
    print('Trackers info read!')

    # DIVIDE RAW DATAFRAMES INTO LIST OF DATAFRAMES BY SITE
    print('Creating incidents dataframes list...')
    site_list, df_list_active, df_list_closed = data_treatment.create_dfs(df_all, min_dur=1, roundto=1)
    print('Incidents dataframes list created')
    print('Creating tracker dataframes...')
    df_tracker_active, df_tracker_closed = data_treatment.create_tracker_dfs(df_all, df_general_info_calc, roundto=1)
    print('Tracker dataframes created')
    print('Please set time of operation')
    df_info_sunlight, final_irradiance_data = data_acquisition.read_time_of_operation(irradiance_file_data,
                                                                     report_template_path, withmean=False)

    #df_info_sunlight = msp.set_time_of_operation(Report_template_path, site_list, date)
    print('Removing incidents occurring after sunset')
    df_list_closed = data_treatment.remove_after_sunset_events(site_list, df_list_closed, df_info_sunlight)
    df_list_active = data_treatment.remove_after_sunset_events(site_list, df_list_active,
                                                               df_info_sunlight, active_df=True)

    df_tracker_closed = data_treatment.remove_after_sunset_events(site_list, df_tracker_closed,
                                                                  df_info_sunlight, tracker=True)

    df_tracker_active = data_treatment.remove_after_sunset_events(site_list, df_tracker_active,
                                                                  df_info_sunlight,active_df=True, tracker=True)
    print('Adding component capacities')
    # ADD CAPACITIES TO DFS
    df_list_closed = data_treatment.complete_dataset_capacity_data(df_list_closed, all_component_data)
    df_list_active = data_treatment.complete_dataset_capacity_data(df_list_active, all_component_data)

    # JOIN INCIDENT TABLES AND DMR TABLES
    df_list_active = data_treatment.complete_dataset_existing_incidents(df_list_active, prev_active_events)
    df_tracker_active = pd.concat([df_tracker_active, prev_active_tracker_events])

    # CREATE INCIDENTS FILE

    print('Creating Incidents file...')
    print(incidents_file)
    add_incidents_to_excel(incidents_file, site_list, df_list_active, df_list_closed, df_info_sunlight, final_irradiance_data)
    print('Incidents file created!')
    print('Creating tracker incidents file...')
    add_tracker_incidents_to_excel(tracker_incidents_file, df_tracker_active, df_tracker_closed, df_general_info)
    print('Tracker incidents file created!')

    return incidents_file, tracker_incidents_file, site_list, all_component_data


def dmrprocess1():
    sg.theme('DarkAmber')  # Add a touch of color
    # All the stuff inside your window.
    layout = [[sg.Text('Enter date of report you want to analyse', pad=((2, 10), (2, 5)))],
              [sg.CalendarButton('Choose date', target='-CAL-', format="%Y-%m-%d"),
               sg.In(key='-CAL-', text_color='black', size=(16, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Text('Choose Alarm report', pad=((0, 10), (10, 2)))],
              [sg.FileBrowse(target='-FILE-'),
               sg.In(key='-FILE-', text_color='black', size=(20, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Text('Choose Irradiance file', pad=((0, 10), (10, 2)))],
              [sg.FileBrowse(target='-IRRFILE-'),
               sg.In(key='-IRRFILE-', text_color='black', size=(20, 1), enable_events=True, readonly=True, visible=True)],
              [sg.Text('Enter geography ', pad=((0, 10), (10, 2)))],
              [sg.Combo(['AUS', 'ES', 'USA'], size=(4, 3), readonly=True, key='-GEO-', pad=((5, 10), (2, 10)))],
              [sg.Button('Create Incidents List'), sg.Exit()]]

    # Create the Window
    window = sg.Window('Daily Monitoring Report', layout, modal=True)
    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read()

        if event == sg.WIN_CLOSED or event == 'Exit':  # if user closes window or clicks exit
            window.close()
            return "No File", "No File", ["No site list"],"PT", "27-03-1996"
            break
        if event == 'Create Incidents List':
            date = values['-CAL-']  # date is string
            Alarm_report_path = values['-FILE-']
            irradiance_file_path = values['-IRRFILE-']

            report_name = os.path.basename(Alarm_report_path)
            geography_report_match = re.search(r'\w+?_', report_name)
            geography_report = geography_report_match.group()[:-1]
            geography = values['-GEO-']

            #print(date[:4])
            #print(date[5:7])
            #print(date[-2:])
            print(Alarm_report_path)
            print(geography)
            print(geography_report)
            if "Daily" and "Alarm" and "Report" in Alarm_report_path and geography == geography_report and\
                    "Irradiance" in irradiance_file_path :

                incidents_file, tracker_incidents_file, site_list, all_component_data = dmr_create_incidents_files(
                    Alarm_report_path,irradiance_file_path, geography, date)
                sg.popup('All incident files are ready for approval', no_titlebar=True)
                window.close()
                return incidents_file, tracker_incidents_file, site_list, geography, date,all_component_data
                break
            elif not geography == geography_report:
                msg = 'Selected Geography ' + geography + ' does not match geography from report ' + geography_report
                sg.popup(msg, title = "Error with the selections")
                #print('Selected Geography ' + geography + ' does not match geography from report ' + geography_report)
            elif not "Daily" and "Alarm" and "Report" in Alarm_report_path:
                msg='File is not a Daily Alarm Report'
                sg.popup(msg, title = "Error with the selections")
                #print('File is not a Daily Alarm Report')
            elif not "Irradiance" in irradiance_file_path:
                msg = 'File is not a Irradiance file'
                sg.popup(msg, title = "Error with the selections")
                #print('File is not a Daily Alarm Report')
    window.close()

    return


def dmrprocess2(incidents_file="No File", tracker_incidents_file="No File",
                site_list=["No site list"], geography="PT", date="27-03-1996"):

    sg.theme('DarkAmber')  # Add a touch of color
    if incidents_file == "No File" or tracker_incidents_file == "No File":
        sg.popup('No files or site list available, please select them', no_titlebar=True)
        incidents_file, tracker_incidents_file, site_list, geography, date = inputs.choose_incidents_files()
        if incidents_file == "No File" or tracker_incidents_file == "No File":
            return None
    else:
        print("Incidents file: " + incidents_file + "\nTracker Incidents file: " + tracker_incidents_file)

    dir = os.path.dirname(incidents_file)
    reportfiletemplate = dir + '/Info&Templates/Reporting_'+ geography +'_Sites_' + 'Template.xlsx'
    general_info_path = dir +  '/Info&Templates/General Info ' + geography + '.xlsx'


    #Reset Report Template to create new report
    reportfile = data_treatment.reset_final_report(reportfiletemplate, date, geography)

    #Read Active and Closed Events
    df_list_active, df_list_closed = data_acquisition.read_approved_incidents(incidents_file, site_list, roundto=1)
    df_tracker_active, df_tracker_closed = data_acquisition.read_approved_tracker_inc(tracker_incidents_file, roundto=1)

    #Read sunrise and sunset hours
    df_info_sunlight = pd.read_excel(incidents_file, sheet_name='Info', engine="openpyxl")
    df_info_sunlight['Time of operation start'] = df_info_sunlight['Time of operation start'].dt.round(freq='s')
    df_info_sunlight['Time of operation end'] = df_info_sunlight['Time of operation end'].dt.round(freq='s')

    #Describe Incidents
    df_list_active = data_treatment.describe_incidents(df_list_active, df_info_sunlight, active_events=True, tracker=False)
    df_list_closed = data_treatment.describe_incidents(df_list_closed, df_info_sunlight, active_events=False, tracker=False)
    df_tracker_active = data_treatment.describe_incidents(df_tracker_active, df_info_sunlight, active_events=True, tracker=True)
    df_tracker_closed = data_treatment.describe_incidents(df_tracker_closed, df_info_sunlight, active_events=False, tracker=True)
    print(df_tracker_closed.columns)

    #Add Events to Report File
    add_events_to_final_report(reportfile, df_list_active, df_list_closed, df_tracker_active, df_tracker_closed)

    #-------------------------------Analysis on Components Failures---------------------------------
    #Read and update the timestamps on the anaysis dataframes
    df_incidents_analysis, df_tracker_analysis = data_acquisition.read_analysis_df_and_correct_date(reportfiletemplate, date,
                                                                                      roundto=1)
    #Analysis of components failures
    df_incidents_analysis_final = data_analysis.analysis_component_incidents(
        df_incidents_analysis,site_list, df_list_closed,df_list_active, df_info_sunlight)

    # Analysis of tracker failures
    df_tracker_analysis_final = data_analysis.analysis_tracker_incidents(
        df_tracker_analysis, df_tracker_closed,df_tracker_active, df_info_sunlight)

    #Add Analysis to excel file
    add_analysis_to_reportfile(reportfile, df_incidents_analysis_final, df_tracker_analysis_final, df_info_sunlight)


    return reportfile