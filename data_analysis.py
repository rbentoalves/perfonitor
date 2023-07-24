import pandas as pd
from datetime import datetime
import perfonitor.data_treatment as data_treatment


# Analysis of incidents ---------------------------------------------------------------------------------------------
def analysis_closed_incidents(site, index_site, df_incidents_analysis, df_closed_events, df_info_sunlight):

    max_percentage = "{:.2%}".format(1)
    min_percentage = "{:.2%}".format(0)
    capacity_site = df_info_sunlight.at[index_site, 'Capacity']

    starttime_day = df_info_sunlight.at[index_site, 'Time of operation start']
    endtime_day = df_info_sunlight.at[index_site, 'Time of operation end']

    if not df_closed_events.empty:
        try:
            mintimestamp = df_closed_events['Rounded Event Start Time'].min()
            maxtimestamp = df_closed_events['Rounded Event End Time'].max()

            if mintimestamp < starttime_day:
                mintimestamp = starttime_day
            if maxtimestamp > endtime_day:
                maxtimestamp = endtime_day

        except KeyError:
            print('KeyError: There were no new closed events in this day on ' + site)

        try:
            index_mint = df_incidents_analysis[
                df_incidents_analysis['Time'] == mintimestamp].index.values  # gets starting time row index
            int_index_mint = index_mint[0]  # turns index from numpy.ndarray to integer

            index_maxt = df_incidents_analysis[
                df_incidents_analysis['Time'] == maxtimestamp].index.values  # gets ending time row index
            int_index_maxt = index_maxt[0]  # turns index

            for index in range(int_index_mint, int_index_maxt):
                sum_capacity = 0
                for index_not, row in df_closed_events.iterrows():
                    if "roducing" in row['Component Status'] and not row['Curtailment Event'] == "x":
                        if row['Rounded Event End Time'] >= df_incidents_analysis.loc[index, 'Time'] >= row['Rounded Event Start Time']:

                            sum_capacity += row['Capacity Related Component']
                    else:
                        continue

                    percentage = "{:.2%}".format(sum_capacity / capacity_site)
                    if float(percentage[:-1]) > float(max_percentage[:-1]):
                        percentage_final = min_percentage    # test1 - max_percentage
                    else:
                        percentage_final = "{:.2%}".format(1-(sum_capacity / capacity_site))
                    df_incidents_analysis.loc[index, site] = percentage_final   # test1 max_percentage -

        except KeyError:
            print('KeyError: There were no approved closed events in this day on ' + site)
        except NameError:
            print('NameError: There were no approved closed events in this day on ' + site)
        except IndexError:
            print('IndexError: There were no approved closed events in this day on ' + site)

    return df_incidents_analysis


def analysis_active_incidents(site, index_site, df_incidents_analysis, df_active_events, df_info_sunlight):

    max_percentage = "{:.2%}".format(1)
    min_percentage = "{:.2%}".format(0)
    starttime_day = df_info_sunlight.at[index_site, 'Time of operation start']
    endtime_day = df_info_sunlight.at[index_site, 'Time of operation end']
    capacity_site = df_info_sunlight.at[index_site, 'Capacity']

    try:
        for index_event, row in df_active_events.iterrows():

            '''Reads each event'''
            if "roducing" in row['Component Status'] and not row['Curtailment Event'] == "x":

                starttime_event = df_active_events.loc[index_event, 'Rounded Event Start Time']
                capacity_affected = df_active_events.loc[index_event, 'Capacity Related Component']
                rel_comp = df_active_events.loc[index_event, 'Related Component']

                if starttime_day > starttime_event:
                    starttime_event = starttime_day

                index_mint = df_incidents_analysis[
                    df_incidents_analysis['Time'] == starttime_event].index.values  # gets starting time row index
                int_index_mint = index_mint[0]  # turns index from numpy.ndarray to integer

                index_maxt = df_incidents_analysis[
                    df_incidents_analysis['Time'] == endtime_day].index.values  # gets ending time row index
                int_index_maxt = index_maxt[0]

                for index_timestamp in range(int_index_mint, int_index_maxt):

                    '''For each event read, it's effect is added in the corresponding period'''

                    percentage = "{:.2%}".format(capacity_affected / capacity_site)
                    if pd.isnull(df_incidents_analysis.loc[index_timestamp, site]):
                        percentage_final = "{:.2%}".format(1-(capacity_affected / capacity_site))
                        df_incidents_analysis.loc[index_timestamp, site] = percentage_final   # test1 max_percentage -
                    else:
                        perc_ce = float(df_incidents_analysis.loc[index_timestamp, site].strip('%')) / 100
                        perc_ae = float(percentage.strip('%')) / 100
                        percentage = "{:.2%}".format(perc_ce - perc_ae)

                        if float(percentage[:-1]) < float(min_percentage[:-1]):
                            percentage_final = min_percentage     # test1 - max_percentage
                        else:
                            percentage_final = percentage
                        df_incidents_analysis.loc[index_timestamp, site] = percentage_final   # test1 max_percentage -
    except KeyError:
        print('There were no approved active events in this day on ' + site)
    except NameError:
        print('There were no approved active events in this day on ' + site)
    except IndexError:
        print('There were no approved active events in this day on ' + site)

    return df_incidents_analysis


def analysis_closed_tracker_incidents(df_tracker_analysis, df_tracker_closed, df_info_sunlight):

    max_percentage = "{:.2%}".format(1)
    min_percentage = "{:.2%}".format(0)

    for index, row in df_tracker_closed.iterrows():
        #Site related info
        site = df_tracker_closed.loc[index, 'Site Name']
        index_site_array = df_info_sunlight[df_info_sunlight['Site'] == site].index.values
        index_site = int(index_site_array[0])
        capacity_site = df_info_sunlight.at[index_site, 'Capacity']
        starttime_site = df_info_sunlight.loc[index_site, 'Time of operation start']
        endtime_site = df_info_sunlight.loc[index_site, 'Time of operation end']

        # Event related info
        starttime_event = df_tracker_closed.loc[index, 'Rounded Event Start Time']
        endtime_event = df_tracker_closed.loc[index, 'Rounded Event End Time']
        capacity_affected = df_tracker_closed.loc[index, 'Capacity Related Component']

        #print(type(starttime_event))
        #print(type(starttime_site))

        if starttime_event < starttime_site:
            print('Start time site: ' + str(starttime_site) + ' is later than Start time event' + str(starttime_event))
            starttime_event = starttime_site
        else:
            print(
                'Start time site: ' + str(starttime_site) + ' is earlier than Start time event' + str(starttime_event))

        if endtime_event > endtime_site:
            print('End time event: ' + str(endtime_event) + ' is later than End time site: ' + str(endtime_site))
            endtime_event = endtime_site
        else:
            print('End time event: ' + str(endtime_event) + ' is earlier than End time site: ' + str(endtime_site))

        index_mint = df_tracker_analysis[
            df_tracker_analysis['Time'] == starttime_event].index.values  # gets starting time row index
        int_index_mint = index_mint[0]  # turns index from numpy.ndarray to integer

        index_maxt = df_tracker_analysis[
            df_tracker_analysis['Time'] == endtime_event].index.values  # gets ending time row index
        int_index_maxt = index_maxt[0]

        for index in range(int_index_mint, int_index_maxt):
            percentage = "{:.2%}".format(capacity_affected / capacity_site)
            if pd.isnull(df_tracker_analysis.loc[index, site]):
                percentage_final = "{:.2%}".format(1-(capacity_affected / capacity_site))
                df_tracker_analysis.loc[index, site] = percentage_final   # test1 max_percentage -
            else:
                perc_ce = float(df_tracker_analysis.loc[index, site].strip('%')) / 100
                perc_ae = float(percentage.strip('%')) / 100
                percentage = "{:.2%}".format(perc_ce - perc_ae)
                if float(percentage[:-1]) < float(min_percentage[:-1]):
                    percentage_final = min_percentage     # test1 - max_percentage
                else:
                    percentage_final = percentage
                df_tracker_analysis.loc[index, site] = percentage_final   # test1 max_percentage -

    return df_tracker_analysis


def analysis_active_tracker_incidents(df_tracker_analysis, df_tracker_active, df_info_sunlight):

    max_percentage = "{:.2%}".format(1)
    min_percentage = "{:.2%}".format(0)

    for index, row in df_tracker_active.iterrows():
        # Site related info
        site = df_tracker_active.loc[index, 'Site Name']

        index_site_array = df_info_sunlight[df_info_sunlight['Site'] == site].index.values
        index_site = int(index_site_array[0])

        capacity_site = df_info_sunlight.at[index_site, 'Capacity']
        starttime_site = df_info_sunlight.loc[index_site, 'Time of operation start']
        endtime_site = df_info_sunlight.loc[index_site, 'Time of operation end']

        # Event related info
        starttime_event = df_tracker_active.loc[index, 'Rounded Event Start Time']
        capacity_affected = df_tracker_active.loc[index, 'Capacity Related Component']

        if starttime_event < starttime_site:
            starttime_event = starttime_site
        try:
            index_mint = df_tracker_analysis[
                df_tracker_analysis['Time'] == starttime_event].index.values  # gets starting time row index
            int_index_mint = index_mint[0]  # turns index from numpy.ndarray to integer
        except IndexError:
            print("This event was not included because it went out of bounds in terms of start time")
            print(row)
        try:
            index_maxt = df_tracker_analysis[
                df_tracker_analysis['Time'] == endtime_site].index.values  # gets ending time row index
            int_index_maxt = index_maxt[0]
        except IndexError:
            print("This event was not included because it went out of bounds in terms of end time")
            print(row)

        for index in range(int_index_mint, int_index_maxt):
            percentage = "{:.2%}".format(capacity_affected / capacity_site)
            if pd.isnull(df_tracker_analysis.loc[index, site]):
                percentage_final = "{:.2%}".format(1-(capacity_affected / capacity_site))
                df_tracker_analysis.loc[index, site] = percentage_final   # test1 max_percentage -
            else:
                perc_ce = float(df_tracker_analysis.loc[index, site].strip('%')) / 100
                perc_ae = float(percentage.strip('%')) / 100
                percentage = "{:.2%}".format(perc_ce - perc_ae)
                if float(percentage[:-1]) < float(min_percentage[:-1]):
                    percentage_final = min_percentage     # test1 - max_percentage
                else:
                    percentage_final = percentage
                df_tracker_analysis.loc[index, site] = percentage_final   # test1 max_percentage -

    return df_tracker_analysis


def get_significance_score(df, active: bool = False):
    df_final = df
    if active is False:
        diff = (df['Event End Time'] - df['Event Start Time'])
        diff_days = [difference.days * (60 * 60 * 24) for difference in diff]
        diff_seconds = [difference.seconds for difference in diff]
        diff_total = [(diff_days[i] + diff_seconds[i]) / (60 * 60 * 24) for i in range(len(diff_seconds))]

        significance_score = [((df['Capacity Related Component'][i] * diff_total[i])/1000)
                              for i in range(len(diff_total))]

        df_final['Significance Score (MW*d)'] = significance_score

    else:
        today = datetime.today()
        diff = [today - df['Event Start Time'][i] for i in range(len(df['Event Start Time']))]
        diff_days = [difference.days * (60 * 60 * 24) for difference in diff]
        diff_seconds = [difference.seconds for difference in diff]
        diff_total = [(diff_days[i] + diff_seconds[i]) / (60 * 60 * 24) for i in range(len(diff_seconds))]

        significance_score = [((df['Capacity Related Component'][i] * diff_total[i]) / 1000) for i in
                              range(len(diff_total))]

        df_final['Significance Score (MW*d)'] = significance_score

    return df_final

# Analysis with dataframe output

def analysis_component_incidents(df_incidents_analysis, site_list, df_list_closed, df_list_active, df_info_sunlight):
    df_incidents_analysis = data_treatment.fill_events_analysis_dataframe(df_incidents_analysis, df_info_sunlight)

    for site in site_list:
        index_site_array = df_info_sunlight[df_info_sunlight['Site'] == site].index.values
        index_site = int(index_site_array[0])

        df_closed_events = df_list_closed[site]
        df_active_events = df_list_active[site]

        # Add efect of closed events
        df_incidents_analysis = analysis_closed_incidents(site, index_site, df_incidents_analysis,
                                                                        df_closed_events,
                                                                        df_info_sunlight)

        # Add effect of active events
        df_incidents_analysis = analysis_active_incidents(site, index_site, df_incidents_analysis,
                                                                        df_active_events,
                                                                        df_info_sunlight)

    # print(df_incidents_analysis)
    return df_incidents_analysis


def analysis_tracker_incidents(df_tracker_analysis, df_tracker_closed, df_tracker_active, df_info_sunlight):
    df_tracker_analysis = data_treatment.fill_events_analysis_dataframe(df_tracker_analysis, df_info_sunlight)

    # Add effect of closed events
    df_tracker_analysis = analysis_closed_tracker_incidents(df_tracker_analysis, df_tracker_closed,
                                                                          df_info_sunlight)

    # Add effect of active events
    df_tracker_analysis = analysis_active_tracker_incidents(df_tracker_analysis, df_tracker_active,
                                                                          df_info_sunlight)

    # print(df_tracker_analysis)

    return df_tracker_analysis


# <editor-fold desc="ET Functions">



# </editor-fold>

