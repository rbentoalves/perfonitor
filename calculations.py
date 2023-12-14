import re
import pandas as pd
import perfonitor.data_treatment as data_treatment
import perfonitor.data_acquisition as data_acquisition
import perfonitor.inputs as inputs
import perfonitor.visuals as visuals
import calendar
from datetime import datetime
import timeit
import math
import time
import datetime as dt


# <editor-fold desc="Expected Energy Calculation">

def calculate_expected_energy(site, start_timestamp, end_timestamp, budget_export, budget_irradiance,
                              actual_irradiance_site):
    # Calculate Expected Energy in period
    if start_timestamp.month == end_timestamp.month and start_timestamp.year == end_timestamp.year:
        expected_energy_info = {}
        seconds_in_period = (end_timestamp - start_timestamp).total_seconds()
        seconds_in_month = calendar.monthrange(start_timestamp.year, start_timestamp.month)[1] * 24 * 3600
        percentage_of_month = seconds_in_period / seconds_in_month

        actual_irradiance_slice = actual_irradiance_site.loc[
            (actual_irradiance_site.index <= end_timestamp) & (actual_irradiance_site.index >= start_timestamp)]

        budget_energy_month = budget_export.loc[site, str(start_timestamp.replace(day=1).date())]
        budget_irradiance_month = budget_irradiance.loc[site, str(start_timestamp.replace(day=1).date())]
        budget_energy_slice = percentage_of_month * budget_energy_month
        budget_irradiance_slice = percentage_of_month * budget_irradiance_month

        expected_energy_slice = (budget_energy_slice * (actual_irradiance_slice.sum() / 4)) / budget_irradiance_slice

        expected_energy = (budget_export.loc[site, str(start_timestamp.replace(day=1).date())] * (
            actual_irradiance_site.sum() / 4)) / budget_irradiance.loc[
                              site, str(start_timestamp.replace(day=1).date())]

        expected_energy_info[str(start_timestamp.replace(day=1).date())] = {
            "Budget Irradiance Month": budget_irradiance_month,
            "Budget Export Month": budget_energy_month,
            "Percentage of month": percentage_of_month,
            "Budget Irradiance Period": budget_irradiance_slice,
            "Budget Export Period": budget_energy_slice,
            "Actual Irradiance Period": actual_irradiance_slice.sum() / 4000,
            "Expected Energy Period": expected_energy}

    else:
        # If not restricted to a month, the script will separate by months and calculate the expected energy in each
        # month's slice, in the end it will sum it all to give an expected energy for the period in analysis
        date_range = pd.date_range(start_timestamp.replace(day=1, minute=0), end_timestamp.replace(day=1, minute=0),
                                   freq=pd.offsets.MonthBegin(1))
        expected_energy_in_period = {}
        expected_energy_info = {}
        print(type(budget_export.columns[0]))
        for date in date_range:
            budget_energy_month = budget_export.loc[site, datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')]
            budget_irradiance_month = budget_irradiance.loc[site, datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')]
            month = date.month

            if month == start_timestamp.month:
                seconds_in_period_month = (pd.Timestamp(start_timestamp.year, start_timestamp.month,
                                                        calendar.monthrange(start_timestamp.year,
                                                                            start_timestamp.month)[1], 23, 59,
                                                        59) - start_timestamp).total_seconds()
                percentage_of_month = seconds_in_period_month / (
                    calendar.monthrange(start_timestamp.year, month)[1] * 24 * 3600)
                actual_irradiance_slice = actual_irradiance_site.loc[(actual_irradiance_site.index <= pd.Timestamp(
                    start_timestamp.year, start_timestamp.month,
                    calendar.monthrange(start_timestamp.year, start_timestamp.month)[1], 23, 59, 59)) & (
                                                                         actual_irradiance_site.index >= start_timestamp)]

            elif month == end_timestamp.month:
                seconds_in_period_month = (
                    end_timestamp - pd.Timestamp(end_timestamp.year, end_timestamp.month, 1)).total_seconds()
                percentage_of_month = seconds_in_period_month / (
                    calendar.monthrange(end_timestamp.year, month)[1] * 24 * 3600)
                actual_irradiance_slice = actual_irradiance_site.loc[(actual_irradiance_site.index <= end_timestamp) & (
                    actual_irradiance_site.index >= pd.Timestamp(start_timestamp.year, end_timestamp.month, 1))]

            else:
                percentage_of_month = 1

                actual_irradiance_slice = actual_irradiance_site.loc[(actual_irradiance_site.index <= pd.Timestamp(
                    date.year, month,
                    calendar.monthrange(date.year, month)[1], 23, 59, 59)) & (actual_irradiance_site.index
                                                                              >= pd.Timestamp(date.year, month, 1, 0, 0,
                                                                                              0))]

            budget_energy_slice = percentage_of_month * budget_energy_month
            budget_irradiance_slice = percentage_of_month * budget_irradiance_month

            if not percentage_of_month == 0:
                expected_energy_slice = (budget_energy_slice * (
                    actual_irradiance_slice.sum() / 4)) / budget_irradiance_slice
            else:
                expected_energy_slice = 0

            expected_energy_in_period[str(date.date())] = expected_energy_slice

            expected_energy_info[str(date.date())] = {"Budget Irradiance Month": budget_irradiance_month,
                                                      "Budget Export Month": budget_energy_month,
                                                      "Percentage of month": percentage_of_month,
                                                      "Budget Irradiance Period": budget_irradiance_slice,
                                                      "Budget Export Period": budget_energy_slice,
                                                      "Actual Irradiance Period": actual_irradiance_slice.sum() / 4000,
                                                      "Expected Energy Period": expected_energy_slice}

        expected_energy = sum(expected_energy_in_period.values())

    return expected_energy, expected_energy_info


# </editor-fold>

# <editor-fold desc="ET Functions">

# <editor-fold desc="Active Hours and Energy Lost">

def activehours_energylost_incidents(df, df_all_irradiance, df_all_export, budget_pr, corrected_incidents_dict,
                                     active_events: bool = False, recalculate: bool = False,
                                     granularity: float = 0.25):

    if active_events:
        for index, row in df.iterrows():
            site = row['Site Name']
            incident_id = row['ID']
            component = row['Related Component']
            capacity = row['Capacity Related Component']
            real_event_start_time = row['Event Start Time']
            event_start_time = row['Rounded Event Start Time']
            budget_pr_site = budget_pr.loc[site, :]

            """if type(event_start_time) == str:
                row['Rounded Event Start Time'] = event_start_time = datetime.strptime(str(event_start_time),
                                                                                       '%Y-%m-%d %H:%M:%S')

            if type(real_event_start_time) == str:
                row['Event Start Time'] = real_event_start_time = datetime.strptime(str(event_start_time),
                                                                                    '%Y-%m-%d %H:%M:%S')"""

            # event_start_time = row['Event Start Time']
            if incident_id not in corrected_incidents_dict.keys():
                df_irradiance_site = df_all_irradiance.loc[:,
                                     df_all_irradiance.columns.str.contains(site + '|Timestamp')]

                df_irradiance_event = df_irradiance_site.loc[df_irradiance_site['Timestamp'] >= event_start_time]

                # Get percentages of first timestamp to account for rounding
                index_event_start_time = \
                    df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_start_time].index.values[0]

                percentage_of_timestamp_start = data_treatment.get_percentage_of_timestamp(real_event_start_time,
                                                                                           event_start_time)

                actual_column, curated_column, data_gaps_proportion, poa_avg_column = \
                    data_treatment.get_actual_irradiance_column(df_irradiance_event)
                # print(actual_column)

                if actual_column is None:
                    print(component, ' on ', event_start_time, ': No irradiance available')
                    continue

                data_gaps_percentage = "{:.2%}".format(data_gaps_proportion)
                print(incident_id, ' - Data Gaps percentage: ', data_gaps_percentage)

                # Correct irradiance in first timestamp to account for rounding
                df_irradiance_event.at[index_event_start_time, actual_column] = \
                    percentage_of_timestamp_start * df_irradiance_event.loc[index_event_start_time, actual_column]

                df_irradiance_event_activeperiods = df_irradiance_event.loc[df_irradiance_event[actual_column] > 20]

                duration = df_irradiance_event.shape[0] * granularity
                active_hours = df_irradiance_event_activeperiods.shape[0] * granularity
                if site == component:
                    df_export_site = df_all_export.loc[:,
                                     df_all_export.columns.str.contains(site + '|Timestamp')]
                    export_column = df_all_export.columns[df_all_export.columns.str.contains(site)].values[0]

                    df_export_event = df_export_site.loc[df_export_site['Timestamp'] >= event_start_time]

                    df_export_event.at[index_event_start_time, export_column] = percentage_of_timestamp_start * \
                                                                                df_export_event.loc[
                                                                                    index_event_start_time, export_column]

                    energy_produced = df_export_event[export_column].sum()

                    energy_lost = (sum(
                        [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                         index_el, row_el in
                         df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

                    energy_lost = (energy_lost - energy_produced)

                    if energy_lost < 0:
                        energy_lost = 0

                else:
                    energy_lost = (sum(
                        [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                         index_el, row_el in
                         df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

            else:
                data_gaps_percentage = "{:.2%}".format(corrected_incidents_dict[incident_id]['Data Gaps Proportion'])

                actual_column = corrected_incidents_dict[incident_id]['Irradiance Column']

                df_irradiance_event_raw = corrected_incidents_dict[incident_id]['Irradiance Raw']
                df_irradiance_event = corrected_incidents_dict[incident_id]['Corrected Irradiance Incident']
                df_cleaned_irradiance_event = corrected_incidents_dict[incident_id]['Cleaned Corrected Irradiance Incident']

                print('Using Corrected Incident for: ', component, " on ", site, "with ", data_gaps_percentage,
                      " of data gaps")

                df_irradiance_event_activeperiods = df_irradiance_event.loc[df_irradiance_event[actual_column] > 20]

                df_cleaned_irradiance_event_activeperiods = df_cleaned_irradiance_event.loc[
                    df_cleaned_irradiance_event[actual_column] > 20]

                #Calculate duration, active hours and energy lost
                duration = df_irradiance_event_raw.shape[0] * granularity

                active_hours = df_irradiance_event_activeperiods.shape[0] * granularity

                energy_lost = (sum(
                    [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                     index_el, row_el in
                     df_cleaned_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000
                """energy_lost = (sum([row_el[actual_column] * float(budget_pr_site[row_el[
                'Timestamp'].month].values) for index_el, row_el in 
                df_cleaned_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000 """

            if active_hours < 0:
                print("Active hours below 0 (how??): " + str(incident_id))
                active_hours = duration

            df.loc[index, 'Duration (h)'] = duration
            df.loc[index, 'Active Hours (h)'] = active_hours
            df.loc[index, 'Energy Lost (MWh)'] = energy_lost / 1000

    else:
        if recalculate:
            df = data_treatment.rounddatesclosed_15m("All", df)
            for index, row in df.iterrows():
                site = row['Site Name']
                incident_id = row['ID']
                component = row['Related Component']
                capacity = row['Capacity Related Component']
                event_start_time = row['Rounded Event Start Time']
                event_end_time = row['Rounded Event End Time']
                real_event_start_time = row['Event Start Time']
                real_event_end_time = row['Event End Time']
                budget_pr_site = budget_pr.loc[site, :]

                if incident_id not in corrected_incidents_dict.keys():
                    df_irradiance_site = df_all_irradiance.loc[:,
                                         df_all_irradiance.columns.str.contains(site + '|Timestamp')]
                    df_irradiance_event = df_irradiance_site.loc[
                        (df_irradiance_site['Timestamp'] >= event_start_time) & (
                            df_irradiance_site['Timestamp'] <= event_end_time)]
                    actual_column, curated_column, data_gaps_proportion, poa_avg_column = data_treatment.get_actual_irradiance_column(
                        df_irradiance_event)

                    if actual_column == None:
                        print(component, ' on ', event_start_time, ': No irradiance available')
                        continue

                    elif 'curated' in actual_column:
                        print(component, ' on ', event_start_time, ': Using curated irradiance')
                    else:
                        print(component, ' on ', event_start_time,
                              ': Using poa average due to curated irradiance having over 25% of data gaps')

                    data_gaps_percentage = "{:.2%}".format(data_gaps_proportion)
                    print(incident_id, ' - Data Gaps percentage: ', data_gaps_percentage)

                    df_irradiance_event_activeperiods = df_irradiance_event.loc[df_irradiance_event[actual_column] > 20]

                    """duration = df_irradiance_event.shape[0] * granularity
                    #duration =
                    active_hours = df_irradiance_event_activeperiods.shape[0] * granularity
                    """
                    duration = ((real_event_end_time - real_event_start_time).days * 24) + (
                        (real_event_end_time - real_event_start_time).seconds / 3600)

                    active_timestamps = df_irradiance_event_activeperiods.shape[0]

                    if active_timestamps == 0:
                        active_hours = 0
                    else:
                        active_hours = duration - (df_irradiance_event.shape[0] - active_timestamps) * granularity



                    # print(real_event_end_time, " - ", real_event_start_time, " = ", duration)
                    # print(real_event_end_time, " - ", real_event_start_time, " = ", active_hours , " - Active Hours")

                    energy_lost = (sum(
                        [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                         index_el, row_el in
                         df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

                else:
                    data_gaps_percentage = "{:.2%}".format(
                        corrected_incidents_dict[incident_id]['Data Gaps Proportion'])
                    actual_column = corrected_incidents_dict[incident_id]['Irradiance Column']

                    df_irradiance_event_raw = corrected_incidents_dict[incident_id]['Irradiance Raw']
                    df_irradiance_event = corrected_incidents_dict[incident_id]['Corrected Irradiance Incident']
                    df_cleaned_irradiance_event = corrected_incidents_dict[incident_id][
                        'Cleaned Corrected Irradiance Incident']

                    """df_irradiance_event = 
                    df_cleaned_irradiance_event ="""

                    print('Using Corrected Incident for: ', component, " on ", site, " with ", data_gaps_percentage,
                          " of data gaps")

                    df_irradiance_event_activeperiods = df_irradiance_event.loc[
                        df_irradiance_event[actual_column] > 20]
                    df_cleaned_irradiance_event_activeperiods = df_cleaned_irradiance_event.loc[
                        df_cleaned_irradiance_event[actual_column] > 20]

                    duration = ((real_event_end_time - real_event_start_time).days * 24) + (
                        (real_event_end_time - real_event_start_time).seconds / 3600)

                    active_timestamps = df_irradiance_event_activeperiods.shape[0]

                    if active_timestamps == 0:
                        active_hours = 0
                    else:
                        active_hours = duration - (df_irradiance_event.shape[0] - active_timestamps) * granularity

                    # print(real_event_end_time, " - ", real_event_start_time, " = ", duration)
                    # print(real_event_end_time, " - ", real_event_start_time, " = ", active_hours, " - Active Hours")

                    energy_lost = (sum(
                        [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                         index_el, row_el in
                         df_cleaned_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

                if active_hours < 0:
                    print("Active hours below 0: " + str(incident_id))
                    active_hours = duration

                df.loc[index, 'Duration (h)'] = duration
                df.loc[index, 'Active Hours (h)'] = active_hours
                df.loc[index, 'Energy Lost (MWh)'] = energy_lost / 1000
        else:
            n_inc_1 = df.shape[0]
            df_to_update = df.loc[df['Energy Lost (MWh)'].isnull()]
            n_inc_2 = df_to_update.shape[0]
            df_to_update = data_treatment.rounddatesclosed_15m("All", df_to_update)
            print('No recalculation, calculating KPIs for ', n_inc_2, ' from a total of ', n_inc_1, ' incidents.')

            for index, row in df_to_update.iterrows():
                site = row['Site Name']
                incident_id = row['ID']
                component = row['Related Component']
                capacity = row['Capacity Related Component']
                event_start_time = row['Rounded Event Start Time']
                event_end_time = row['Rounded Event End Time']
                real_event_start_time = row['Event Start Time']
                real_event_end_time = row['Event End Time']
                budget_pr_site = budget_pr.loc[site, :]

                if incident_id not in corrected_incidents_dict.keys():
                    print("\n" + incident_id)
                    df_irradiance_site = df_all_irradiance.loc[:,
                                         df_all_irradiance.columns.str.contains(site + '|Timestamp')]

                    df_irradiance_event = df_irradiance_site.loc[
                        (df_irradiance_site['Timestamp'] >= event_start_time) & (
                            df_irradiance_site['Timestamp'] <= event_end_time)]

                    # Get percentages of first timestamp to account for rounding
                    index_event_start_time = \
                        df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_start_time].index.values[0]

                    #print(event_end_time)
                    #print(real_event_end_time)
                    #print(df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_end_time].index.values)
                    #print(df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_end_time])

                    index_event_end_time = \
                        df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_end_time].index.values[0]

                    percentage_of_timestamp_start = data_treatment.get_percentage_of_timestamp(real_event_start_time,
                                                                                               event_start_time)
                    percentage_of_timestamp_end = data_treatment.get_percentage_of_timestamp(real_event_end_time,
                                                                                             event_end_time)

                    # GEt actual column to work with
                    actual_column, curated_column, data_gaps_proportion, poa_avg_column = \
                        data_treatment.get_actual_irradiance_column(df_irradiance_event)

                    if actual_column is None:
                        print(component, ' on ', event_start_time, ': No irradiance available')
                        continue

                    elif 'curated' in actual_column:
                        print(component, ' on ', event_start_time, ': Using curated irradiance')
                    else:
                        print(component, ' on ', event_start_time,
                              ': Using poa average due to curated irradiance having over 25% of data gaps')

                    # Communicate data gaps percentage
                    data_gaps_percentage = "{:.2%}".format(data_gaps_proportion)
                    print('Data Gaps percentage: ', data_gaps_percentage)

                    # Correct irradiance in first timestamp to account for rounding
                    df_irradiance_event.at[index_event_start_time, actual_column] = percentage_of_timestamp_start * \
                                                                                    df_irradiance_event.loc[
                                                                                        index_event_start_time, actual_column]

                    df_irradiance_event.at[index_event_end_time, actual_column] = percentage_of_timestamp_end * \
                                                                                  df_irradiance_event.loc[
                                                                                      index_event_end_time, actual_column]

                    # Get irradiance periods over 20W/m2
                    df_irradiance_event_activeperiods = df_irradiance_event.loc[df_irradiance_event[actual_column] > 20]

                    duration = ((real_event_end_time - real_event_start_time).days * 24) + \
                               ((real_event_end_time - real_event_start_time).seconds / 3600)

                    active_timestamps = df_irradiance_event_activeperiods.shape[0]

                    if active_timestamps == 0:
                        active_hours = 0
                    else:
                        active_hours = duration - (df_irradiance_event.shape[0] - active_timestamps) * granularity


                    if site == component:
                        df_export_site = df_all_export.loc[:,
                                         df_all_export.columns.str.contains(site + '|Timestamp')]
                        export_column = df_all_export.columns[df_all_export.columns.str.contains(site)].values[0]

                        df_export_event = df_export_site.loc[
                            (df_export_site['Timestamp'] >= event_start_time) & (
                                df_export_site['Timestamp'] <= event_end_time)]

                        df_export_event.at[index_event_start_time, export_column] = \
                            percentage_of_timestamp_start * df_export_event.loc[index_event_start_time, export_column]

                        df_export_event.at[index_event_end_time, export_column] = \
                            percentage_of_timestamp_end * df_export_event.loc[index_event_end_time, export_column]

                        energy_produced = df_export_event[export_column].sum()

                        energy_lost = (sum(
                            [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                             index_el, row_el in
                             df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

                        #print("Site: ", site, "\nEnergy produced: ", energy_produced, "\nEnergy Expected: ",
                        #      energy_lost)

                        energy_lost = (energy_lost - energy_produced)

                        #print("Real Energy Lost: ", energy_lost)

                        if energy_lost < 0:
                            energy_lost = 0

                    else:
                        energy_lost = (sum(
                            [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                             index_el, row_el in
                             df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

                else:
                    data_gaps_percentage = "{:.2%}".format(
                        corrected_incidents_dict[incident_id]['Data Gaps Proportion'])

                    actual_column = corrected_incidents_dict[incident_id]['Irradiance Column']

                    df_irradiance_event_raw = corrected_incidents_dict[incident_id][
                        'Irradiance Raw']
                    df_irradiance_event = corrected_incidents_dict[incident_id][
                        'Corrected Irradiance Incident']
                    df_cleaned_irradiance_event = corrected_incidents_dict[incident_id][
                        'Cleaned Corrected Irradiance Incident']

                    print('Using Corrected Incident for: ', component, " on ", site, "with ", data_gaps_percentage,
                          " of data gaps")

                    df_irradiance_event_activeperiods = df_irradiance_event.loc[
                        df_irradiance_event[actual_column] > 20]
                    df_cleaned_irradiance_event_activeperiods = df_cleaned_irradiance_event.loc[
                        df_cleaned_irradiance_event[actual_column] > 20]

                    duration = ((real_event_end_time - real_event_start_time).days * 24) \
                               + ((real_event_end_time - real_event_start_time).seconds / 3600)

                    active_timestamps = df_irradiance_event_activeperiods.shape[0]

                    if active_timestamps == 0:
                        active_hours = 0
                    else:
                        active_hours = duration - (df_irradiance_event.shape[0] - active_timestamps) * granularity

                    energy_lost = (sum(
                        [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                         index_el, row_el in
                         df_cleaned_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

                if active_hours < 0:
                    #print("Active hours below 0: " + str(incident_id))
                    active_hours = duration

                df.loc[index, 'Duration (h)'] = duration
                df.loc[index, 'Active Hours (h)'] = active_hours
                df.loc[index, 'Energy Lost (MWh)'] = energy_lost / 1000

    return df

def activehours_energylost_tracker(df, df_all_irradiance, df_all_export, budget_pr, active_events: bool = False,
                                   granularity: float = 0.25):

    if active_events:
        for index, row in df.iterrows():
            site = row['Site Name']
            incident_id = row['ID']
            component = row['Related Component']
            capacity = row['Capacity Related Component']
            real_event_start_time = row['Event Start Time']
            event_start_time = row['Rounded Event Start Time']
            budget_pr_site = budget_pr.loc[site, :]

            """if type(event_start_time) == str:
                row['Rounded Event Start Time'] = event_start_time = datetime.strptime(str(event_start_time),
                                                                                       '%Y-%m-%d %H:%M:%S')

            if type(real_event_start_time) == str:
                row['Event Start Time'] = real_event_start_time = datetime.strptime(str(event_start_time),
                                                                                    '%Y-%m-%d %H:%M:%S')"""

            df_irradiance_site = df_all_irradiance.loc[:,
                                 df_all_irradiance.columns.str.contains(site + '|Timestamp')]

            df_irradiance_event = df_irradiance_site.loc[df_irradiance_site['Timestamp'] >= event_start_time]

            # Get percentages of first timestamp to account for rounding
            index_event_start_time = \
                df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_start_time].index.values[0]

            percentage_of_timestamp_start = data_treatment.get_percentage_of_timestamp(real_event_start_time,
                                                                                       event_start_time)

            actual_column, curated_column, data_gaps_proportion, poa_avg_column = \
                data_treatment.get_actual_irradiance_column(df_irradiance_event)
            # print(actual_column)

            if actual_column is None:
                print(component, ' on ', event_start_time, ': No irradiance available')
                continue

            data_gaps_percentage = "{:.2%}".format(data_gaps_proportion)
            print(incident_id, ' - Data Gaps percentage: ', data_gaps_percentage)

            # Correct irradiance in first timestamp to account for rounding
            df_irradiance_event.at[index_event_start_time, actual_column] = \
                percentage_of_timestamp_start * df_irradiance_event.loc[index_event_start_time, actual_column]

            df_irradiance_event_activeperiods = df_irradiance_event.loc[df_irradiance_event[actual_column] > 20]

            duration = df_irradiance_event.shape[0] * granularity
            active_hours = df_irradiance_event_activeperiods.shape[0] * granularity
            energy_lost = (sum([row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                     index_el, row_el in
                     df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

            if active_hours < 0:
                print("Active hours below 0 (how??): " + str(incident_id))
                active_hours = duration

            df.loc[index, 'Duration (h)'] = duration
            df.loc[index, 'Active Hours (h)'] = active_hours
            df.loc[index, 'Energy Lost (MWh)'] = energy_lost / 1000

    else:
        n_inc_1 = df.shape[0]
        df_to_update = df.loc[df['Energy Lost (MWh)'].isnull()]
        n_inc_2 = df_to_update.shape[0]
        df_to_update = data_treatment.rounddatesclosed_15m("All", df_to_update)
        print('[Trackers] No recalculation, calculating KPIs for ', n_inc_2, ' from a total of ', n_inc_1, ' incidents.')

        for index, row in df_to_update.iterrows():
            site = row['Site Name']
            incident_id = row['ID']
            component = row['Related Component']
            capacity = row['Capacity Related Component']
            event_start_time = row['Rounded Event Start Time']
            event_end_time = row['Rounded Event End Time']
            real_event_start_time = row['Event Start Time']
            real_event_end_time = row['Event End Time']
            budget_pr_site = budget_pr.loc[site, :]

            print("\n" + str(incident_id))
            df_irradiance_site = df_all_irradiance.loc[:,
                                 df_all_irradiance.columns.str.contains(site + '|Timestamp')]

            df_irradiance_event = df_irradiance_site.loc[
                (df_irradiance_site['Timestamp'] >= event_start_time) & (
                    df_irradiance_site['Timestamp'] <= event_end_time)]

            # Get percentages of first timestamp to account for rounding
            index_event_start_time = \
                df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_start_time].index.values[0]

            #print(event_end_time)
            #print(real_event_end_time)
            #print(df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_end_time].index.values)
            #print(df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_end_time])

            index_event_end_time = \
                df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_end_time].index.values[0]

            percentage_of_timestamp_start = data_treatment.get_percentage_of_timestamp(real_event_start_time,
                                                                                       event_start_time)
            percentage_of_timestamp_end = data_treatment.get_percentage_of_timestamp(real_event_end_time,
                                                                                     event_end_time)

            # GEt actual column to work with
            actual_column, curated_column, data_gaps_proportion, poa_avg_column = \
                data_treatment.get_actual_irradiance_column(df_irradiance_event)

            if actual_column is None:
                print(component, ' on ', event_start_time, ': No irradiance available')
                continue

            elif 'curated' in actual_column:
                print(component, ' on ', event_start_time, ': Using curated irradiance')
            else:
                print(component, ' on ', event_start_time,
                      ': Using poa average due to curated irradiance having over 25% of data gaps')

            # Communicate data gaps percentage
            data_gaps_percentage = "{:.2%}".format(data_gaps_proportion)
            print('Data Gaps percentage: ', data_gaps_percentage)

            # Correct irradiance in first timestamp to account for rounding
            df_irradiance_event.at[index_event_start_time, actual_column] = percentage_of_timestamp_start * \
                                                                            df_irradiance_event.loc[
                                                                                index_event_start_time, actual_column]

            df_irradiance_event.at[index_event_end_time, actual_column] = percentage_of_timestamp_end * \
                                                                          df_irradiance_event.loc[
                                                                              index_event_end_time, actual_column]

            # Get irradiance periods over 20W/m2
            df_irradiance_event_activeperiods = df_irradiance_event.loc[df_irradiance_event[actual_column] > 20]

            duration = ((real_event_end_time - real_event_start_time).days * 24) + \
                       ((real_event_end_time - real_event_start_time).seconds / 3600)

            active_timestamps = df_irradiance_event_activeperiods.shape[0]

            if active_timestamps == 0:
                active_hours = 0
            else:
                active_hours = duration - (df_irradiance_event.shape[0] - active_timestamps) * granularity

            energy_lost = (sum([row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                     index_el, row_el in
                     df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

            if active_hours < 0:
                #print("Active hours below 0: " + str(incident_id))
                active_hours = duration

            df.loc[index, 'Duration (h)'] = duration
            df.loc[index, 'Active Hours (h)'] = active_hours
            df.loc[index, 'Energy Lost (MWh)'] = energy_lost / 1000

    return df


def active_hours_and_energy_lost_all_dfs(final_df_to_add, corrected_incidents_dict, df_all_irradiance, df_all_export,
                                         budget_pr,
                                         irradiance_threshold: int = 20, timestamp: int = 15,
                                         recalculate_value: bool = False):
    granularity = timestamp / 60
    tic = timeit.default_timer()
    for key, df in final_df_to_add.items():

        if "Active" in key:
            if "tracker" in key:
                active_events = True
                df = data_treatment.rounddatesactive_15m("All", df)
                df = activehours_energylost_tracker(df, df_all_irradiance, df_all_export, budget_pr, active_events, granularity)
                df = data_treatment.match_df_to_event_tracker(df, None, None, active=active_events, simple_match=True)
                final_df_to_add[key] = df


            else:
                active_events = True
                df = data_treatment.rounddatesactive_15m("All", df)
                df = activehours_energylost_incidents(df, df_all_irradiance, df_all_export, budget_pr,
                                                      corrected_incidents_dict, active_events,
                                                      recalculate_value, granularity)

                df = data_treatment.match_df_to_event_tracker(df, None, None, active=active_events, simple_match=True)
                final_df_to_add[key] = df


        elif "Closed" in key:
            if "tracker" in key:
                active_events = False
                activehours_energylost_tracker(df, df_all_irradiance, df_all_export, budget_pr, active_events, granularity)

                df = data_treatment.match_df_to_event_tracker(df, None, None, simple_match=True)
                final_df_to_add[key] = df

            else:
                active_events = False
                df = activehours_energylost_incidents(df, df_all_irradiance, df_all_export, budget_pr,
                                                      corrected_incidents_dict, active_events,
                                                      recalculate_value, granularity)
                df = data_treatment.match_df_to_event_tracker(df, None, None, simple_match=True)
                final_df_to_add[key] = df

        else:
            continue

    toc = timeit.default_timer()
    print(toc - tic)

    return final_df_to_add


# </editor-fold>


# <editor-fold desc="Availability Calculation">

def calculate_availability_period(site, incidents, tracker_incidents, component_data, budget_pr, df_all_irradiance, df_all_export,
                                  irradiance_threshold, date_start_str, date_end_str, granularity: float = 0.25,
                                  recalculate_value: bool = False):
    print(site)
    period = date_start_str + " to " + date_end_str
    active_events = False

    irradiance_incidents_corrected = {}

    # <editor-fold desc="Get relevant data">

    # Get site info --------------------------------------------------------------------------------------------
    #site_info = component_data.loc[component_data['Site'] == site]
    #budget_pr_site = budget_pr.loc[site, :]
    site_capacity = float(component_data.loc[component_data['Component'] == site]['Nominal Power DC'].values)

    # Get site Incidents --------------------------------------------------------------------------------------------
    site_incidents = incidents.loc[incidents['Site Name'] == site]
    site_tracker_incidents = tracker_incidents.loc[tracker_incidents['Site Name'] == site]
    # Get site irradiance & export --------------------------------------------------------------------------------------------
    df_irradiance_site = df_all_irradiance.loc[:, df_all_irradiance.columns.str.contains(site + '|Timestamp')]
    df_export_site = df_all_export.loc[:, df_all_export.columns.str.contains(site + '|Timestamp')]

    # Get irradiance poa avg column and curated -----------------------------------------------------------------------
    actual_column, curated_column, data_gaps_proportion, poa_avg_column = \
        data_treatment.get_actual_irradiance_column(df_irradiance_site)

    # Get first timestamp under analysis and df from that timestamp onwards -------------------------------------------
    try:
        stime_index = next(i for i, v in enumerate(df_irradiance_site[poa_avg_column]) if v > irradiance_threshold)
        site_start_time = df_irradiance_site['Timestamp'][stime_index]
    except StopIteration:
        site_start_time = date_start_str + " 07:00:00"

    df_irradiance_operation_site = df_irradiance_site.loc[df_irradiance_site['Timestamp'] >= site_start_time]
    df_export_operation_site = df_export_site.loc[df_export_site['Timestamp'] >= site_start_time]

    df_irradiance_operation_site['Day'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').date() for timestamp
                                           in df_irradiance_operation_site['Timestamp']]

    df_export_operation_site['Day'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').date() for timestamp
                                       in df_export_operation_site['Timestamp']]

    # </editor-fold>

    # <editor-fold desc="Get timeframe">
    if not date_start_str == 'None' and not date_end_str == 'None':

        # Get start time analysis
        date_start_avail_analysis = datetime.strptime(date_start_str, '%Y-%m-%d').date()
        timestamp_start_avail_analysis = datetime.strptime(date_start_str + " 00:00:00", '%Y-%m-%d %H:%M:%S')

        # Get end time analysis
        date_end_avail_analysis = datetime.strptime(date_end_str, '%Y-%m-%d').date()
        timestamp_end_avail_analysis = datetime.strptime(str(date_end_avail_analysis) + " 23:45:00", '%Y-%m-%d %H:%M:%S')

        # Get days list under analysis
        days_list = pd.date_range(start=date_start_avail_analysis, end=date_end_avail_analysis).date

    else:
        # Get days list under analysis
        days_list = sorted(list(set(df_irradiance_operation_site['Day'].to_list())))

        # Get start and end time analysis
        timestamp_start_avail_analysis = datetime.strptime(str(df_irradiance_operation_site['Timestamp'].to_list()[0]),
                                                           '%Y-%m-%d %H:%M:%S')
        timestamp_end_avail_analysis = datetime.strptime(str(df_irradiance_operation_site['Timestamp'].to_list()[-1]),
                                                         '%Y-%m-%d %H:%M:%S')

    # </editor-fold>

    # <editor-fold desc="Get irradiance of period under analysis">
    irradiance_analysis = df_irradiance_operation_site.loc[
        (df_irradiance_operation_site['Day'] >= date_start_avail_analysis) & (
            df_irradiance_operation_site['Day'] <= date_end_avail_analysis)]

    export_analysis = df_export_operation_site.loc[
        (df_export_operation_site['Day'] >= date_start_avail_analysis) & (
            df_export_operation_site['Day'] <= date_end_avail_analysis)]

    actual_column, curated_column, data_gaps_proportion, poa_avg_column = data_treatment.get_actual_irradiance_column(
        irradiance_analysis)

    if actual_column:
        df_irradiance_event_activeperiods = irradiance_analysis.loc[
            irradiance_analysis[actual_column] > irradiance_threshold]
    else:
        df_irradiance_event_activeperiods = irradiance_analysis.loc[
            irradiance_analysis[poa_avg_column] > irradiance_threshold]

    active_hours = df_irradiance_event_activeperiods.shape[0] * granularity

    site_active_hours_daily = {
        day: df_irradiance_event_activeperiods.loc[df_irradiance_event_activeperiods['Day'] == day].shape[
                 0] * granularity for day in days_list}

    site_active_hours_df_daily = pd.DataFrame.from_dict(site_active_hours_daily, orient='index',
                                                        columns=[site + ' Active Hours (h)'])

    site_active_hours_period = df_irradiance_event_activeperiods.shape[0] * granularity
    site_active_hours_df_period = pd.DataFrame({'Active Hours (h) ' + period: [site_active_hours_period]}, index=[site])
    # site_active_hours_df_period = pd.DataFrame({'Active Hours (h) ':[df_irradiance_event_activeperiods.shape[0]*granularity]}, index = [site])

    ## print(site_active_hours_df_daily)
    ## print(site_active_hours_df_period)

    # </editor-fold>

    # <editor-fold desc="Get incidents in that period">
    # print(site_incidents[['ID', 'Event Start Time', 'Event End Time']])

    relevant_incidents = site_incidents.loc[~(site_incidents['Event Start Time'] > timestamp_end_avail_analysis) & ~(
        site_incidents['Event End Time'] < timestamp_start_avail_analysis)]

    relevant_tracker_incidents = site_tracker_incidents.loc[
        ~(site_tracker_incidents['Event Start Time'] > timestamp_end_avail_analysis) &
        ~(site_tracker_incidents['Event End Time'] < timestamp_start_avail_analysis)]

    #print(relevant_incidents[['ID', 'Event Start Time', 'Event End Time', 'Energy Lost (MWh)']])

    # </editor-fold>


    # Correct Timestamps of incidents to timeframe of analysis ---------------------------------------------------------
    for index, row in relevant_incidents.iterrows():
        try:
            if pd.isnull(row['Event End Time']):
                print("Found active event: ", str(row["ID"]))
                relevant_incidents.loc[index, 'Event End Time'] = timestamp_end_avail_analysis
                relevant_incidents.loc[index, 'Energy Lost (MWh)'] = None

            elif row['Event End Time'] > timestamp_end_avail_analysis:
                relevant_incidents.loc[index, 'Event End Time'] = timestamp_end_avail_analysis
                relevant_incidents.loc[index, 'Energy Lost (MWh)'] = None

        except TypeError:
            if row['Event End Time'] > timestamp_end_avail_analysis:
                relevant_incidents.loc[index, 'Event End Time'] = timestamp_end_avail_analysis
                relevant_incidents.loc[index, 'Energy Lost (MWh)'] = None

        finally:
            if row['Event Start Time'] < timestamp_start_avail_analysis:
                relevant_incidents.loc[index, 'Event Start Time'] = timestamp_start_avail_analysis
                relevant_incidents.loc[index, 'Energy Lost (MWh)'] = None

    for index, row in relevant_tracker_incidents.iterrows():
        try:
            if pd.isnull(row['Event End Time']):
                print("Found active event: ", str(row["ID"]))
                relevant_tracker_incidents.loc[index, 'Event End Time'] = timestamp_end_avail_analysis
                relevant_tracker_incidents.loc[index, 'Energy Lost (MWh)'] = None

            elif row['Event End Time'] > timestamp_end_avail_analysis:
                relevant_tracker_incidents.loc[index, 'Event End Time'] = timestamp_end_avail_analysis
                relevant_tracker_incidents.loc[index, 'Energy Lost (MWh)'] = None

        except TypeError:
            if row['Event End Time'] > timestamp_end_avail_analysis:
                relevant_tracker_incidents.loc[index, 'Event End Time'] = timestamp_end_avail_analysis
                relevant_tracker_incidents.loc[index, 'Energy Lost (MWh)'] = None

        finally:
            if row['Event Start Time'] < timestamp_start_avail_analysis:
                relevant_tracker_incidents.loc[index, 'Event Start Time'] = timestamp_start_avail_analysis
                relevant_tracker_incidents.loc[index, 'Energy Lost (MWh)'] = None

    ## print(relevant_incidents.loc[(relevant_incidents['Site Name'] == "LSBP - Bighorn") & (relevant_incidents['Related Component'] == "Inverter 65")])

    # Get incidents to keep unaltered
    incidents_unaltered = relevant_incidents.loc[~(relevant_incidents['Event Start Time'] ==
                                                   timestamp_start_avail_analysis) &
                                                 ~(relevant_incidents[
                                                       'Event End Time'] == timestamp_end_avail_analysis)]

    tracker_incidents_unaltered = relevant_tracker_incidents.loc[~(relevant_tracker_incidents['Event Start Time'] ==
                                                   timestamp_start_avail_analysis) &
                                                 ~(relevant_tracker_incidents[
                                                       'Event End Time'] == timestamp_end_avail_analysis)]

    # Get corrected incidents dict (overlappers) and then calculate real active hours and losses with that info --------
    corrected_incidents_dict_period = data_treatment.correct_incidents_irradiance_for_overlapping_parents(
        relevant_incidents,
        irradiance_analysis,
        export_analysis,
        component_data,
        recalculate_value)

    all_corrected_incidents = activehours_energylost_incidents(relevant_incidents, irradiance_analysis,
                                                               export_analysis, budget_pr,
                                                               corrected_incidents_dict_period,
                                                               active_events, recalculate=False,
                                                               granularity=granularity)

    tracker_corrected_incidents = activehours_energylost_tracker(relevant_tracker_incidents, irradiance_analysis,
                                                                 export_analysis, budget_pr, active_events)

    # Get corrected relevant incidents to concat with unaltered ones
    corrected_relevant_incidents = all_corrected_incidents.loc[(all_corrected_incidents['Event Start Time'] ==
                                                                timestamp_start_avail_analysis) |
                                                               (all_corrected_incidents['Event End Time'] ==
                                                                timestamp_end_avail_analysis)]

    corrected_relevant_tracker_inc = tracker_corrected_incidents.loc[(tracker_corrected_incidents['Event Start Time'] ==
                                                                timestamp_start_avail_analysis) |
                                                               (tracker_corrected_incidents['Event End Time'] ==
                                                                timestamp_end_avail_analysis)]

    ## print(corrected_relevant_incidents[['Related Component', 'Event Start Time','Event End Time', "Duration (h)","Active Hours (h)", 'Energy Lost (MWh)' ]])

    # Join corrected incidents and non-corrected incidents

    """if date_start_str == date_end_str:
        corrected_relevant_incidents = all_corrected_incidents
    else:
        """
    corrected_relevant_incidents = pd.concat([incidents_unaltered, corrected_relevant_incidents])
    corrected_relevant_tracker_inc = pd.concat([tracker_incidents_unaltered, corrected_relevant_tracker_inc])

    # corrected_relevant_incidents = final_relevant_incidents
    # Calculate period availability-------------------------------------------------------------------------------------
    weighted_downtime = {}
    raw_weighted_downtime = {}
    tracker_weighted_downtime = {}
    corrected_relevant_incidents['Weighted Downtime'] = ""
    corrected_relevant_incidents['Contribution to downtime %'] = ""
    corrected_relevant_incidents['Raw Contribution to downtime %'] = ""
    corrected_relevant_tracker_inc['Contribution to downtime %'] = ""

    for index, row in corrected_relevant_incidents.iterrows():
        capacity = row['Capacity Related Component']
        active_hours = row['Active Hours (h)']
        failure_mode = row['Failure Mode']
        duration = row["Duration (h)"]

        if not failure_mode == "Curtailment":
            try:
                if capacity == float(0) or math.isnan(active_hours) or type(active_hours) == str:
                    weighted_downtime_incident = raw_weighted_downtime_incident = 0
                # elif math.isnan(active_hours):
                #    weighted_downtime_incident = raw_weighted_downtime_incident = 0
                # elif type(active_hours) == str:
                #    weighted_downtime_incident = raw_weighted_downtime_incident = 0
                else:
                    weighted_downtime_incident = raw_weighted_downtime_incident = (capacity * active_hours) / \
                                                                                  site_capacity
            except TypeError:
                weighted_downtime_incident = raw_weighted_downtime_incident = 0

        else:
            weighted_downtime_incident = 0
            raw_weighted_downtime_incident = active_hours

        weighted_downtime[row['ID']] = weighted_downtime_incident
        raw_weighted_downtime[row['ID']] = raw_weighted_downtime_incident

        corrected_relevant_incidents.loc[index, 'Weighted Downtime'] = weighted_downtime_incident
        corrected_relevant_incidents.loc[index, 'Raw Weighted Downtime'] = raw_weighted_downtime_incident
        try:
            corrected_relevant_incidents.loc[
                index, 'Contribution to downtime %'] = weighted_downtime_incident / site_active_hours_period
            corrected_relevant_incidents.loc[
                index, 'Raw Contribution to downtime %'] = raw_weighted_downtime_incident / site_active_hours_period

        except ZeroDivisionError:
            corrected_relevant_incidents.loc[
                index, 'Contribution to downtime %'] = ""
            corrected_relevant_incidents.loc[
                index, 'Raw Contribution to downtime %'] = ""


    for index, row in corrected_relevant_tracker_inc.iterrows():
        capacity = row['Capacity Related Component']
        active_hours = row['Active Hours (h)']


        try:
            if capacity == float(0) or math.isnan(active_hours) or type(active_hours) == str:
                weighted_downtime_tracker_incident = 0

            else:
                weighted_downtime_tracker_incident = (capacity * active_hours) / site_capacity
        except TypeError:
            weighted_downtime_tracker_incident = 0


        tracker_weighted_downtime[row['ID']] = weighted_downtime_tracker_incident

        corrected_relevant_tracker_inc.loc[index, 'Weighted Downtime'] = weighted_downtime_tracker_incident

        try:
            corrected_relevant_tracker_inc.loc[
                index, 'Contribution to downtime %'] = weighted_downtime_tracker_incident / site_active_hours_period

        except ZeroDivisionError:
            corrected_relevant_tracker_inc.loc[
                index, 'Contribution to downtime %'] = ""


    weighted_downtime_df = pd.DataFrame.from_dict(weighted_downtime, orient='index',
                                                  columns=['Incident weighted downtime (h)'])
    raw_weighted_downtime_df = pd.DataFrame.from_dict(raw_weighted_downtime, orient='index',
                                                      columns=['Incident weighted downtime (h)'])

    weighted_downtime_tracker_df = pd.DataFrame.from_dict(tracker_weighted_downtime, orient='index',
                                                      columns=['Incident weighted downtime (h)'])
    # print(weighted_downtime_df)




    total_weighted_downtime = weighted_downtime_df['Incident weighted downtime (h)'].sum()
    total_raw_weighted_downtime = raw_weighted_downtime_df['Incident weighted downtime (h)'].sum()
    total_weighted_tracker_downtime = weighted_downtime_tracker_df['Incident weighted downtime (h)'].sum()

    try:
        availability_period = ((site_active_hours_period - total_weighted_downtime) / site_active_hours_period)
        raw_availability_period = ((site_active_hours_period - total_raw_weighted_downtime) / site_active_hours_period)
        tracker_availability_period = ((site_active_hours_period - total_weighted_tracker_downtime) / site_active_hours_period)


    except (ZeroDivisionError, RuntimeWarning):
        availability_period = 0
        raw_availability_period = 0
        tracker_availability_period = 0

    return availability_period, raw_availability_period, tracker_availability_period, site_active_hours_period, \
           corrected_relevant_incidents, all_corrected_incidents


def availability_in_period(incidents, tracker_incidents, period, component_data, df_all_irradiance, df_all_export, budget_pr,
                           irradiance_threshold: int = 20, timestamp: int = 15, date: str = "", site_list: list = [],
                           recalculate_value: bool = False):
    granularity = timestamp / 60
    last_timestamp = list(df_all_irradiance["Timestamp"])[-2]
    print(last_timestamp)
    # Get dates from period info
    date_start_str, date_end_str = inputs.choose_period_of_analysis(period, last_timestamp, date=date)
    date_range = date_start_str + " to " + date_end_str
    print(date_range)

    # Get site list --------- could be input
    if len(site_list) == 0:
        site_list = list(set([re.search(r'\[.+\]', site).group().replace('[', "").replace(']', "") for site in
                              df_all_irradiance.loc[:, df_all_irradiance.columns.str.contains('Irradiance')].columns]))
        site_list = [data_treatment.correct_site_name(site) for site in site_list]

    # Get site and fleet capacities --------- could be input
    site_capacities = component_data.loc[component_data['Component Type'] == 'Site'][
                          ['Component', 'Nominal Power DC']].set_index('Component').loc[site_list, :]
    fleet_capacity = site_capacities['Nominal Power DC'].sum()

    # Get only incidents that count for availability, aka, "Not producing"
    incidents = incidents.loc[incidents['Component Status'] == "Not Producing"].reset_index(None, drop=True)

    # Calculate Availability, Active Hours and Corrected Dataframe
    availability_period_per_site = {}
    raw_availability_period_per_site = {}
    tracker_availability_period_per_site = {}
    active_hours_per_site = {}
    incidents_corrected_period_per_site = {}
    all_corrected_incidents_per_site = {}

    for site in site_list:
        availability_period, raw_availability_period, tracker_availability_period, site_active_hours_period, \
        corrected_relevant_incidents, all_corrected_incidents_site = calculate_availability_period(site, incidents,
                                                                                                   tracker_incidents,
                                                                                                   component_data,
                                                                                                   budget_pr,
                                                                                                   df_all_irradiance,
                                                                                                   df_all_export,
                                                                                                   irradiance_threshold,
                                                                                                   date_start_str,
                                                                                                   date_end_str,
                                                                                                   granularity,
                                                                                                   recalculate_value)

        availability_period_per_site[site] = availability_period
        raw_availability_period_per_site[site] = raw_availability_period
        tracker_availability_period_per_site[site] = tracker_availability_period
        active_hours_per_site[site] = site_active_hours_period
        incidents_corrected_period_per_site[site] = corrected_relevant_incidents
        all_corrected_incidents_per_site[site] = all_corrected_incidents_site

    # <editor-fold desc="Add fleet value and company goals values">
    availability_period_per_site['Fleet'] = sum(
        [availability_period_per_site[site] * site_capacities.loc[site, 'Nominal Power DC'] for site in
         site_list]) / fleet_capacity

    raw_availability_period_per_site['Fleet'] = sum(
        [raw_availability_period_per_site[site] * site_capacities.loc[site, 'Nominal Power DC'] for site in
         site_list]) / fleet_capacity

    availability_period_per_site['Company goal'] = 0.944
    availability_period_per_site['Company max goal'] = 0.964
    # </editor-fold>

    availability_period_df = pd.DataFrame.from_dict(availability_period_per_site, orient='index', columns=[
        date_range])  # , orient='index', columns=['Incident weighted downtime (h)'])
    raw_availability_period_df = pd.DataFrame.from_dict(raw_availability_period_per_site, orient='index', columns=[
        date_range])  # , orient='index', columns=['Incident weighted downtime (h)'])
    tracker_availability_period_df = pd.DataFrame.from_dict(tracker_availability_period_per_site, orient='index',
                                                            columns=[date_range])

    activehours_period_df = pd.DataFrame.from_dict(active_hours_per_site, orient='index', columns=[date_range])
    incidents_corrected_period = pd.concat(list(incidents_corrected_period_per_site.values()))
    all_corrected_incidents = pd.concat(list(all_corrected_incidents_per_site.values()))

    return availability_period_df, raw_availability_period_df,tracker_availability_period_df, activehours_period_df, \
           incidents_corrected_period, all_corrected_incidents, date_range


def day_end_availability(pr_data_period_df, final_df_to_add, component_data, tracker_data, all_site_info):
    active_incidents = final_df_to_add["Active incidents"]
    active_tracker_incidents = final_df_to_add["Active tracker incidents"]

    down_capacity_by_site = down_capacity_calculation(active_incidents, component_data)
    dayend_availability = ["{:.2%}".format(float(1 - (down_capacity_by_site.loc[site, "Capacity Related Component"] /
                                                      all_site_info.loc[site, "Nominal Power DC"])))
                           if site in down_capacity_by_site.index.tolist() else "{:.2%}".format(1)
                           for site in pr_data_period_df.index.tolist()]

    down_capacity_by_site_trackers = down_capacity_calculation(active_tracker_incidents, tracker_data)
    dayend_availability_trackers = ["{:.2%}".format(float(1 - (down_capacity_by_site_trackers.loc
                                                               [site, "Capacity Related Component"] /
                                                               all_site_info.loc[site, "Nominal Power DC"])))
                                    if site in down_capacity_by_site_trackers.index.tolist() else "{:.2%}".format(1)
                                    for site in pr_data_period_df.index.tolist()]

    pr_data_period_df.insert(0, "Day-End Availability (%)", dayend_availability)
    pr_data_period_df.insert(3, "Tracker Day-End Availability (%)", dayend_availability_trackers)
    pr_data_period_df["Portfolio"] = [all_site_info.loc[site, "Portfolio"] for site in pr_data_period_df.index.tolist()]
    pr_data_period_df["Fault Status"] = ["Open" if
                                         sum([float(pr_data_period_df.loc[site, "Day-End Availability (%)"][:-1]),
                                              float(pr_data_period_df.loc[site, "Tracker Day-End Availability (%)"][:-1]
                                                    )]) / 2 < 100 else "Closed"
                                         for site in pr_data_period_df.index.tolist()]

    pr_data_period_df.sort_values(by=["Portfolio", "Day-End Availability (%)"], inplace=True)

    return pr_data_period_df


def down_capacity_calculation(df, component_data):
    df["Active Parents"] = "No"
    df = df.loc[df["Component Status"] == "Not Producing"]

    for index, row in df.iterrows():
        site = row["Site Name"]
        component = row["Related Component"]

        parents = (component_data.loc[(component_data['Component'] == component) &
                                      (component_data['Site'] == site)]).loc[:,
                  component_data.columns.str.contains('Parent')].values.flatten().tolist()

        other_site_incidents = df.loc[df["Site Name"] == site]["Related Component"].values.tolist()
        active_parents = list(set(other_site_incidents).intersection(parents))

        if len(active_parents) > 0:
            df.loc[index, "Active Parents"] = "Yes"

    corrected_active_incidents = df.loc[df["Active Parents"] == "No"]
    down_capacity_by_site = corrected_active_incidents.groupby(['Site Name']).sum()

    return down_capacity_by_site


# </editor-fold>


# <editor-fold desc="PR Calculation">

def pr_in_period(incidents_period, availability_period, raw_availability_period, tracker_availability_period,
                 period, component_data,df_all_irradiance, df_all_export, budget_pr, budget_export, budget_irradiance,
                 irradiance_threshold: int = 20, timestamp: int = 15, date: str = "", site_list: list = []):
    # Get site list --------- could be input
    if len(site_list) == 0:
        site_list = list(set([re.search(r'\[.+\]', site).group().replace('[', "").replace(']', "") for site in
                              df_all_irradiance.loc[:, df_all_irradiance.columns.str.contains('Irradiance')].columns]))
        site_list = [data_treatment.correct_site_name(site) for site in site_list]

    # Get site and fleet capacities --------- could be input
    site_capacities = component_data.loc[component_data['Component Type'] == 'Site'][
                          ['Component', 'Nominal Power DC']].set_index('Component').loc[site_list, :]
    fleet_capacity = site_capacities['Nominal Power DC'].sum()

    # Get dates from period info
    last_timestamp = list(df_all_irradiance["Timestamp"])[-2]
    date_start_str, date_end_str = inputs.choose_period_of_analysis(period, last_timestamp, date=date)

    # Get start time analysis
    date_start_avail_analysis = datetime.strptime(date_start_str, '%Y-%m-%d').date()
    timestamp_start_avail_analysis = datetime.strptime(date_start_str + " 00:00:00", '%Y-%m-%d %H:%M:%S')

    # Get end time analysis
    date_end_avail_analysis = datetime.strptime(date_end_str, '%Y-%m-%d').date()
    date_end_str_event = str(datetime.strptime(date_end_str, '%Y-%m-%d').date() + dt.timedelta(days=1))
    timestamp_end_avail_analysis = datetime.strptime(date_end_str_event + " 00:00:00", '%Y-%m-%d %H:%M:%S')

    # Get Data to analyse: incidents, export data and irradiance data
    df_export_period = df_all_export.loc[(df_all_export['Timestamp'] >= timestamp_start_avail_analysis) & (
        df_all_export['Timestamp'] <= timestamp_end_avail_analysis)].set_index('Timestamp')

    df_irradiance_period = df_all_irradiance.loc[(df_all_irradiance['Timestamp'] >= timestamp_start_avail_analysis) & (
        df_all_irradiance['Timestamp'] <= timestamp_end_avail_analysis)].set_index('Timestamp')

    print(date_start_avail_analysis, " ", date_end_avail_analysis)

    pr_period_per_site = {}
    data_period_per_site = {}

    for site in site_list:
        print(site)
        export_column = list(df_export_period.columns[df_export_period.columns.str.contains(str(site))].values)[0]
        site_capacity = site_capacities.loc[site, "Nominal Power DC"]

        if len(export_column) > 0:
            export_data = df_export_period[[export_column]]
            exported_energy = float(export_data.sum().iloc[0])

            # Get relevant irradiance data and calculate expected energy
            irradiance_site = df_irradiance_period.loc[:, df_irradiance_period.columns.str.contains(site)]
            actual_column, curated_column, data_gaps_proportion, poa_avg_column = data_treatment.get_actual_irradiance_column(
                irradiance_site)

            if actual_column is not None:
                actual_irradiance_site = irradiance_site.loc[:, actual_column]
            else:
                actual_irradiance_site = irradiance_site.loc[:, poa_avg_column]

            start_timestamp = actual_irradiance_site.index[0]
            end_timestamp = actual_irradiance_site.index[-1]
            start_day = start_timestamp.date()
            end_day = actual_irradiance_site.index[-2].date()

            # Calculate Expected Energy in period
            expected_energy, expected_energy_info = calculate_expected_energy(site, start_timestamp, end_timestamp,
                                                                              budget_export, budget_irradiance,
                                                                              actual_irradiance_site)

            # Calculate Energy Lost
            energy_lost = incidents_period.loc[incidents_period['Site Name'] == site][
                              'Energy Lost (MWh)'].replace("", 0).sum() * 1000

            # Calculate PRs
            actual_pr = exported_energy / ((actual_irradiance_site.sum() / 4000) * site_capacity)
            # print(exported_energy)
            # print(actual_irradiance_site)
            # print(site_capacity)
            # print(actual_pr)
            possible_pr = (exported_energy + energy_lost) / ((actual_irradiance_site.sum() / 4000) * site_capacity)

            # Calculate Variances
            actual_expected_variance = (exported_energy / expected_energy) - 1
            corrected_actual_expected_variance = ((exported_energy + energy_lost) / expected_energy) - 1

            # Get availability in Period
            availability = availability_period.loc[site, :].values[0]
            raw_availability = raw_availability_period.loc[site, :].values[0]
            tracker_availability = tracker_availability_period.loc[site, :].values[0]

            # print(site, "\n Expected Energy: ", expected_energy,"\n Exported Energy: " ,exported_energy, "\n Energy Lost: ", energy_lost)
            # incidents_period_site = incidents_period.loc[(incidents_period['Component Status'] == "Not Producing") & (incidents['Site Name'] == site) & (incidents['Event Start Time'] > )].reset_index(None, drop=True)

            # Store relevant data

            data_period_per_site[site] = ("{:.2%}".format(availability),
                                          "{:.2%}".format(raw_availability),
                                          "{:.2%}".format(tracker_availability),
                                          "{:.2%}".format(actual_pr),
                                          "{:,.2f}".format(exported_energy),
                                          "{:,.2f}".format(energy_lost),
                                          "{:.2%}".format(possible_pr),
                                          "{:,.2f}".format(actual_irradiance_site.sum() / 4000),
                                          "{:,.2f}".format(expected_energy),
                                          "{:.2%}".format(actual_expected_variance),
                                          "{:.2%}".format(corrected_actual_expected_variance),
                                          "{:.2%}".format(data_gaps_proportion))

        else:
            print(site, " Exported energy data not found.")
            continue

    data_period_df = pd.DataFrame.from_dict(data_period_per_site, columns=['Availability (%)', 'Raw Availability (%)',
                                                                           'Tracker Availability (%)', 'Actual PR (%)',
                                                                           "Actual Exported Energy (kWh)",
                                                                           "Energy Lost (kWh)", "Corrected PR (%)",
                                                                           "Actual Irradiance (kWh/m2)",
                                                                           "Expected Energy (kWh)",
                                                                           "Actual vs Expected Energy Variance",
                                                                           "Corrected Actual vs Expected Energy Variance",
                                                                           "Data Gaps (%)"], orient='index')

    return data_period_df


# </editor-fold>


# </editor-fold>

# <editor-fold desc="Curtailment and Clipping">

def curtailment_classic(source_folder, geography, geography_folder, site_selection, period,
                        irradiance_threshold: int = 20):

    df_irradiance, df_power, active_power_setpoint_df, component_data, tracker_data, fmeca_data, site_capacities, \
    fleet_capacity, budget_irradiance, budget_pr, budget_export, all_site_info, incidents, dest_file = \
        data_acquisition.read_curtailment_dataframes(source_folder, geography, geography_folder, site_selection,
                                                     period, irradiance_threshold)

    site_list_setpoint = list(set([re.search(r'\[.+\]', site).group().replace('[', "").replace(']', "") for site in
                                   active_power_setpoint_df.loc[:,
                                   active_power_setpoint_df.columns.str.contains('etpoint')].columns]))

    site_list = [site for site in site_selection if site in site_list_setpoint]
    diff_site_list = list(set(site_selection).difference(site_list))

    print("The following sites were removed because there is no setpoint data for them: \n", diff_site_list)
    print("New site list: \n", site_list)

    # <editor-fold desc="Curtailment Calculation">
    power_dfs_files = {}
    curtailment_events_by_site = {}
    curtailment_events_by_site_to_et = {}
    monthly_curtailment_by_site = {}

    for site in site_list:
        site_info = all_site_info.loc[site, :]
        max_export_capacity = site_info['Maximum Export Capacity']

        nominal_power = site_info['Nominal Power DC']

        # Get curated site irradiance
        irradiance_site = df_irradiance.loc[:, df_irradiance.columns.str.contains(site + "|Timestamp")]
        irradiance_site_curated = irradiance_site.loc[:, irradiance_site.columns.str.contains('curated|Timestamp')]

        # Get site power
        power_site = df_power.loc[:, df_power.columns.str.contains(site + "|Timestamp")]

        incidents_site = incidents.loc[incidents['Site Name'] == site].reset_index(None, drop=True)
        np_incidents_site = incidents_site.loc[(incidents_site['Component Status'] == "Not Producing") & ~(
            incidents_site['Failure Mode'] == "Curtailment")]

        # break
        # get incidents of curtailment for each site
        site_max_active_power_setpoint = all_site_info.loc[site, "Maximum Export Capacity"]
        df_curtailment_events_site = active_power_setpoint_df.loc[:, active_power_setpoint_df.columns.str.contains(
            site + "|Timestamp")].dropna().reset_index(drop=True)

        setpoint_column = df_curtailment_events_site.columns[1]

        print(df_curtailment_events_site)
        print(setpoint_column)

        # <editor-fold desc="Get timestamps of curtailment events">
        end_timestamps = df_curtailment_events_site.loc[
            df_curtailment_events_site[setpoint_column] >= site_max_active_power_setpoint]
        end_timestamps_index = list(end_timestamps.index)

        start_timestamps_index = end_timestamps.index + 1
        start_timestamps_index = list(start_timestamps_index.insert(0, 0))[:-1]

        start_data = df_curtailment_events_site.iloc[start_timestamps_index, :]
        # start_timestamps = list(start_data.index)
        start_timestamps = list(start_data["Timestamp"])
        setpoints = list(start_data[setpoint_column])

        end_data = df_curtailment_events_site.iloc[end_timestamps_index, :]
        end_timestamps = list(end_data["Timestamp"])
        # </editor-fold>

        # print(start_timestamps)

        if len(start_timestamps) == 0:
            pass
        else:
            data_for_df = {"Site Name": [site] * len(setpoints),
                           "Related Component": [site] * len(setpoints),
                           "Capacity Related Component": [nominal_power] * len(setpoints),
                           "Component Status": ["Not Producing"] * len(setpoints),
                           "Setpoint": setpoints,
                           "Event Start Time": start_timestamps,
                           "Event End Time": end_timestamps,
                           "Rounded Event Start Time": pd.Series(
                               [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S') for timestamp in
                                start_timestamps]).dt.ceil("1min"),
                           "Rounded Event End Time": pd.Series(
                               [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S') for timestamp in
                                end_timestamps]).dt.ceil("1min"),
                           "Duration (h)": [0] * len(setpoints),
                           "Active hours (h)": [0] * len(setpoints),
                           "Expected Energy Loss (kWh)": [0] * len(setpoints),
                           "Corrected Expected Energy Loss (kWh)": [0] * len(setpoints),
                           "Comments": [str(site + " is curtailed at " + str(setpoint) + " kW") for setpoint in
                                        setpoints]}

            curtailment_inc_df = pd.DataFrame(data_for_df)
            curtailment_inc_df["Curtailment Event"] = ["x"] * len(setpoints)
            curtailment_inc_df = curtailment_inc_df.loc[curtailment_inc_df["Setpoint"] < site_max_active_power_setpoint]

            for index, row in curtailment_inc_df.iterrows():
                stime = row['Rounded Event Start Time']
                etime = row['Rounded Event End Time']

                print(row[["Site Name", "Event Start Time", "Event End Time"]])

                budget_pr_stime = budget_pr.loc[site, datetime.strptime(str(stime.date())[:-2] + "01 00:00:00",
                                                                        '%Y-%m-%d %H:%M:%S')]

                budget_pr_etime = budget_pr.loc[site, datetime.strptime(str(etime.date())[:-2] + "01 00:00:00",
                                                                        '%Y-%m-%d %H:%M:%S')]

                if budget_pr_stime == 0 and budget_pr_etime == 0:
                    curtailment_inc_df.loc[index, "Expected Energy Loss (kWh)"] = 0
                    curtailment_inc_df.loc[index, "Corrected Expected Energy Loss (kWh)"] = 0

                else:
                    slice_power_df_site = power_site.loc[
                        (power_site['Timestamp'] <= etime) & (power_site['Timestamp'] >= stime)]
                    slice_irradiance_df_site = irradiance_site_curated.loc[
                        (irradiance_site_curated['Timestamp'] <= etime) & (
                            irradiance_site_curated['Timestamp'] >= stime)]

                    power_irradiance_site = pd.merge_asof(slice_irradiance_df_site, slice_power_df_site, on='Timestamp')
                    print(power_irradiance_site.columns)

                    irradiance_column = list(power_irradiance_site.columns[tuple([power_irradiance_site.columns.str.
                                                                                 contains('Irradiance')])])[0]
                    power_column = list(
                        power_irradiance_site.columns[tuple([power_irradiance_site.columns.str.
                                                            contains('Active power|Power')])])[0]

                    if power_irradiance_site[irradiance_column].sum() == 0:

                        curtailment_inc_df.loc[index, "Expected Energy Loss (kWh)"] = 0
                        curtailment_inc_df.loc[index, "Corrected Expected Energy Loss (kWh)"] = 0
                        curtailment_inc_df.loc[index, "Active Hours (h)"] = 0
                        curtailment_inc_df.loc[index, "Duration (h)"] = 0

                    else:

                        print("Adding budget PR and Available capacity at each moment")
                        power_irradiance_site[["Budget PR", 'Available Capacity']] = \
                            [[budget_pr.loc[site, datetime.strptime(str(timestamp.date())[:-2] +
                                                                    "01 00:00:00", '%Y-%m-%d %H:%M:%S')],
                              (nominal_power -
                               np_incidents_site.loc[(np_incidents_site['Event Start Time'] <= timestamp) &
                                                     ((np_incidents_site['Event End Time'] >= timestamp) | (
                                                         np_incidents_site['Event End Time'].isna()))][
                                   'Capacity Related Component'].sum()) / nominal_power] for timestamp in
                             power_irradiance_site['Timestamp']]

                        print("Correcting Available capacity at each moment")
                        power_irradiance_site['Available Capacity'] = [value if value > 0 else 0 for value in
                                                                       power_irradiance_site['Available Capacity']]

                        # Complete dataframes of power with expected power
                        print("Expected and Corrected Expected Power at each moment")
                        power_irradiance_site['Expected Power'] = [
                            (nominal_power * row["Budget PR"] * row[irradiance_column] / 1000) for index, row in
                            power_irradiance_site.iterrows()]
                        power_irradiance_site['Corrected Expected Power'] = [
                            (nominal_power * row["Budget PR"] * row['Available Capacity'] * row[
                                irradiance_column] / 1000)
                            for index, row in power_irradiance_site.iterrows()]

                        print("Expected and Corrected Power Clipped at each moment")
                        power_irradiance_site['Power Lost'] = [(row['Expected Power'] - row[power_column]) if
                                                               (row['Expected Power'] - row[power_column]) > 0 else 0
                                                               for index, row in power_irradiance_site.iterrows()]

                        power_irradiance_site['Corrected Power Lost'] = [
                            (row['Corrected Expected Power'] - row[power_column]) if (row['Corrected Expected Power'] -
                                                                                      row[
                                                                                          power_column]) > 0 else 0 for
                            index, row in power_irradiance_site.iterrows()]

                        # print(power_irradiance_site)

                        curtailment_inc_df.loc[index, "Expected Energy Loss (kWh)"] = power_irradiance_site[
                                                                                          'Power Lost'].sum() / 60 \
                            if (power_irradiance_site['Power Lost'].sum() / 60) > 0 else 0

                        curtailment_inc_df.loc[index, "Corrected Expected Energy Loss (kWh)"] = power_irradiance_site[
                                                                                                    'Corrected Power Lost'].sum() / 60 \
                            if (power_irradiance_site['Corrected Power Lost'].sum() / 60) > 0 else 0

                        curtailment_inc_df.loc[index, "Duration (h)"] = power_irradiance_site[power_column].count() / 60
                        curtailment_inc_df.loc[index, "Active Hours (h)"] = power_irradiance_site.loc[
                                                                                (power_irradiance_site[
                                                                                     power_column] <= 0) &
                                                                                (power_irradiance_site[
                                                                                     irradiance_column] > 20)][
                                                                                power_column].count() / 60

            curtailment_inc_df["Month"] = [timestamp.strftime("%Y-%m")
                                           for timestamp in curtailment_inc_df["Event Start Time"]]



            df_month = curtailment_inc_df[['Month', "Expected Energy Loss (kWh)",
                                           "Corrected Expected Energy Loss (kWh)"]].groupby(['Month']).sum() #[["Expected Energy Loss (kWh)", "Corrected Expected Energy Loss (kWh)"]]

            # print(curtailment_inc_df)
            curtailment_inc_df["Energy Lost (MWh)"] = curtailment_inc_df["Corrected Expected Energy Loss (kWh)"]/1000
            curtailment_events_by_site[site] = curtailment_inc_df
            monthly_curtailment_by_site[site] = df_month
    # </editor-fold>

    site_list = [site for site in site_list if site in monthly_curtailment_by_site.keys()]

    return curtailment_events_by_site, monthly_curtailment_by_site, site_list, dest_file, component_data, fmeca_data


def clipping_classic(source_folder, geography, geography_folder, site_selection, period,
                     irradiance_threshold: int = 20):

    df_irradiance, df_power, component_data, tracker_data, fmeca_data, site_capacities, fleet_capacity, \
    budget_irradiance, budget_pr, budget_export, all_site_info, incidents, dest_file, folder_img = \
        data_acquisition.read_clipping_dataframes(source_folder, geography, geography_folder, site_selection,
                                                  period, irradiance_threshold)

    #<editor-fold desc="Clipping Calculation">
    summaries_by_site = {}
    graphs_by_site = {}

    for site in site_selection:
        incidents_site = incidents.loc[incidents['Site Name'] == site].reset_index(None, drop=True)
        np_incidents_site = incidents_site.loc[incidents_site['Component Status'] == "Not Producing"]

        print(site)

        summaries = {}

        site_info = all_site_info.loc[site, :]
        max_export_capacity = site_info['Maximum Export Capacity']

        start_of_reporting = site_info['Start of reporting']

        if start_of_reporting.year < 2021:
            start_of_reporting = start_of_reporting.replace(year=2021, month=1)

        buffer = 0.005
        max_export_capacity_buffed = max_export_capacity * (1 - buffer)
        nominal_power = site_info['Nominal Power DC']

        print("Nominal power: ", nominal_power)
        print("Effective Max export capacity: ", max_export_capacity_buffed)
        print("Start of reporting: ", start_of_reporting)

        # <editor-fold desc="Complete DFs of irradiance and power with Availability and Budget PR">
        # Complete dataframes of power with Availability and Budget PR ------------------------------------------------

        start = time.time()

        # Get curated site irradiance
        irradiance_site = df_irradiance.loc[:, df_irradiance.columns.str.contains(site + "|Timestamp")]
        irradiance_site_curated = irradiance_site.loc[:, irradiance_site.columns.str.contains('curated|Timestamp')]

        # Get site power
        power_site = df_power.loc[:, df_power.columns.str.contains(site + "|Timestamp")]

        # power_irradiance_site = pd.concat([irradiance_site_curated,power_site])
        power_irradiance_site = pd.merge_asof(irradiance_site_curated, power_site, on='Timestamp')
        power_irradiance_site = power_irradiance_site.loc[power_irradiance_site['Timestamp'] >=
                                                          datetime(start_of_reporting.year, start_of_reporting.month,
                                                                   start_of_reporting.day, 0, 0, 0)]

        print("Adding budget PR and Available capacity at each moment")
        power_irradiance_site[['Budget PR', 'Available Capacity']] = [[budget_pr.loc[site, datetime.strptime(str(
            timestamp.date())[:-2] + "01 00:00:00", '%Y-%m-%d %H:%M:%S')], (nominal_power - np_incidents_site.loc[
            (np_incidents_site['Event Start Time'] <= timestamp) & (np_incidents_site['Event End Time'] >= timestamp)][
            'Capacity Related Component'].sum()) / nominal_power] for timestamp in power_irradiance_site['Timestamp']]

        print("Correcting Available capacity at each moment")
        power_irradiance_site['Available Capacity'] = [value if value > 0 else 0 for value in
                                                       power_irradiance_site['Available Capacity']]

        irradiance_column = \
            list(power_irradiance_site.columns[tuple([power_irradiance_site.columns.str.contains('Irradiance')])])[0]
        power_column = list(power_irradiance_site.columns[tuple([power_irradiance_site.columns.str.contains('ower')])])[
            0]

        end = time.time()
        print(end - start)
        # </editor-fold>

        # <editor-fold desc="Complete DFs of power with expected power">
        # Complete dataframes of power with expected power  --------------------------------------------------------
        print("Expected and Corrected Expected Power at each moment")
        power_irradiance_site['Expected Power'] = [(nominal_power * row["Budget PR"] * row[irradiance_column] / 1000)
                                                   for index, row in power_irradiance_site.iterrows()]

        power_irradiance_site['Corrected Expected Power'] = [
            (nominal_power * row["Budget PR"] * row['Available Capacity'] * row[irradiance_column] / 1000) for
            index, row in power_irradiance_site.iterrows()]

        print("Expected and Corrected Power Clipped at each moment")
        power_irradiance_site['Power Clipped'] = [
            (row['Expected Power'] - row[power_column]) if row[power_column] >= max_export_capacity_buffed and (
                row['Expected Power'] - row[power_column]) > 0 else 0 for index, row in
            power_irradiance_site.iterrows()]

        power_irradiance_site['Corrected Power Clipped'] = [
            (row['Corrected Expected Power'] - row[power_column]) if row[power_column] >= max_export_capacity_buffed
                                                                     and (row['Corrected Expected Power'] -
                                                                          row[power_column]) > 0 else 0
            for index, row in power_irradiance_site.iterrows()]
        # </editor-fold>

        power_irradiance_site[['Day', 'Month']] = [[timestamp.date(), timestamp.strftime("%Y-%m")] for timestamp in
                                                   power_irradiance_site["Timestamp"]]

        # print('Done')

        # Create daily and monthly summaries --------------------------------------------------------------------------------

        daily_summary = power_irradiance_site.groupby(['Day']).sum()[
                            [power_column, "Power Clipped", "Corrected Power Clipped"]] / 60
        monthly_summary = power_irradiance_site.groupby(['Month']).sum()[
                              [power_column, "Power Clipped", "Corrected Power Clipped"]] / 60

        daily_summary['% of loss'] = daily_summary["Power Clipped"] / (
            daily_summary[power_column] + daily_summary["Power Clipped"]) * 100
        monthly_summary['% of loss'] = monthly_summary["Power Clipped"] / (
            monthly_summary[power_column] + monthly_summary["Power Clipped"]) * 100
        # daily_summary['% of loss'] = ["{:.2%}".format(value) for value in daily_summary['% of loss']]

        daily_summary['% of loss corrected'] = daily_summary["Corrected Power Clipped"] / (
            daily_summary[power_column] + daily_summary["Corrected Power Clipped"]) * 100
        monthly_summary['% of loss corrected'] = monthly_summary["Corrected Power Clipped"] / (
            monthly_summary[power_column] + monthly_summary["Corrected Power Clipped"]) * 100
        # daily_summary['% of loss corrected'] = ["{:.2%}".format(value) for value in daily_summary['% of loss corrected']]

        # Save summaries
        summaries['Daily'] = daily_summary
        summaries['Monthly'] = monthly_summary
        summaries_by_site[site] = summaries

        # Plot graphs
        graphs_by_gran = {}

        # </editor-fold>

        graphs_site = visuals.clipping_visuals(summaries, folder_img, site)
        graphs_by_site[site] = graphs_site

    return summaries_by_site, site_selection, dest_file, component_data, fmeca_data, graphs_by_site

# </editor-fold>
