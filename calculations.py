import re
import sys
import pandas as pd
import numpy as np
import data_treatment
import data_analysis
import calendar


# Analysis with dataframe output

def analysis_component_incidents(df_incidents_analysis, site_list, df_list_closed, df_list_active, df_info_sunlight):
    df_incidents_analysis = data_treatment.fill_events_analysis_dataframe(df_incidents_analysis, df_info_sunlight)

    for site in site_list:
        index_site_array = df_info_sunlight[df_info_sunlight['Site'] == site].index.values
        index_site = int(index_site_array[0])

        df_closed_events = df_list_closed[site]
        df_active_events = df_list_active[site]

        # Add efect of closed events
        df_incidents_analysis = data_analysis.analysis_closed_incidents(site, index_site, df_incidents_analysis,
                                                                        df_closed_events,
                                                                        df_info_sunlight)

        # Add effect of active events
        df_incidents_analysis = data_analysis.analysis_active_incidents(site, index_site, df_incidents_analysis,
                                                                        df_active_events,
                                                                        df_info_sunlight)

    # print(df_incidents_analysis)
    return df_incidents_analysis


def analysis_tracker_incidents(df_tracker_analysis, df_tracker_closed, df_tracker_active, df_info_sunlight):
    df_tracker_analysis = data_treatment.fill_events_analysis_dataframe(df_tracker_analysis, df_info_sunlight)

    # Add effect of closed events
    df_tracker_analysis = data_analysis.analysis_closed_tracker_incidents(df_tracker_analysis, df_tracker_closed,
                                                                          df_info_sunlight)

    # Add effect of active events
    df_tracker_analysis = data_analysis.analysis_active_tracker_incidents(df_tracker_analysis, df_tracker_active,
                                                                          df_info_sunlight)

    # print(df_tracker_analysis)

    return df_tracker_analysis


# <editor-fold desc="Summaries">

def get_events_summary_per_fault_component(components_to_analyse, inverter_incidents_site, inverter_operation,
                                           df_operation_hours):
    unit_failure_dict = {}
    events_summary_dict = {}
    count = 0

    for unit in inverter_operation.keys():

        # From unit get component, aka, Inv 01.r2 --> Inv 01
        try:
            component = unit.replace(re.search(r'\.r\d*', unit).group(), "")
        except AttributeError:
            component = unit

        # Get unit incidents
        unit_incidents = inverter_incidents_site.loc[inverter_incidents_site['Unit Component'] == unit]
        # print(unit_incidents)

        unit_age = \
            df_operation_hours.loc[df_operation_hours['Timestamp'] == inverter_operation[unit][1]][component].values[0]
        # print(unit, unit_age)

        # Get last time of operation from timestamp, if time empty, look for last datapoint
        changed = False
        while np.isnan(unit_age):
            rounded_incident_time = rounded_incident_time - pd.Timedelta(minutes=15)
            incident_operation_time = \
                df_operation_hours.loc[df_operation_hours['Timestamp'] == rounded_incident_time][component].values[0]
            changed = True
        if changed is True:
            print("Changed rounded time to forward timestamp because backward was NaN, new timestamp: ",
                  rounded_incident_time)

        # From original dataframe, reduce to dataframe with required data
        components_failed = list(set(unit_incidents['Fault Component']))
        events_summary = unit_incidents[['Unit Component', 'Fault Component', 'Event Start Time', 'Operation Time']]
        events_summary['Time to Failure'] = ""
        events_summary['Failure'] = "Yes"

        # Add last entries of dataframe, aka, hours of operation at the last point of analysis
        end_of_analysis_entries = pd.DataFrame({'Unit Component': [unit] * len(components_to_analyse),
                                                'Fault Component': components_to_analyse,
                                                'Event Start Time': [inverter_operation[unit][1]] * len(
                                                    components_to_analyse),
                                                'Operation Time': [unit_age] * len(components_to_analyse),
                                                'Time to Failure': [""] * len(components_to_analyse),
                                                'Failure': ['No'] * len(components_to_analyse)})

        # Get complete events summary
        events_summary = pd.concat([events_summary, end_of_analysis_entries]).sort_values(
            by=['Event Start Time', 'Fault Component']).reset_index(None, drop=True)
        events_summary = events_summary.loc[
            ~(events_summary['Fault Component'] == "Phase Fuse") & ~(events_summary['Fault Component'] == "Unknown")]

        print(events_summary)
        print("\n")

        print(components_failed)
        # Separate multiple components incidents to calculate spare parts
        for failed_component in components_failed:
            if ";" in failed_component:
                incidents_to_split = events_summary.loc[events_summary['Fault Component'] == failed_component]
                index_incidents_to_split = incidents_to_split.index
                actual_components = failed_component.split(';')
                n_repeats = len(actual_components)

                splitted_incidents = pd.concat([incidents_to_split] * len(actual_components))
                splitted_incidents['Fault Component'] = actual_components * len(incidents_to_split)
                splitted_incidents = splitted_incidents.sort_values(
                    by=['Event Start Time', 'Fault Component']).reset_index(None, drop=True)

                events_summary = pd.concat(
                    [events_summary.drop(index=index_incidents_to_split), splitted_incidents]).sort_values(
                    by=['Event Start Time', 'Fault Component']).reset_index(None, drop=True)

                """print(events_summary)
                print(splitted_incidents)
                print(new_events_summary)"""

        # Add time to failure
        for fault_component in components_to_analyse:

            fc_events_summary = events_summary.loc[events_summary['Fault Component'] == fault_component]
            n_incidents = len(fc_events_summary)

            if n_incidents == 1:
                index_of_incident = int(fc_events_summary.index.values)
                events_summary.loc[index_of_incident, "Time to Failure"] = fc_events_summary['Operation Time'][
                    index_of_incident]

            else:
                op_time = list(fc_events_summary['Operation Time'])
                op_time_2 = list(fc_events_summary['Operation Time'])
                op_time_2.insert(0, 0)
                del op_time_2[-1]

                fc_events_summary['Time to Failure'] = [op_time_i - op_time_2_i for op_time_i, op_time_2_i in
                                                        zip(op_time, op_time_2)]

                for index, row in fc_events_summary.iterrows():
                    events_summary.loc[index, "Time to Failure"] = row['Time to Failure']

            fr_calc_events_summary = events_summary.loc[events_summary['Fault Component'] == fault_component]
            n_incidents = len(fr_calc_events_summary.loc[fr_calc_events_summary['Failure'] == 'Yes'])
            n_hours = sum(fr_calc_events_summary['Time to Failure'])
            failure_rate = (n_incidents / n_hours) * 1000

            # print(unit,fault_component, n_incidents, n_hours, failure_rate)

        try:
            all_events_summary = pd.concat([all_events_summary,  events_summary])
            # sort_values(by = ['Event Start Time', 'Fault Component']).reset_index(None, drop=True)
        except NameError:
            all_events_summary = events_summary

        # print(events_summary)

        # print(unit, components_failed)

        unit_failure_dict[unit] = {'Incidents': unit_incidents, 'Unit Age': unit_age, 'Events Summary': events_summary}
        events_summary_dict[unit] = events_summary

    return events_summary_dict, unit_failure_dict, all_events_summary


def get_events_summary_per_failure_mode(components_to_analyse, inverter_incidents_site, inverter_operation,
                                        df_operation_hours):
    unit_failure_dict = {}
    events_summary_dict = {}
    count = 0

    for unit in inverter_operation.keys():

        # From unit get component, aka, Inv 01.r2 --> Inv 01
        try:
            component = unit.replace(re.search(r'\.r\d*', unit).group(), "")
        except AttributeError:
            component = unit

        # Get unit incidents
        unit_incidents = inverter_incidents_site.loc[inverter_incidents_site['Unit Component'] == unit]
        # print(unit_incidents)

        unit_age = \
            df_operation_hours.loc[df_operation_hours['Timestamp'] == inverter_operation[unit][1]][component].values[0]
        # print(unit, unit_age)

        # Get last time of operation from timestamp, if time empty, look for last datapoint
        changed = False
        while np.isnan(unit_age):
            rounded_incident_time = rounded_incident_time - pd.Timedelta(minutes=15)
            incident_operation_time = \
                df_operation_hours.loc[df_operation_hours['Timestamp'] == rounded_incident_time][component].values[0]
            changed = True
        if changed == True:
            print("Changed rounded time to forward timestamp because backward was NaN, new timestamp: ",
                  rounded_incident_time)

        # From original dataframe, reduce to dataframe with required data
        components_failed = list(set(unit_incidents['Fault Component']))
        events_summary = unit_incidents[
            ['Unit Component', 'Fault Component', 'Failure Mode', 'Event Start Time', 'Operation Time']]
        events_summary['Time to Failure'] = ""
        events_summary['Failure'] = "Yes"

        # Add last entries of dataframe, aka, hours of operation at the last point of analysis
        end_of_analysis_entries = pd.DataFrame({'Unit Component': [unit] * len(components_to_analyse),
                                                'Fault Component': components_to_analyse,
                                                'Failure Mode': [""] * len(components_to_analyse),
                                                'Event Start Time': [inverter_operation[unit][1]] * len(
                                                    components_to_analyse),
                                                'Operation Time': [unit_age] * len(components_to_analyse),
                                                'Time to Failure': [""] * len(components_to_analyse),
                                                'Failure': ['No'] * len(components_to_analyse)})

        # Get complete events summary
        events_summary = pd.concat([events_summary, end_of_analysis_entries]).sort_values(
            by=['Event Start Time', 'Fault Component']).reset_index(None, drop=True)
        events_summary = events_summary.loc[
            ~(events_summary['Fault Component'] == "Phase Fuse") & ~(events_summary['Fault Component'] == "Unknown")]

        print(events_summary)
        print("\n")

        print(components_failed)
        # Separate multiple components incidents to calculate spare parts
        for failed_component in components_failed:
            if ";" in failed_component:
                incidents_to_split = events_summary.loc[events_summary['Fault Component'] == failed_component]
                index_incidents_to_split = incidents_to_split.index
                actual_components = failed_component.split(';')
                n_repeats = len(actual_components)

                splitted_incidents = pd.concat([incidents_to_split] * len(actual_components))
                splitted_incidents['Fault Component'] = actual_components * len(incidents_to_split)
                splitted_incidents = splitted_incidents.sort_values(
                    by=['Event Start Time', 'Fault Component']).reset_index(None, drop=True)

                events_summary = pd.concat(
                    [events_summary.drop(index=index_incidents_to_split), splitted_incidents]).sort_values(
                    by=['Event Start Time', 'Fault Component']).reset_index(None, drop=True)

                """print(events_summary)
                print(splitted_incidents)
                print(new_events_summary)"""

        # Add time to failure
        for fault_component in components_to_analyse:

            fc_events_summary = events_summary.loc[events_summary['Fault Component'] == fault_component]
            n_incidents = len(fc_events_summary)

            if n_incidents == 1:
                index_of_incident = int(fc_events_summary.index.values)
                events_summary.loc[index_of_incident, "Time to Failure"] = fc_events_summary['Operation Time'][
                    index_of_incident]

            else:
                op_time = list(fc_events_summary['Operation Time'])
                op_time_2 = list(fc_events_summary['Operation Time'])
                op_time_2.insert(0, 0)
                del op_time_2[-1]

                fc_events_summary['Time to Failure'] = [op_time_i - op_time_2_i for op_time_i, op_time_2_i in
                                                        zip(op_time, op_time_2)]

                for index, row in fc_events_summary.iterrows():
                    events_summary.loc[index, "Time to Failure"] = row['Time to Failure']

            fr_calc_events_summary = events_summary.loc[events_summary['Fault Component'] == fault_component]
            n_incidents = len(fr_calc_events_summary.loc[fr_calc_events_summary['Failure'] == 'Yes'])
            n_hours = sum(fr_calc_events_summary['Time to Failure'])
            failure_rate = (n_incidents / n_hours) * 1000

            # print(unit,fault_component, n_incidents, n_hours, failure_rate)

        try:
            all_events_summary = pd.concat([all_events_summary,
                                            events_summary])  # .sort_values(by = ['Event Start Time', 'Fault Component']).reset_index(None, drop=True)
        except NameError:
            all_events_summary = events_summary

        # print(events_summary)

        # print(unit, components_failed)

        unit_failure_dict[unit] = {'Incidents': unit_incidents, 'Unit Age': unit_age, 'Events Summary': events_summary}
        events_summary_dict[unit] = events_summary

    return events_summary_dict, unit_failure_dict, all_events_summary


# </editor-fold>

# <editor-fold desc="PR Calculation">


def calculate_pr_inverters(inverter_list, all_inverter_power_data_dict, site_info, general_info,
                           pr_type: str = 'raw', granularity: str = 'daily'):
    possible_prs = ['raw', 'corrected', 'corrected_DCfocus']
    possible_gran = ['daily', 'monthly']

    if pr_type not in possible_prs:
        print('Possible PR types: ' + str(possible_prs) + "\n Your input: " + str(pr_type))
        print('Please try again. :)')
        sys.exit()

    if granularity not in possible_gran:
        print('Possible PR types: ' + str(possible_gran) + "\n Your input: " + str(granularity))
        print('Please try again. :)')
        sys.exit()

    days_under_analysis = site_info['Days']
    months_under_analysis = site_info['Months']

    if pr_type == 'raw' and granularity == 'daily':
        for inverter in inverter_list:
            print(inverter)
            power_data = all_inverter_power_data_dict[inverter]['Power Data']
            power_data['Day'] = pd.to_datetime(power_data['Timestamp']).dt.date
            power_data['Month'] = pd.to_datetime(power_data['Timestamp']).dt.month

            maxexport_capacity_ac = float(
                site_info['Component Info'].loc[site_info['Component Info']['Component'] == inverter][
                    'Capacity AC'].values) * 1.001

            daily_pr_df_inverter, irradiance_column = calculate_daily_raw_pr(power_data, days_under_analysis, inverter)

            try:
                df_to_add = daily_pr_df_inverter.drop(columns=irradiance_column)
                daily_pr_df = pd.concat([daily_pr_df, df_to_add], axis=1)

            except NameError:
                daily_pr_df = daily_pr_df_inverter

        return daily_pr_df

    elif pr_type == 'corrected' and granularity == 'daily':
        for inverter in inverter_list:
            # print(inverter)
            power_data = all_inverter_power_data_dict[inverter]['Power Data']
            power_data['Day'] = pd.to_datetime(power_data['Timestamp']).dt.date
            power_data['Month'] = pd.to_datetime(power_data['Timestamp']).dt.month

            maxexport_capacity_ac = float(
                site_info['Component Info'].loc[site_info['Component Info']['Component'] == inverter][
                    'Capacity AC'].values) * 1.001
            print(maxexport_capacity_ac)
            daily_corrected_pr_df_inverter, irradiance_column = calculate_daily_corrected_pr(power_data,
                                                                                             days_under_analysis,
                                                                                             inverter,
                                                                                             maxexport_capacity_ac)

            try:
                corrected_df_to_add = daily_corrected_pr_df_inverter.drop(columns=irradiance_column)
                corrected_daily_pr_df = pd.concat([corrected_daily_pr_df, corrected_df_to_add], axis=1)

            except NameError:
                corrected_daily_pr_df = daily_corrected_pr_df_inverter

        return corrected_daily_pr_df

    elif pr_type == 'corrected_DCfocus' and granularity == 'daily':
        for inverter in inverter_list:
            # print(inverter)
            power_data = all_inverter_power_data_dict[inverter]['Power Data']
            power_data['Day'] = pd.to_datetime(power_data['Timestamp']).dt.date
            power_data['Month'] = pd.to_datetime(power_data['Timestamp']).dt.month

            maxexport_capacity_ac = float(
                site_info['Component Info'].loc[site_info['Component Info']['Component'] == inverter][
                    'Capacity AC'].values) * 1.001

            dcfocus_corrected_df, irradiance_column = calculate_daily_corrected_pr_focusDC(power_data,
                                                                                           days_under_analysis,
                                                                                           inverter,
                                                                                           maxexport_capacity_ac)

            try:
                dcfocus_corrected_df_to_add = dcfocus_corrected_df.drop(columns=irradiance_column)
                dcfocus_corrected_daily_pr_df = pd.concat([dcfocus_corrected_daily_pr_df, dcfocus_corrected_df_to_add],
                                                          axis=1)

            except NameError:
                dcfocus_corrected_daily_pr_df = dcfocus_corrected_df
        return dcfocus_corrected_daily_pr_df
    elif pr_type == 'raw' and granularity == 'monthly':

        for inverter in inverter_list:
            # print(inverter)
            power_data = all_inverter_power_data_dict[inverter]['Power Data']
            power_data['Day'] = pd.to_datetime(power_data['Timestamp']).dt.date
            power_data['Month'] = pd.to_datetime(power_data['Timestamp']).apply(lambda x: x.strftime('%m-%Y'))

            raw_df_month, raw_powers_df_forsite_inv, irradiance_column = calculate_monthly_raw_pr(power_data,
                                                                                                  months_under_analysis,
                                                                                                  inverter)

            try:
                dcfocus_corrected_df_to_add = raw_df_month.drop(columns=irradiance_column)
                raw_daily_pr_df = pd.concat([raw_daily_pr_df, dcfocus_corrected_df_to_add],
                                            axis=1)

            except NameError:
                raw_daily_pr_df = raw_df_month

            try:
                raw_powers_df_forsite = pd.concat(
                    [raw_powers_df_forsite, raw_powers_df_forsite_inv], axis=1)

            except NameError:
                print('Creating dataframe with all inverters')
                raw_powers_df_forsite = raw_powers_df_forsite_inv

        # Add site wide results
        ac_power_results = raw_powers_df_forsite.loc[:, raw_powers_df_forsite.columns.str.contains('Inverter AC')]
        ac_power_results['Site'] = [ac_power_results.loc[i, :].sum() for i in ac_power_results.index]

        ideal_power_results = raw_powers_df_forsite.loc[:, raw_powers_df_forsite.columns.str.contains('Ideal')]
        ideal_power_results['Site'] = [ideal_power_results.loc[i, :].sum() for i in
                                       ideal_power_results.index]

        site_pr = ac_power_results['Site'] / ideal_power_results['Site']

        raw_daily_pr_df.insert(len(raw_daily_pr_df.columns) - 1, 'Site PR %',
                               site_pr)

        return raw_daily_pr_df

    elif pr_type == 'corrected' and granularity == 'monthly':
        for inverter in inverter_list:
            # print(inverter)
            power_data = all_inverter_power_data_dict[inverter]['Power Data']
            power_data['Day'] = pd.to_datetime(power_data['Timestamp']).dt.date
            power_data['Month'] = pd.to_datetime(power_data['Timestamp']).apply(lambda x: x.strftime('%m-%Y'))

            maxexport_capacity_ac = float(
                site_info['Component Info'].loc[site_info['Component Info']['Component'] == inverter][
                    'Capacity AC'].values) * 1.001

            corrected_df_month, corrected_powers_df_forsite_inv, irradiance_column = \
                calculate_monthly_corrected_pr_and_production(power_data, months_under_analysis,
                                                              inverter, maxexport_capacity_ac)

            try:
                corrected_df_to_add_month = corrected_df_month.drop(columns=irradiance_column)
                corrected_monthly_pr_df = pd.concat(
                    [corrected_monthly_pr_df, corrected_df_to_add_month], axis=1)

            except NameError:
                corrected_monthly_pr_df = corrected_df_month

            try:
                corrected_powers_df_forsite = pd.concat(
                    [corrected_powers_df_forsite, corrected_powers_df_forsite_inv], axis=1)

            except NameError:
                print('Creating dataframe with all inverters')
                corrected_powers_df_forsite = corrected_powers_df_forsite_inv

            # Add site wide results
        ac_power_results = corrected_powers_df_forsite.loc[
                           :, corrected_powers_df_forsite.columns.str.contains('Inverter AC')]

        ac_power_results['Site'] = [ac_power_results.loc[i, :].sum() for i in ac_power_results.index]

        ideal_power_results = corrected_powers_df_forsite.loc[
                              :, corrected_powers_df_forsite.columns.str.contains('Ideal')]

        ideal_power_results['Site'] = [ideal_power_results.loc[i, :].sum() for i in ideal_power_results.index]

        site_pr = ac_power_results['Site'] / ideal_power_results['Site']

        corrected_monthly_pr_df.insert(len(corrected_monthly_pr_df.columns) - 1, 'Site PR %',
                                       site_pr)

        return corrected_monthly_pr_df

    elif pr_type == 'corrected_DCfocus' and granularity == 'monthly':
        for inverter in inverter_list:
            # print(inverter)
            power_data = all_inverter_power_data_dict[inverter]['Power Data']
            power_data['Day'] = pd.to_datetime(power_data['Timestamp']).dt.date
            power_data['Month'] = pd.to_datetime(power_data['Timestamp']).apply(lambda x: x.strftime('%m-%Y'))

            maxexport_capacity_ac = float(
                site_info['Component Info'].loc[site_info['Component Info']['Component'] == inverter][
                    'Capacity AC'].values) * 1.001

            dcfocus_corrected_df_month, dcfocus_corrected_powers_df_forsite_inv, irradiance_column = \
                calculate_monthly_corrected_pr_and_production_focusDC(power_data, months_under_analysis,
                                                                      inverter, maxexport_capacity_ac)

            try:
                dcfocus_corrected_df_to_add_month = dcfocus_corrected_df_month.drop(columns=irradiance_column)
                dcfocus_corrected_monthly_pr_df = pd.concat(
                    [dcfocus_corrected_monthly_pr_df, dcfocus_corrected_df_to_add_month], axis=1)

            except NameError:
                dcfocus_corrected_monthly_pr_df = dcfocus_corrected_df_month

            try:
                dcfocus_corrected_powers_df_forsite = pd.concat(
                    [dcfocus_corrected_powers_df_forsite, dcfocus_corrected_powers_df_forsite_inv], axis=1)

            except NameError:
                dcfocus_corrected_powers_df_forsite = dcfocus_corrected_powers_df_forsite_inv

        # Add site wide results
        ac_power_results = dcfocus_corrected_powers_df_forsite.loc[:,
                           dcfocus_corrected_powers_df_forsite.columns.str.contains('Inverter AC')]
        ac_power_results['Site'] = [ac_power_results.loc[i, :].sum() for i in ac_power_results.index]

        ideal_power_results = dcfocus_corrected_powers_df_forsite.loc[:, dcfocus_corrected_powers_df_forsite.
                                                                             columns.str.contains('Ideal')]
        ideal_power_results['Site'] = [ideal_power_results.loc[i, :].sum() for i in
                                       ideal_power_results.index]

        site_pr = ac_power_results['Site'] / ideal_power_results['Site']

        dcfocus_corrected_monthly_pr_df.insert(len(dcfocus_corrected_monthly_pr_df.columns) - 1, 'Site PR %',
                                               site_pr)

        return dcfocus_corrected_monthly_pr_df, dcfocus_corrected_powers_df_forsite

    else:
        print('Combination of PR type and granularity not possible')
        sys.exit()

    return


# <editor-fold desc="Data analysis functions">

def calculate_daily_raw_pr(inverter_data, days_under_analysis, inverter):
    """From Inverter data (Power AC and Expected Power) calculates Raw PR
    Also uses irradiance to complete Dataframe"""

    ac_power_column = inverter_data.columns[inverter_data.columns.str.contains('AC')].values[0]
    expected_power_column = inverter_data.columns[inverter_data.columns.str.contains('Expected')].values[0]
    ideal_power_column = inverter_data.columns[inverter_data.columns.str.contains('Ideal')].values[0]
    irradiance_column = inverter_data.columns[inverter_data.columns.str.contains('Irradiance')].values[0]

    daily_pr_inverter = {}
    for day in days_under_analysis:
        data_day = inverter_data.loc[inverter_data['Day'] == day]
        actual_power_day = data_day[ac_power_column].sum() / 4
        expected_power_day = data_day[expected_power_column].sum() / 4
        ideal_power_day = data_day[ideal_power_column].sum() / 4

        irradiance_day = data_day[irradiance_column].sum() / 4

        pr_day = actual_power_day / ideal_power_day
        daily_pr_inverter[day] = (pr_day, irradiance_day)

        # print(day, ": ", actual_power_day, " / " , expected_power_day, " / ", pr_day, " / ")

    daily_pr_df = pd.DataFrame.from_dict(daily_pr_inverter, orient='index',
                                         columns=[str(inverter) + ' PR %', irradiance_column])
    # print(df)

    return daily_pr_df, irradiance_column


def calculate_daily_corrected_pr(inverter_data, days_under_analysis, inverter, maxexport_capacity_ac):
    '''From Inverter data (Power AC, Expected Power and Max export capacity ) calculates Corrected PR
    Corrected PR, in this case, is the correction for max export capacity.
        Also uses irradiance to complete Dataframe'''

    ac_power_column = inverter_data.columns[inverter_data.columns.str.contains('AC')].values[0]
    expected_power_column = inverter_data.columns[inverter_data.columns.str.contains('Expected')].values[0]
    ideal_power_column = inverter_data.columns[inverter_data.columns.str.contains('Ideal')].values[0]
    irradiance_column = inverter_data.columns[inverter_data.columns.str.contains('Irradiance')].values[0]

    corrected_power_data = inverter_data
    corrected_power_data[expected_power_column] = [maxexport_capacity_ac if power > maxexport_capacity_ac else power for
                                                   power in
                                                   corrected_power_data[expected_power_column]]
    corrected_power_data[ideal_power_column] = [maxexport_capacity_ac if power > maxexport_capacity_ac else power for
                                                power in
                                                corrected_power_data[ideal_power_column]]
    corrected_daily_pr_dict = {}

    for day in days_under_analysis:
        corrected_data_day = corrected_power_data.loc[corrected_power_data['Day'] == day]
        corrected_actual_power_day = corrected_data_day[ac_power_column].sum() / 4
        corrected_expected_power_day = corrected_data_day[expected_power_column].sum() / 4
        corrected_ideal_power_day = corrected_data_day[ideal_power_column].sum() / 4

        irradiance_day = corrected_data_day[irradiance_column].sum() / 4

        corrected_pr_day = corrected_actual_power_day / corrected_ideal_power_day
        corrected_daily_pr_dict[day] = (corrected_pr_day, irradiance_day)

    corrected_daily_pr_df = pd.DataFrame.from_dict(corrected_daily_pr_dict, orient='index',
                                                   columns=[str(inverter) + ' Corrected PR %',
                                                            irradiance_column])
    # print(corrected_daily_pr_df)

    return corrected_daily_pr_df, irradiance_column


def calculate_daily_corrected_pr_focusDC(inverter_data, days_under_analysis, inverter, maxexport_capacity_ac):
    """From Inverter data (Power AC, Expected Power and Max export capacity ) calculates Corrected PR
    Corrected PR, in this case, is the correction for inverter failures (focus on DC side) and with max export capacity in place
        Also uses irradiance to complete Dataframe"""

    ac_power_column = inverter_data.columns[inverter_data.columns.str.contains('AC')].values[0]
    expected_power_column = inverter_data.columns[inverter_data.columns.str.contains('Expected')].values[0]
    ideal_power_column = inverter_data.columns[inverter_data.columns.str.contains('Ideal')].values[0]
    irradiance_column = inverter_data.columns[inverter_data.columns.str.contains('Irradiance')].values[0]

    corrected_power_data = inverter_data.loc[inverter_data[ac_power_column] > 0]
    corrected_power_data[expected_power_column] = [maxexport_capacity_ac if power > maxexport_capacity_ac else power for
                                                   power in
                                                   corrected_power_data[expected_power_column]]
    corrected_power_data[ideal_power_column] = [maxexport_capacity_ac if power > maxexport_capacity_ac else power for
                                                power in
                                                corrected_power_data[ideal_power_column]]
    corrected_daily_pr_dict = {}

    for day in days_under_analysis:
        corrected_data_day = corrected_power_data.loc[corrected_power_data['Day'] == day]
        corrected_actual_power_day = corrected_data_day[ac_power_column].sum() / 4
        corrected_expected_power_day = corrected_data_day[expected_power_column].sum() / 4
        corrected_ideal_power_day = corrected_data_day[ideal_power_column].sum() / 4

        irradiance_day = corrected_data_day[irradiance_column].sum() / 4

        corrected_pr_day = corrected_actual_power_day / corrected_ideal_power_day
        corrected_daily_pr_dict[day] = (corrected_pr_day, irradiance_day)

    corrected_df = pd.DataFrame.from_dict(corrected_daily_pr_dict, orient='index',
                                          columns=[str(inverter) + ' - DC focus - Corrected PR %',
                                                   irradiance_column])
    # print(df)

    return corrected_df, irradiance_column


def calculate_monthly_raw_pr(inverter_data, months_under_analysis, inverter):
    """From Inverter data (Power AC, Expected Power and Max export capacity ) calculates Corrected PR
        Corrected PR, in this case, is the correction for inverter failures (focus on DC side) and with max export capacity in place
        Also uses irradiance to complete Dataframe"""

    ac_power_column = inverter_data.columns[inverter_data.columns.str.contains('AC')].values[0]
    expected_power_column = inverter_data.columns[inverter_data.columns.str.contains('Expected')].values[0]
    ideal_power_column = inverter_data.columns[inverter_data.columns.str.contains('Ideal')].values[0]
    irradiance_column = inverter_data.columns[inverter_data.columns.str.contains('Irradiance')].values[0]

    raw_monthly_pr_dict = {}
    raw_powers_dict_forsite = {}

    for month in months_under_analysis:
        raw_data_month = inverter_data.loc[inverter_data['Month'] == month]
        raw_actual_power_month = raw_data_month[ac_power_column].sum() / 4
        raw_expected_power_month = raw_data_month[expected_power_column].sum() / 4
        raw_ideal_power_month = raw_data_month[ideal_power_column].sum() / 4

        irradiance_month = raw_data_month[irradiance_column].sum() / 4

        raw_pr_month = raw_actual_power_month / raw_ideal_power_month
        raw_monthly_pr_dict[month] = (raw_pr_month, irradiance_month)
        raw_powers_dict_forsite[month] = (raw_actual_power_month, raw_expected_power_month, raw_ideal_power_month)

    raw_monthly_pr_df = pd.DataFrame.from_dict(raw_monthly_pr_dict, orient='index', columns=[
        str(inverter) + ' Raw Monthly PR %', irradiance_column])
    raw_monthly_production_df = pd.DataFrame.from_dict(raw_powers_dict_forsite,
                                                       orient='index',
                                                       columns=[ac_power_column, expected_power_column,
                                                                ideal_power_column])
    # print(df)

    return raw_monthly_pr_df, raw_monthly_production_df, irradiance_column


def calculate_monthly_corrected_pr_and_production_focusDC(inverter_data, months_under_analysis, inverter,
                                                          maxexport_capacity_ac):
    """From Inverter data (Power AC, Expected Power and Max export capacity ) calculates Corrected PR
        Corrected PR, in this case, is the correction for inverter failures (focus on DC side) and with max export capacity in place
        Also uses irradiance to complete Dataframe"""

    ac_power_column = inverter_data.columns[inverter_data.columns.str.contains('AC')].values[0]
    expected_power_column = inverter_data.columns[inverter_data.columns.str.contains('Expected')].values[0]
    ideal_power_column = inverter_data.columns[inverter_data.columns.str.contains('Ideal')].values[0]
    irradiance_column = inverter_data.columns[inverter_data.columns.str.contains('Irradiance')].values[0]

    corrected_power_data = inverter_data.loc[inverter_data[ac_power_column] > 0]
    corrected_power_data[expected_power_column] = [maxexport_capacity_ac if power > maxexport_capacity_ac else power for
                                                   power in
                                                   corrected_power_data[expected_power_column]]
    corrected_power_data[ideal_power_column] = [maxexport_capacity_ac if power > maxexport_capacity_ac else power for
                                                power in corrected_power_data[ideal_power_column]]

    corrected_monthly_pr_dict = {}
    corrected_powers_dict_forsite = {}

    for month in months_under_analysis:
        corrected_data_month = corrected_power_data.loc[corrected_power_data['Month'] == month]
        corrected_actual_power_month = corrected_data_month[ac_power_column].sum() / 4
        corrected_expected_power_month = corrected_data_month[expected_power_column].sum() / 4
        corrected_ideal_power_month = corrected_data_month[ideal_power_column].sum() / 4

        irradiance_month = corrected_data_month[irradiance_column].sum() / 4

        corrected_pr_month = corrected_actual_power_month / corrected_ideal_power_month
        corrected_monthly_pr_dict[month] = (corrected_pr_month, irradiance_month)
        corrected_powers_dict_forsite[month] = (
            corrected_actual_power_month, corrected_expected_power_month, corrected_ideal_power_month)

    corrected_monthly_pr_df = pd.DataFrame.from_dict(corrected_monthly_pr_dict, orient='index', columns=[
        str(inverter) + ' Corrected (w/clipping) Monthly PR %', irradiance_column])
    corrected_monthly_production_df = pd.DataFrame.from_dict(corrected_powers_dict_forsite, orient='index',
                                                             columns=[ac_power_column,
                                                                      expected_power_column, ideal_power_column])
    # print(df)

    return corrected_monthly_pr_df, corrected_monthly_production_df, irradiance_column


def calculate_monthly_corrected_pr_and_production(inverter_data, months_under_analysis, inverter, capacity_ac):
    """From Inverter data (Power AC, Expected Power and Max export capacity ) calculates Corrected PR
        Corrected PR, in this case, is the correction for inverter failures (focus on DC side) and with max export capacity in place
        Also uses irradiance to complete Dataframe"""

    ac_power_column = inverter_data.columns[inverter_data.columns.str.contains('AC')].values[0]
    expected_power_column = inverter_data.columns[inverter_data.columns.str.contains('Expected')].values[0]
    ideal_power_column = inverter_data.columns[inverter_data.columns.str.contains('Ideal')].values[0]
    irradiance_column = inverter_data.columns[inverter_data.columns.str.contains('Irradiance')].values[0]

    corrected_power_data = inverter_data
    corrected_power_data[expected_power_column] = [capacity_ac if power > capacity_ac else power for power in
                                                   corrected_power_data[expected_power_column]]
    corrected_power_data[ideal_power_column] = [capacity_ac if power > capacity_ac else power for power in
                                                corrected_power_data[ideal_power_column]]

    corrected_monthly_pr_dict = {}
    corrected_powers_dict_forsite = {}

    for month in months_under_analysis:
        corrected_data_month = corrected_power_data.loc[corrected_power_data['Month'] == month]
        corrected_actual_power_month = corrected_data_month[ac_power_column].sum() / 4
        corrected_expected_power_month = corrected_data_month[expected_power_column].sum() / 4
        corrected_ideal_power_month = corrected_data_month[ideal_power_column].sum() / 4

        irradiance_month = corrected_data_month[irradiance_column].sum() / 4

        corrected_pr_month = corrected_actual_power_month / corrected_ideal_power_month
        corrected_monthly_pr_dict[month] = (corrected_pr_month, irradiance_month)
        corrected_powers_dict_forsite[month] = (
            corrected_actual_power_month, corrected_expected_power_month, corrected_ideal_power_month)

    corrected_monthly_pr_df = pd.DataFrame.from_dict(corrected_monthly_pr_dict, orient='index', columns=[
        str(inverter) + ' Corrected (w/clipping) Monthly PR %', irradiance_column])
    corrected_monthly_production_df = pd.DataFrame.from_dict(corrected_powers_dict_forsite,
                                                             orient='index',
                                                             columns=[ac_power_column, expected_power_column,
                                                                      ideal_power_column])
    # print(df)

    return corrected_monthly_pr_df, corrected_monthly_production_df, irradiance_column


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
        date_range = pd.date_range(start_timestamp.replace(day=1), end_timestamp.replace(day=1),
                                   freq=pd.offsets.MonthBegin(1))
        expected_energy_in_period = {}
        expected_energy_info = {}
        for date in date_range:
            budget_energy_month = budget_export.loc[site, str(date.date())]
            budget_irradiance_month = budget_irradiance.loc[site, str(date.date())]
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
                    calendar.monthrange(date.year, month)[1], 23, 59, 59)) & (
                                                                         actual_irradiance_site.index >= pd.Timestamp(
                                                                         date.year, month, 1, 0, 0, 0))]

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


# </editor-fold>
