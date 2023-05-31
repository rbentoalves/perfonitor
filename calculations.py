import re
import sys
import pandas as pd
import numpy as np
import perfonitor.data_treatment
import calendar
from datetime import datetime
import timeit
import math
import perfonitor.inputs
import datetime as dt


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


# <editor-fold desc="ET Functions">

# <editor-fold desc="Active Hours and Energy Lost">

def activehours_energylost_incidents(df, df_all_irradiance,df_all_export, budget_pr,corrected_incidents_dict,
                                               active_events: bool = False, recalculate: bool = False,
                                               granularity: float=0.25):

    if active_events == True:
        for index, row in df.iterrows():
            site = row['Site Name']
            incident_id = row['ID']
            component = row['Related Component']
            capacity = row['Capacity Related Component']
            real_event_start_time = row['Event Start Time']
            event_start_time = row['Rounded Event Start Time']
            budget_pr_site = budget_pr.loc[site, :]

            if type(event_start_time) == str:
                row['Rounded Event Start Time'] = event_start_time = datetime.strptime(str(event_start_time), '%Y-%m-%d %H:%M:%S')

            if type(real_event_start_time) == str:
                row['Event Start Time'] = real_event_start_time = datetime.strptime(str(event_start_time), '%Y-%m-%d %H:%M:%S')

            # event_start_time = row['Event Start Time']
            if incident_id not in corrected_incidents_dict.keys():
                df_irradiance_site = df_all_irradiance.loc[:, df_all_irradiance.columns.str.contains(site + '|Timestamp')]
                df_irradiance_event = df_irradiance_site.loc[df_irradiance_site['Timestamp'] >= event_start_time]

                # Get percentages of first timestamp to account for rounding
                index_event_start_time = df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_start_time].index.values[0]
                percentage_of_timestamp_start = data_treatment.get_percentage_of_timestamp(real_event_start_time, event_start_time)


                actual_column, curated_column,data_gaps_proportion, poa_avg_column = \
                    data_treatment.get_actual_irradiance_column(df_irradiance_event)
                # print(actual_column)


                if actual_column == None:
                    print(component, ' on ', event_start_time, ': No irradiance available')
                    continue
                """elif 'curated' in actual_column:
                    print(component, ' on ', event_start_time,': Using curated irradiance')
                else:
                    print(component, ' on ', event_start_time,': Using poa average due to curated irradiance having over 25% of data gaps')
                    """


                data_gaps_percentage = "{:.2%}".format(data_gaps_proportion)
                print(incident_id, ' - Data Gaps percentage: ', data_gaps_percentage)

                #Correct irradiance in first timestamp to account for rounding
                df_irradiance_event.at[index_event_start_time, actual_column] = percentage_of_timestamp_start * df_irradiance_event.loc[index_event_start_time, actual_column]
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
                        enery_lost = 0

                else:
                    energy_lost = (sum(
                        [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                         index_el, row_el in
                         df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000


            else:
                data_gaps_percentage = "{:.2%}".format(corrected_incidents_dict[incident_id]['Data Gaps Proportion'])
                actual_column = corrected_incidents_dict[incident_id]['Irradiance Column']
                df_irradiance_event_raw = corrected_incidents_dict[incident_id][
                    'Irradiance Raw']
                df_irradiance_event = corrected_incidents_dict[incident_id][
                    'Corrected Irradiance Incident']
                df_cleaned_irradiance_event = corrected_incidents_dict[incident_id][
                    'Cleaned Corrected Irradiance Incident']

                print('Using Corrected Incident for: ', component, " on ", site, "with ", data_gaps_percentage, " of data gaps")

                df_irradiance_event_activeperiods = df_irradiance_event.loc[
                    df_irradiance_event[actual_column] > 20]
                df_cleaned_irradiance_event_activeperiods = df_cleaned_irradiance_event.loc[
                    df_cleaned_irradiance_event[actual_column] > 20]

                duration = df_irradiance_event_raw.shape[0] * granularity
                active_hours = df_irradiance_event_activeperiods.shape[0] * granularity
                energy_lost = (sum([row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                                    index_el, row_el in
                                    df_cleaned_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000
                """energy_lost = (sum([row_el[actual_column] * float(budget_pr_site[row_el['Timestamp'].month].values) for
                                    index_el, row_el in
                                    df_cleaned_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000
"""
            if active_hours < 0:
                active_hours = duration

            df.loc[index, 'Duration (h)'] = duration
            df.loc[index, 'Active Hours (h)'] = active_hours
            df.loc[index, 'Energy Lost (MWh)'] = energy_lost / 1000

    else:
        if recalculate == True:
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
                    df_irradiance_event = df_irradiance_site.loc[(df_irradiance_site['Timestamp'] >= event_start_time) & (
                            df_irradiance_site['Timestamp'] <= event_end_time)]
                    actual_column, curated_column, data_gaps_proportion, poa_avg_column = data_treatment.get_actual_irradiance_column(df_irradiance_event)

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
                    duration = ((real_event_end_time-real_event_start_time).days * 24) + ((real_event_end_time-real_event_start_time).seconds/3600)

                    active_hours = duration - ((
                            df_irradiance_event.shape[0] - df_irradiance_event_activeperiods.shape[0]) * granularity)
                    #print(real_event_end_time, " - ", real_event_start_time, " = ", duration)
                    #print(real_event_end_time, " - ", real_event_start_time, " = ", active_hours , " - Active Hours")

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

                    duration = ((real_event_end_time-real_event_start_time).days * 24) + ((real_event_end_time-real_event_start_time).seconds/3600)

                    active_hours = duration - (
                                df_irradiance_event.shape[0] - df_irradiance_event_activeperiods.shape[0]) * granularity

                    #print(real_event_end_time, " - ", real_event_start_time, " = ", duration)
                    #print(real_event_end_time, " - ", real_event_start_time, " = ", active_hours, " - Active Hours")

                    energy_lost = (sum(
                        [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                         index_el, row_el in
                         df_cleaned_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

                if active_hours < 0:
                    active_hours = duration

                df.loc[index, 'Duration (h)'] = duration
                df.loc[index, 'Active Hours (h)'] = active_hours
                df.loc[index, 'Energy Lost (MWh)'] = energy_lost / 1000
        else:
            df_to_update = df.loc[df['Energy Lost (MWh)'].isnull()]
            df_to_update = data_treatment.rounddatesclosed_15m("All", df_to_update)

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
                    df_irradiance_site = df_all_irradiance.loc[:,
                                         df_all_irradiance.columns.str.contains(site + '|Timestamp')]

                    df_irradiance_event = df_irradiance_site.loc[
                        (df_irradiance_site['Timestamp'] >= event_start_time) & (
                            df_irradiance_site['Timestamp'] <= event_end_time)]

                    #Get percentages of first timestamp to account for rounding
                    index_event_start_time = \
                    df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_start_time].index.values[0]

                    index_event_end_time = \
                        df_irradiance_site.loc[df_irradiance_site['Timestamp'] == event_end_time].index.values[0]

                    percentage_of_timestamp_start = data_treatment.get_percentage_of_timestamp(real_event_start_time,
                                                                                   event_start_time)
                    percentage_of_timestamp_end = data_treatment.get_percentage_of_timestamp(real_event_end_time,
                                                                                   event_end_time)


                    #GEt actual column to work with
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

                    #Communicate data gaps percentage
                    data_gaps_percentage = "{:.2%}".format(data_gaps_proportion)
                    print(incident_id, ' - Data Gaps percentage: ', data_gaps_percentage)

                    # Correct irradiance in first timestamp to account for rounding
                    df_irradiance_event.at[index_event_start_time, actual_column] = percentage_of_timestamp_start * \
                                                                                    df_irradiance_event.loc[
                                                                                        index_event_start_time, actual_column]

                    df_irradiance_event.at[index_event_end_time, actual_column] = percentage_of_timestamp_end * \
                                                                                    df_irradiance_event.loc[
                                                                                        index_event_end_time, actual_column]

                    #Get irradiance periods over 20W/m2
                    df_irradiance_event_activeperiods = df_irradiance_event.loc[df_irradiance_event[actual_column] > 20]

                    duration = ((real_event_end_time-real_event_start_time).days * 24) + ((real_event_end_time-real_event_start_time).seconds/3600)
                    active_hours = duration - (df_irradiance_event.shape[0]-df_irradiance_event_activeperiods.shape[0]) * granularity


                    if site == component:
                        df_export_site = df_all_export.loc[:,
                                             df_all_export.columns.str.contains(site + '|Timestamp')]
                        export_column = df_all_export.columns[df_all_export.columns.str.contains(site)].values[0]

                        df_export_event = df_export_site.loc[
                            (df_export_site['Timestamp'] >= event_start_time) & (
                                df_export_site['Timestamp'] <= event_end_time)]

                        df_export_event.at[index_event_start_time, export_column] = percentage_of_timestamp_start * \
                                                                                        df_export_event.loc[
                                                                                            index_event_start_time, export_column]

                        df_export_event.at[index_event_end_time, export_column] = percentage_of_timestamp_end * \
                                                                                      df_export_event.loc[
                                                                                          index_event_end_time, export_column]

                        energy_produced = df_export_event[export_column].sum()

                        energy_lost = (sum(
                            [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                             index_el, row_el in
                             df_irradiance_event_activeperiods.iterrows()]) * capacity * granularity)/ 1000

                        print("Site: ", site, "\nEnergy produced: ", energy_produced, "\nEnergy Expected: ", energy_lost)

                        energy_lost = (energy_lost - energy_produced)

                        print("Real Energy Lost: ", energy_lost)

                        if energy_lost < 0:
                            enery_lost = 0

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

                    duration = ((real_event_end_time-real_event_start_time).days * 24) \
                               + ((real_event_end_time-real_event_start_time).seconds/3600)

                    active_hours = duration - (df_irradiance_event.shape[0]-df_irradiance_event_activeperiods.shape[0]) \
                                   * granularity

                    energy_lost = (sum(
                        [row_el[actual_column] * budget_pr_site.loc[str(row_el['Timestamp'].date())[:-2] + "01"] for
                         index_el, row_el in
                         df_cleaned_irradiance_event_activeperiods.iterrows()]) * capacity * granularity) / 1000

                if active_hours < 0:
                    active_hours = duration

                df.loc[index, 'Duration (h)'] = duration
                df.loc[index, 'Active Hours (h)'] = active_hours
                df.loc[index, 'Energy Lost (MWh)'] = energy_lost / 1000



    return df



def active_hours_and_energy_lost_all_dfs(final_df_to_add,corrected_incidents_dict,df_all_irradiance, df_all_export,budget_pr,
                                           irradiance_threshold: int = 20, timestamp: int = 15, recalculate_value: bool=False):
    granularity = timestamp / 60
    tic = timeit.default_timer()
    for key, df in final_df_to_add.items():

        if "Active" in key:
            if "tracker" in key:
                continue
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
                continue
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

def calculate_availability_period(site, incidents,component_data, budget_pr, df_all_irradiance,df_all_export,
                                  irradiance_threshold, date_start_str, date_end_str,granularity: float = 0.25):
    print(site)
    period = date_start_str + " to " + date_end_str
    active_events = False
    recalculate_value = True
    irradiance_incidents_corrected = {}

    # Get site info --------------------------------------------------------------------------------------------
    site_info = component_data.loc[component_data['Site'] == site]
    site_capacity = float(component_data.loc[component_data['Component'] == site]['Nominal Power DC'].values)
    budget_pr_site = budget_pr.loc[site, :]

    # Get site Incidents --------------------------------------------------------------------------------------------
    site_incidents = incidents.loc[incidents['Site Name'] == site]

    # Get site irradiance & export --------------------------------------------------------------------------------------------
    df_irradiance_site = df_all_irradiance.loc[:, df_all_irradiance.columns.str.contains(site + '|Timestamp')]
    df_export_site = df_all_export.loc[:,df_all_export.columns.str.contains(site + '|Timestamp')]

    # Get irradiance poa avg column and curated -----------------------------------------------------------------------
    actual_column, curated_column, data_gaps_proportion, poa_avg_column = \
        data_treatment.get_actual_irradiance_column(df_irradiance_site)

    # Get first timestamp under analysis and df from that timestamp onwards -------------------------------------------
    stime_index = next(i for i, v in enumerate(df_irradiance_site[poa_avg_column]) if v > irradiance_threshold)
    site_start_time = df_irradiance_site['Timestamp'][stime_index]

    df_irradiance_operation_site = df_irradiance_site.loc[df_irradiance_site['Timestamp'] >= site_start_time]
    df_export_operation_site = df_export_site.loc[df_export_site['Timestamp'] >= site_start_time]

    df_irradiance_operation_site['Day'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').date() for timestamp
                                           in
                                           df_irradiance_operation_site['Timestamp']]
    df_export_operation_site['Day'] = [datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').date() for timestamp
                                           in
                                           df_export_operation_site['Timestamp']]


    # Defined timeframe----------------------------------------------------
    if not date_start_str == 'None' and not date_end_str == 'None':

        # Get start time analysis
        date_start_avail_analysis = datetime.strptime(date_start_str, '%Y-%m-%d').date()
        timestamp_start_avail_analysis = datetime.strptime(date_start_str + " 00:00:00", '%Y-%m-%d %H:%M:%S')

        # Get end time analysis
        date_end_avail_analysis = datetime.strptime(date_end_str, '%Y-%m-%d').date()
        date_end_str_event = str(datetime.strptime(date_end_str, '%Y-%m-%d').date() + dt.timedelta(days=1))
        timestamp_end_avail_analysis = datetime.strptime(date_end_str_event + " 00:00:00", '%Y-%m-%d %H:%M:%S')

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


    # Get incidents in that period --------------------------------------------------------------------------------
    print(site_incidents[['ID', 'Event Start Time', 'Event End Time']])

    relevant_incidents = site_incidents.loc[~(site_incidents['Event Start Time'] > timestamp_end_avail_analysis) & ~(
        site_incidents['Event End Time'] < timestamp_start_avail_analysis)]
    #test
    """## print(relevant_incidents)
    ## print(relevant_incidents.loc[(relevant_incidents['Site Name'] == "LSBP - Bighorn") & (relevant_incidents['Related Component'] == "Inverter 65")])
    ## print(relevant_incidents[['Related Component', 'Event Start Time','Event End Time', "Duration (h)","Active Hours (h)", 'Energy Lost (MWh)' ]])"""


    # Get irradiance of period under analysis --------------------------------------------------------------------------
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


    # Correct Timestamps of incidents to timeframe of analysis ---------------------------------------------------------
    for index, row in relevant_incidents.iterrows():
        try:
            if math.isnan(row['Event End Time']):
                relevant_incidents.loc[index, 'Event End Time'] = timestamp_end_avail_analysis


            if row['Event Start Time'] < timestamp_start_avail_analysis:
                relevant_incidents.loc[index, 'Event Start Time'] = timestamp_start_avail_analysis

        except TypeError:
            if row['Event End Time'] > timestamp_end_avail_analysis:
                relevant_incidents.loc[index, 'Event End Time'] = timestamp_end_avail_analysis

            if row['Event Start Time'] < timestamp_start_avail_analysis:
                ## print('LOOK HERE')
                ## print(row[['Related Component', 'Event Start Time','Event End Time']])
                relevant_incidents.loc[index, 'Event Start Time'] = timestamp_start_avail_analysis

    ## print(relevant_incidents.loc[(relevant_incidents['Site Name'] == "LSBP - Bighorn") & (relevant_incidents['Related Component'] == "Inverter 65")])

    # Get incidents to keep unaltered
    incidents_unaltered = relevant_incidents.loc[~(relevant_incidents['Event Start Time'] == timestamp_start_avail_analysis) & ~(
        relevant_incidents['Event End Time'] == timestamp_end_avail_analysis)]

    # Get corrected incidents dict (overlappers) and then calculate real active hours and losses with that info --------
    corrected_incidents_dict_period = data_treatment.correct_incidents_irradiance_for_overlapping_parents(relevant_incidents,
                                                                                              irradiance_analysis,
                                                                                              export_analysis,
                                                                                              component_data,
                                                                                              recalculate_value)

    corrected_relevant_incidents = activehours_energylost_incidents(relevant_incidents,irradiance_analysis,
                                                                                  export_analysis, budget_pr,
                                                                                  corrected_incidents_dict_period,
                                                                                  active_events, recalculate_value,
                                                                                  granularity)

    # Get corrected relevant incidents to concat with unaltered ones
    corrected_relevant_incidents = corrected_relevant_incidents.loc[
        (corrected_relevant_incidents['Event Start Time'] == timestamp_start_avail_analysis) | (
            corrected_relevant_incidents['Event End Time'] == timestamp_end_avail_analysis)]


    """# Get corrected incidents dict (overlappers) and then calculate real active hours and losses with that info --------
    corrected_incidents_dict_period = mf.correct_incidents_irradiance_for_overlapping_parents(relevant_incidents,
                                                                                              irradiance_analysis,
                                                                                              component_data,
                                                                                              recalculate_value)
    corrected_relevant_incidents = msp.calculate_activehours_energylost_incidents(relevant_incidents,
                                                                                  irradiance_analysis, export_analysis,
                                                                                  budget_pr,
                                                                                  corrected_incidents_dict_period,
                                                                                  active_events, recalculate_value,
                                                                                  granularity)"""

    ## print(corrected_relevant_incidents[['Related Component', 'Event Start Time','Event End Time', "Duration (h)","Active Hours (h)", 'Energy Lost (MWh)' ]])

    # Join corrected incidents and non-corrected incidents

    corrected_relevant_incidents = pd.concat([incidents_unaltered,corrected_relevant_incidents])


    # corrected_relevant_incidents = final_relevant_incidents
    # Calculate period availability-------------------------------------------------------------------------------------
    weighted_downtime = {}
    raw_weighted_downtime = {}
    corrected_relevant_incidents['Weighted Downtime'] = ""

    for index, row in corrected_relevant_incidents.iterrows():
        capacity = row['Capacity Related Component']
        active_hours = row['Active Hours (h)']
        failure_mode = row['Failure Mode']
        if not failure_mode == "Curtailment":
            try:
                if capacity == float(0):
                    weighted_downtime_incident = raw_weighted_downtime_incident = 0
                elif math.isnan(active_hours):
                    weighted_downtime_incident = raw_weighted_downtime_incident = 0
                elif type(active_hours) == str:
                    weighted_downtime_incident = raw_weighted_downtime_incident = 0
                else:
                    weighted_downtime_incident = raw_weighted_downtime_incident = (capacity * active_hours) / site_capacity
            except TypeError:
                weighted_downtime_incident = raw_weighted_downtime_incident = 0

        else:
            weighted_downtime_incident = 0
            raw_weighted_downtime_incident = active_hours

        weighted_downtime[row['ID']] = weighted_downtime_incident
        raw_weighted_downtime[row['ID']] = raw_weighted_downtime_incident

        corrected_relevant_incidents.loc[index, 'Weighted Downtime'] = weighted_downtime_incident
        corrected_relevant_incidents.loc[index, 'Raw Weighted Downtime'] = raw_weighted_downtime_incident

    weighted_downtime_df = pd.DataFrame.from_dict(weighted_downtime, orient='index',
                                                  columns=['Incident weighted downtime (h)'])
    raw_weighted_downtime_df = pd.DataFrame.from_dict(raw_weighted_downtime, orient='index',
                                                  columns=['Incident weighted downtime (h)'])
    #print(weighted_downtime_df)

    total_weighted_downtime = weighted_downtime_df['Incident weighted downtime (h)'].sum()
    total_raw_weighted_downtime = raw_weighted_downtime_df['Incident weighted downtime (h)'].sum()
    try:
        availability_period = ((site_active_hours_period - total_weighted_downtime) / site_active_hours_period)
        raw_availability_period = ((site_active_hours_period - total_raw_weighted_downtime) / site_active_hours_period)

    except ZeroDivisionError:
        availability_period = 0
        raw_availability_period = 0




    return availability_period,raw_availability_period, site_active_hours_period, corrected_relevant_incidents


def availability_in_period(incidents, period, component_data, df_all_irradiance, df_all_export,budget_pr,
                                     irradiance_threshold: int = 20,timestamp: int = 15):
    granularity = timestamp / 60

    #Get dates from period info
    date_start_str, date_end_str = inputs.choose_period_of_analysis(period)
    date_range = date_start_str + " to " + date_end_str
    print(date_range)


    #Get site list --------- could be input
    site_list = list(set([re.search(r'\[.+\]', site).group().replace('[', "").replace(']', "") for site in
                          df_all_irradiance.loc[:, df_all_irradiance.columns.str.contains('Irradiance')].columns]))
    site_list = [data_treatment.correct_site_name(site) for site in site_list]


    #Get site and fleet capacities --------- could be input
    site_capacities = component_data.loc[component_data['Component Type'] == 'Site'][
        ['Component', 'Nominal Power DC']].set_index('Component').loc[site_list,:]
    fleet_capacity = site_capacities['Nominal Power DC'].sum()

    #Get only incidents that count for availaility, aka, "Not producing"
    incidents = incidents.loc[incidents['Component Status'] == "Not Producing"].reset_index(None, drop=True)

    # Calculate Availability, Active Hours and Corrected Dataframe
    availability_period_per_site = {}
    raw_availability_period_per_site = {}
    active_hours_per_site = {}
    incidents_corrected_period_per_site = {}

    for site in site_list:
        availability_period,raw_availability_period, site_active_hours_period, corrected_relevant_incidents = calculate_availability_period(
            site, incidents, component_data, budget_pr, df_all_irradiance, df_all_export, irradiance_threshold,
            date_start_str, date_end_str, granularity)

        availability_period_per_site[site] = availability_period
        raw_availability_period_per_site[site] = raw_availability_period
        active_hours_per_site[site] = site_active_hours_period
        incidents_corrected_period_per_site[site] = corrected_relevant_incidents

    #Add fleet value and company goals values
    availability_period_per_site['Fleet'] = sum(
            [availability_period_per_site[site] * site_capacities.loc[site, 'Nominal Power DC'] for site in
             site_list]) / fleet_capacity

    raw_availability_period_per_site['Fleet'] = sum(
        [raw_availability_period_per_site[site] * site_capacities.loc[site, 'Nominal Power DC'] for site in
         site_list]) / fleet_capacity

    availability_period_per_site['Company goal'] = 0.944
    availability_period_per_site['Company max goal'] = 0.964



    availability_period_df = pd.DataFrame.from_dict(availability_period_per_site, orient='index', columns=[
        date_range])  # , orient='index', columns=['Incident weighted downtime (h)'])
    raw_availability_period_df = pd.DataFrame.from_dict(raw_availability_period_per_site, orient='index', columns=[
        date_range])  # , orient='index', columns=['Incident weighted downtime (h)'])

    activehours_period_df = pd.DataFrame.from_dict(active_hours_per_site, orient='index', columns=[date_range])
    incidents_corrected_period = pd.concat(list(incidents_corrected_period_per_site.values()))






    return availability_period_df,raw_availability_period_df,activehours_period_df,incidents_corrected_period,date_range






# </editor-fold>


# <editor-fold desc="PR Calculation">

def pr_in_period(incidents_period,availability_period,raw_availability_period, period, component_data,
                                       df_all_irradiance, df_all_export,budget_pr,budget_export,budget_irradiance,
                                       irradiance_threshold: int = 20,timestamp: int = 15):

    # Get site list --------- could be input
    site_list = list(set([re.search(r'\[.+\]', site).group().replace('[', "").replace(']', "") for site in
                          df_all_irradiance.loc[:, df_all_irradiance.columns.str.contains('Irradiance')].columns]))
    site_list = [data_treatment.correct_site_name(site) for site in site_list]

    # Get site and fleet capacities --------- could be input
    site_capacities = component_data.loc[component_data['Component Type'] == 'Site'][
                          ['Component', 'Nominal Power DC']].set_index('Component').loc[site_list, :]
    fleet_capacity = site_capacities['Nominal Power DC'].sum()


    #Get dates from period info
    date_start_str, date_end_str = inputs.choose_period_of_analysis(period)

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
            exported_energy = float(export_data.sum())

            # Get relevant irradiance data and calculate expected energy
            irradiance_site = df_irradiance_period.loc[:, df_irradiance_period.columns.str.contains(site)]
            actual_column, curated_column, data_gaps_proportion, poa_avg_column = data_treatment.get_actual_irradiance_column(
                irradiance_site)

            if not actual_column == None:
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
            """try:
                energy_lost = incidents_period.loc[incidents_period['Site Name'] == site][
                                  'Energy Lost (MWh)'].replace("", 0).sum() * 1000

                print(incidents_period.loc[incidents_period['Site Name'] == site])
                print(energy_lost)
                print(type(energy_lost))
            except TypeError:
                print("TypeError, replacing nan values")
                energy_lost = incidents_period.loc[incidents_period['Site Name'] == site]['Energy Lost (MWh)'].replace(
                    "", 0).sum() * 1000"""

            # Calculate PRs
            actual_pr = exported_energy / ((actual_irradiance_site.sum() / 4000) * site_capacity)
            possible_pr = (exported_energy + energy_lost) / ((actual_irradiance_site.sum() / 4000) * site_capacity)

            # Calculate Variances
            actual_expected_variance = (exported_energy / expected_energy) - 1
            corrected_actual_expected_variance = ((exported_energy + energy_lost) / expected_energy) - 1

            # Get availability in Period
            availability = availability_period.loc[site, :].values[0]
            raw_availability = raw_availability_period.loc[site, :].values[0]

            # print(site, "\n Expected Energy: ", expected_energy,"\n Exported Energy: " ,exported_energy, "\n Energy Lost: ", energy_lost)
            # incidents_period_site = incidents_period.loc[(incidents_period['Component Status'] == "Not Producing") & (incidents['Site Name'] == site) & (incidents['Event Start Time'] > )].reset_index(None, drop=True)

            # Store relevant data

            data_period_per_site[site] = ("{:.2%}".format(availability),
                                          "{:.2%}".format(raw_availability),
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
                                                                           'Actual PR (%)',
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