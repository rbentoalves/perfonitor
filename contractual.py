import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
from datetime import datetime
import datetime as dt
from openpyxl import Workbook
import openpyxl
import re
import os
import PySimpleGUI as sg
import xlwings as xlw
import xlsxwriter as xlw
import statistics
import sys
import IPython
import math
import calendar


def calculate_top40_energylost(start_time, end_time, r_start_time, r_end_time, power_inverters_incident,
                               granularity_factor):
    if start_time == end_time or start_time > end_time:

        energy_lost_t40 = 0

    elif r_start_time == r_end_time:
        duration_hours = ((end_time - start_time).seconds) / (3600)
        power_period = power_inverters_incident.values.mean()

        energy_lost_t40 = (duration_hours * power_period) / 1000


    else:
        granularity = dt.timedelta(minutes=60 * granularity_factor)

        first_timestamp_percentage = (r_start_time - start_time).seconds / 3600
        last_timestamp_percentage = (granularity - (r_end_time - end_time)).seconds / 3600

        energy_first_timestamp = power_inverters_incident.iloc[0] * first_timestamp_percentage
        energy_last_timestamp = power_inverters_incident.iloc[-1] * last_timestamp_percentage

        energy_lost_first_last = (energy_first_timestamp + energy_last_timestamp) / 1000

        if len(power_inverters_incident) == 2:
            energy_lost_contractual = energy_lost_t40 = energy_lost_first_last


        else:

            energy_lost_t40 = energy_lost_first_last + (
                    (power_inverters_incident[1:-1].values.sum() * granularity_factor) / 1000)

    return energy_lost_t40


def get_incidents_df_for_exclusions(incidents_period, site):
    np_site_incidents = incidents_period.loc[
        (incidents_period["Component Status"] == "Not Producing") & (incidents_period["Site Name"] == site)]
    # print(np_site_incidents.shape)

    energy_lost_index = np_site_incidents.columns.get_loc("Energy Lost (MWh)")

    np_site_incidents.insert(energy_lost_index + 1, "Energy Lost T40 (MWh)",
                             [0] * len(np_site_incidents["Energy Lost (MWh)"]))
    np_site_incidents.insert(energy_lost_index + 2, "Energy Lost Contractual (MWh)",
                             [0] * len(np_site_incidents["Energy Lost (MWh)"]))

    active_hours_index = np_site_incidents.columns.get_loc("Active Hours (h)")
    np_site_incidents.insert(active_hours_index + 1, "Active Hours Contractual (h)",
                             [0] * len(np_site_incidents["Active Hours (h)"]))

    np_site_incidents["Rounded Event Start Time"] = pd.to_datetime(np_site_incidents["Event Start Time"],
                                                                   format='%Y-%m-%d %H:%M:%S').dt.ceil("15min")
    np_site_incidents["Rounded Event End Time"] = pd.to_datetime(np_site_incidents["Event End Time"],
                                                                 format='%Y-%m-%d %H:%M:%S').dt.ceil("15min")

    np_site_incidents_par_exc = np_site_incidents.loc[(np_site_incidents["Excludable"] == "Yes")
                                                      & ~(
            np_site_incidents["Excludable Category"] == "Sub-Inverter Level")
                                                      & ~(np_site_incidents["Excludable Category"] == "Curtailment")]

    np_site_incidents_non_exc = np_site_incidents.loc[~(np_site_incidents["Excludable"] == "Yes")]

    np_site_incidents_sub_inv = np_site_incidents.loc[
        (np_site_incidents["Excludable Category"] == "Sub-Inverter Level")]
    np_site_incidents_curt = np_site_incidents.loc[(np_site_incidents["Excludable Category"] == "Curtailment")]

    print(np_site_incidents_par_exc.shape)
    print(np_site_incidents_non_exc.shape)
    print(np_site_incidents_sub_inv.shape)
    print(np_site_incidents_curt.shape)

    print(np_site_incidents.shape)

    # print(np_site_incidents.shape)

    rounded_start_index = np_site_incidents.columns.get_loc("Rounded Event Start Time")
    np_site_incidents_par_exc.insert(rounded_start_index + 1, "Rounded Exclusion Start Time",
                                     pd.to_datetime(np_site_incidents_par_exc["Exclusion Start Time"],
                                                    format='%Y-%m-%d %H:%M:%S').dt.ceil("15min"))
    np_site_incidents_par_exc.insert(rounded_start_index + 2, "Rounded Exclusion End Time",
                                     pd.to_datetime(np_site_incidents_par_exc["Exclusion End Time"],
                                                    format='%Y-%m-%d %H:%M:%S').dt.ceil("15min"))

    np_site_incidents_sub_inv.insert(rounded_start_index + 1, "Rounded Exclusion Start Time",
                                     pd.to_datetime(np_site_incidents_sub_inv["Exclusion Start Time"],
                                                    format='%Y-%m-%d %H:%M:%S').dt.ceil("15min"))
    np_site_incidents_sub_inv.insert(rounded_start_index + 2, "Rounded Exclusion End Time",
                                     pd.to_datetime(np_site_incidents_sub_inv["Exclusion End Time"],
                                                    format='%Y-%m-%d %H:%M:%S').dt.ceil("15min"))

    # np_site_incidents_par_exc["Rounded Event O&M Response Time"] = pd.to_datetime(np_site_incidents_par_exc["Event O&M Response Time"], format='%Y-%m-%d %H:%M:%S').dt.ceil("15min")

    return np_site_incidents, np_site_incidents_non_exc, np_site_incidents_par_exc, np_site_incidents_sub_inv, np_site_incidents_curt


def theoretical_power_site(df_irradiance_site, all_site_info, site, curated_column):
    nompower_site = all_site_info.loc[site, "Nominal Power DC"]
    mec_site = all_site_info.loc[site, "Maximum Export Capacity"]

    df_irradiance_site["Site Theoretical Production (kW)"] = [(df_irradiance_site.loc[timestamp, curated_column] *
                                                               budget_pr.loc[
                                                                   site, timestamp.replace(day=1, hour=0, minute=0,
                                                                                           second=0)] * nompower_site) / 1000
                                                              for timestamp in df_irradiance_site.index]
    df_irradiance_site["Site Theoretical Production (kW)"] = [value if value <= mec_site else mec_site for value in
                                                              df_irradiance_site["Site Theoretical Production (kW)"]]

    return df_irradiance_site


def top40_power_inverter(df_inverter_power, all_site_info, site):
    df_inverter_power["Top 40% Power"] = pd.NA

    for timestamp in df_inverter_power.index:
        array_prod = sorted(df_inverter_power.loc[timestamp].dropna().values,
                            reverse=True)  # get all values and remove nan
        array_prod = [value if value > 0 else 0 for value in array_prod]  # remove zeros
        n_components_40 = math.floor(0.4 * len(array_prod))  # check number of comp that == 40% of components

        if len(array_prod) == 0:  # if no components then value is 0
            mean_prod_40 = 0

        elif n_components_40 == 0:
            mean_prod_40 = array_prod[0]

        else:
            array_prod_40 = array_prod[:n_components_40]
            mean_prod_40 = statistics.mean(array_prod_40)

        df_inverter_power.loc[timestamp, "Top 40% Power"] = mean_prod_40
        top40_power = df_inverter_power[["Top 40% Power"]]

    return top40_power


def correct_overlapping_parents(component, power_inverters_incident_excludable, power_inverters_incident,
                                relevant_parents_incidents):
    """try:
        del timestamps_to_remove
        del timestamps_to_remove_om
    except NameError:
        pass"""

    for p_index, p_row in relevant_parents_incidents.iterrows():
        # print(p_row[["Related Component", "Rounded Event Start Time", "Event O&M Response Time", "Rounded Event End Time"]])

        timestamps_to_remove_incident = list(
            pd.date_range(p_row["Rounded Event Start Time"], p_row["Rounded Event End Time"], freq="15T"))

        # print(timestamps_to_remove_incident)
        try:
            timestamps_to_remove.extend(timestamps_to_remove_incident)
            # print(timestamps_to_remove)
        except NameError:
            timestamps_to_remove = timestamps_to_remove_incident

    timestamps_to_remove_exc = [timestamp for timestamp in sorted(list(set(timestamps_to_remove))) if
                                timestamp in power_inverters_incident_excludable.index][
                               :-1]  # removes last timestamp as the parent is already producing
    timestamps_to_remove = [timestamp for timestamp in sorted(list(set(timestamps_to_remove))) if
                            timestamp in power_inverters_incident.index][
                           :-1]  # removes last timestamp as the parent is already producing

    power_inverters_incident_excludable.drop(timestamps_to_remove_exc, inplace=True)
    power_inverters_incident.drop(timestamps_to_remove, inplace=True)

    return power_inverters_incident_excludable, power_inverters_incident


def energy_lost_nonexcludable(production_irr_df, np_site_incidents_non_exc, component_data_site, granularity_factor,
                              np_site_incidents):
    for index, row in np_site_incidents_non_exc.iterrows():

        component = row["Related Component"]
        print(row["ID"])
        component_type = \
        component_data_site.loc[component_data_site["Component"] == component]["Component Type"].values[0]
        nompower_component = \
        component_data_site.loc[component_data_site["Component"] == component]["Nominal Power DC"].values[0]

        start_time = row["Event Start Time"]
        end_time = row["Event End Time"]

        r_start_time = row["Rounded Event Start Time"]
        r_end_time = row["Rounded Event End Time"]

        if component_type == "Site":

            site_prod_incident = production_irr_df.loc[r_start_time:r_end_time, "Site Theoretical Production (kW)"]

            energy_lost_t40 = energy_lost_contractual = calculate_top40_energylost(start_time, end_time, r_start_time,
                                                                                   r_end_time, site_prod_incident,
                                                                                   granularity_factor)


        else:
            # Correct overlapping parents
            parents = (component_data_site.loc[component_data_site['Component'] == row['Related Component']]).loc[:,
                      component_data_site.columns.str.contains('Parent')].values.flatten().tolist()
            parents = [x for x in parents if str(x) != 'nan']

            parents_incidents = np_site_incidents[np_site_incidents['Related Component'].isin(parents)]

            relevant_parents_incidents = parents_incidents.loc[~(parents_incidents['Event End Time'] <= start_time) & ~(
                    parents_incidents['Event Start Time'] >= end_time)]

            if not relevant_parents_incidents.empty:
                try:
                    del timestamps_to_remove
                except NameError:
                    pass

                for p_index, p_row in relevant_parents_incidents.iterrows():

                    timestamps_to_remove_incident = list(
                        pd.date_range(p_row["Rounded Event Start Time"], p_row["Rounded Event End Time"], freq="15T"))
                    # print(timestamps_to_remove_incident)
                    try:
                        timestamps_to_remove.extend(timestamps_to_remove_incident)
                    except NameError:
                        timestamps_to_remove = timestamps_to_remove_incident

                power_inverters_incident = production_irr_df.loc[r_start_time:r_end_time,
                                           "Top 40% Power"] * nompower_component
                timestamps_to_remove = [timestamp for timestamp in sorted(list(set(timestamps_to_remove))) if
                                        timestamp in power_inverters_incident.index][
                                       :-1]  # removes last timestamp as the parent is already producing

                power_inverters_incident.drop(timestamps_to_remove, inplace=True)
                # print(power_inverters_incident.index)

            else:
                power_inverters_incident = production_irr_df.loc[r_start_time:r_end_time,
                                           "Top 40% Power"] * nompower_component

            # power_inverters_incident = production_irr_df.loc[r_start_time:r_end_time,"Top 40% Power"]
            energy_lost_t40 = energy_lost_contractual = calculate_top40_energylost(start_time, end_time, r_start_time,
                                                                                   r_end_time, power_inverters_incident,
                                                                                   granularity_factor)

        np_site_incidents_non_exc.loc[index, "Energy Lost T40 (MWh)"] = energy_lost_t40
        np_site_incidents_non_exc.loc[index, "Energy Lost Contractual (MWh)"] = energy_lost_contractual

    return np_site_incidents_non_exc


def energy_lost_excludable(production_irr_df, np_site_incidents_par_exc, component_data_site, granularity_factor,
                           np_site_incidents):
    for index, row in np_site_incidents_par_exc.iterrows():

        print(row["ID"])

        component = row["Related Component"]

        print(component)

        print(component_data_site.loc[component_data_site["Component"] == component]["Component Type"])

        component_type = \
        component_data_site.loc[component_data_site["Component"] == component]["Component Type"].values[0]
        nompower_component = \
        component_data_site.loc[component_data_site["Component"] == component]["Nominal Power DC"].values[0]

        start_time = row["Event Start Time"]
        end_time = row["Event End Time"]
        exc_start_time = row["Exclusion Start Time"]
        exc_end_time = row["Exclusion End Time"]

        r_start_time = row["Rounded Event Start Time"]
        r_end_time = row["Rounded Event End Time"]
        r_exc_start_time = row["Rounded Exclusion Start Time"]
        r_exc_end_time = row["Rounded Exclusion End Time"]

        if r_exc_end_time > r_end_time:
            r_exc_end_time = r_end_time
            exc_start_time = start_time

        if r_exc_start_time > r_start_time:
            r_exc_start_time = r_start_time
            exc_end_time = end_time

        if component_type == "Site":
            mec_site = all_site_info.loc[site, "Maximum Export Capacity"]

            df_irradiance_period = production_irr_df.loc[r_start_time:r_end_time, curated_column]
            df_irradiance_period["Energy Lost (MWh)"] = [df_irradiance_period[timestamp] * budget_pr.loc[
                site, timestamp.replace(day=1, hour=0, minute=0, second=0)] * nompower_component for timestamp in
                                                         df_irradiance_period.index]

            site_prod_incident_excludable = production_irr_df.loc[r_exc_start_time:r_exc_end_time,
                                            "Site Theoretical Production (kW)"]
            # print(site_prod_incident_om)
            site_prod_incident = production_irr_df.loc[r_start_time:r_end_time, "Site Theoretical Production (kW)"]
            # print(site_prod_incident)

            energy_lost_excludable_period = calculate_top40_energylost(exc_end_time, exc_start_time, r_exc_end_time,
                                                                       r_exc_start_time, site_prod_incident_excludable,
                                                                       granularity_factor)
            energy_lost_t40 = calculate_top40_energylost(start_time, end_time, r_start_time, r_end_time,
                                                         site_prod_incident, granularity_factor)
            energy_lost_contractual_om = energy_lost_t40 - energy_lost_excludable_period

            # print(energy_lost_contractual_om)
            # print(energy_lost_t40)

        else:

            power_inverters_incident_excludable = production_irr_df.loc[r_exc_start_time:r_exc_end_time,
                                                  "Top 40% Power"] * nompower_component
            power_inverters_incident = production_irr_df.loc[r_start_time:r_end_time,
                                       "Top 40% Power"] * nompower_component

            # Correct overlapping parents
            parents = (component_data_site.loc[component_data_site['Component'] == row['Related Component']]).loc[:,
                      component_data_site.columns.str.contains('Parent')].values.flatten().tolist()
            parents = [x for x in parents if str(x) != 'nan']

            parents_incidents = np_site_incidents[np_site_incidents['Related Component'].isin(parents)]

            relevant_parents_incidents = parents_incidents.loc[~(parents_incidents['Event End Time'] <= start_time) & ~(
                    parents_incidents['Event Start Time'] >= end_time) & ~(
                    parents_incidents['Failure Mode'] == "Curtailment")]

            if not relevant_parents_incidents.empty:
                power_inverters_incident_excludable, power_inverters_incident = correct_overlapping_parents(component,
                                                                                                            power_inverters_incident_excludable,
                                                                                                            power_inverters_incident,
                                                                                                            relevant_parents_incidents)

            energy_lost_excludable_period = calculate_top40_energylost(exc_start_time, exc_end_time, r_exc_start_time,
                                                                       r_exc_end_time,
                                                                       power_inverters_incident_excludable,
                                                                       granularity_factor)
            energy_lost_t40 = calculate_top40_energylost(start_time, end_time, r_start_time, r_end_time,
                                                         power_inverters_incident, granularity_factor)
            energy_lost_contractual_om = energy_lost_t40 - energy_lost_excludable_period

        np_site_incidents_par_exc.loc[index, "Energy Lost T40 (MWh)"] = energy_lost_t40
        np_site_incidents_par_exc.loc[index, "Energy Lost Contractual (MWh)"] = energy_lost_contractual_om

    return np_site_incidents_par_exc


def prod_irr_dataframe_site(site, contract_type, df_export_site, df_irradiance_site, component_data_site,
                            start_date, end_date, all_site_info, general_folder):  # , irr_threshold: float = 50):

    export_column = df_export_site.columns[df_export_site.columns.str.contains(site)][0]

    curated_column = df_irradiance_site.columns[df_irradiance_site.columns.str.contains('curated')][0]

    if contract_type == "Energy-based":

        print("Energy-based availability method")

        all_spower_inverter_ac_file = general_folder + "Site Data/" + site + "/2023/" + site + " Specific Power Inverter AC.xlsx"
        df_inverter_power = pd.read_excel(all_spower_inverter_ac_file, engine='openpyxl')

        # df_all_power = pd.read_excel(all_power_file, engine='openpyxl')#, index_col = 0)

        df_irradiance_site['Timestamp'] = pd.to_datetime(df_irradiance_site['Timestamp'])
        df_export_site['Timestamp'] = pd.to_datetime(df_export_site['Timestamp'])
        df_inverter_power['Timestamp'] = pd.to_datetime(df_inverter_power['Timestamp'])

        df_irradiance_site = df_irradiance_site.loc[
            (df_irradiance_site["Timestamp"] >= start_date) & (df_irradiance_site["Timestamp"] <= end_date)]
        df_export_site = df_export_site.loc[
            (df_export_site["Timestamp"] >= start_date) & (df_export_site["Timestamp"] <= end_date)]
        df_inverter_power = df_inverter_power.loc[
            (df_inverter_power["Timestamp"] >= start_date) & (df_inverter_power["Timestamp"] <= end_date)]

        df_irradiance_site.set_index('Timestamp', inplace=True)
        df_export_site.set_index('Timestamp', inplace=True)
        df_inverter_power.set_index('Timestamp', inplace=True)

        print("Calculating Top 40% Power")

        top40_power = top40_power_inverter(df_inverter_power, all_site_info, site)
        top40_power = top40_power[~top40_power.index.duplicated(keep='first')]

        print("Calculating theoretical production of site")

        df_irradiance_site = theoretical_power_site(df_irradiance_site, all_site_info, site, curated_column)

        print("Creating table of production and irradiance with top 40% inverter production")

        production_irr_df = pd.concat([df_export_site, df_irradiance_site, top40_power], axis=1)

        print("Done")

    elif contract_type == "Time-based":

        print("Time-based availability method")

        df_irradiance_site['Timestamp'] = pd.to_datetime(df_irradiance_site['Timestamp'])
        df_export_site['Timestamp'] = pd.to_datetime(df_export_site['Timestamp'])

        df_irradiance_site = df_irradiance_site.loc[
            (df_irradiance_site["Timestamp"] >= start_date) & (df_irradiance_site["Timestamp"] <= end_date)]
        df_export_site = df_export_site.loc[
            (df_export_site["Timestamp"] >= start_date) & (df_export_site["Timestamp"] <= end_date)]

        df_irradiance_site.set_index('Timestamp', inplace=True)
        df_export_site.set_index('Timestamp', inplace=True)

        print("Calculating theoretical production of site")

        df_irradiance_site = theoretical_power_site(df_irradiance_site, all_site_info, site, curated_column)

        print("Creating table of production and irradiance")

        production_irr_df = pd.concat([df_export_site, df_irradiance_site], axis=1)

        print("Done")

    return production_irr_df


def active_hours_non_excludable(production_irr_df, curated_column, np_site_incidents_non_exc, component_data_site,
                                granularity_factor, np_site_incidents, irradiance_threshold):
    for index, row in np_site_incidents_non_exc.iterrows():

        component = row["Related Component"]

        component_type = \
        component_data_site.loc[component_data_site["Component"] == component]["Component Type"].values[0]

        start_time = row["Event Start Time"]
        end_time = row["Event End Time"]

        r_start_time = row["Rounded Event Start Time"]
        r_end_time = row["Rounded Event End Time"]

        if component_type == "Site":

            irradiance_incident = production_irr_df.loc[r_start_time:r_end_time, curated_column]
            irradiance_incident = irradiance_incident[irradiance_incident >= irradiance_threshold]
            # print(site_prod_incident)

            active_hours_incident = len(irradiance_incident) / 4



        else:
            irradiance_incident = production_irr_df.loc[r_start_time:r_end_time, curated_column]
            irradiance_incident = irradiance_incident[irradiance_incident >= irradiance_threshold]

            if irradiance_incident.empty:
                active_hours_incident = active_hours_incident_contract = 0

            else:
                # Correct overlapping parents
                parents = (component_data_site.loc[component_data_site['Component'] == row['Related Component']]).loc[:,
                          component_data_site.columns.str.contains('Parent')].values.flatten().tolist()
                parents = [x for x in parents if str(x) != 'nan']

                parents_incidents = np_site_incidents[np_site_incidents['Related Component'].isin(parents)]

                relevant_parents_incidents = parents_incidents.loc[
                    ~(parents_incidents['Event End Time'] <= start_time) & ~(
                            parents_incidents['Event Start Time'] >= end_time) & ~(
                            parents_incidents['Failure Mode'] == "Curtailment")]

                if not relevant_parents_incidents.empty:
                    try:
                        del timestamps_to_remove
                    except NameError:
                        pass

                    for p_index, p_row in relevant_parents_incidents.iterrows():

                        timestamps_to_remove_incident = list(
                            pd.date_range(p_row["Rounded Event Start Time"], p_row["Rounded Event End Time"],
                                          freq="15T"))
                        # print(timestamps_to_remove_incident)
                        try:
                            timestamps_to_remove.extend(timestamps_to_remove_incident)
                        except NameError:
                            timestamps_to_remove = timestamps_to_remove_incident

                    irradiance_incident = production_irr_df.loc[r_start_time:r_end_time, curated_column]
                    timestamps_to_remove = [timestamp for timestamp in sorted(list(set(timestamps_to_remove))) if
                                            timestamp in irradiance_incident.index][
                                           :-1]  # removes last timestamp as the parent is already producing

                    irradiance_incident.drop(timestamps_to_remove, inplace=True)
                    irradiance_incident = irradiance_incident[irradiance_incident >= irradiance_threshold]

                if irradiance_incident.empty:
                    active_hours_incident = active_hours_incident_contract = 0

                else:
                    active_hours_incident = len(irradiance_incident) / 4

        np_site_incidents_non_exc.loc[
            index, ["Active Hours (h)", "Active Hours Contractual (h)"]] = active_hours_incident
        # np_site_incidents_par_exc.loc[index, "Active Hours Contractual (h)"] = active_hours_incident

    return np_site_incidents_non_exc


def active_hours_excludable(production_irr_df, curated_column, np_site_incidents_par_exc, component_data_site,
                            granularity_factor,
                            np_site_incidents, irradiance_threshold):
    for index, row in np_site_incidents_par_exc.iterrows():

        component = row["Related Component"]
        # print(component)

        component_type = \
        component_data_site.loc[component_data_site["Component"] == component]["Component Type"].values[0]
        nompower_component = \
        component_data_site.loc[component_data_site["Component"] == component]["Nominal Power DC"].values[0]

        start_time = row["Event Start Time"]
        end_time = row["Event End Time"]
        exc_start_time = row["Exclusion Start Time"]
        exc_end_time = row["Exclusion End Time"]

        r_start_time = row["Rounded Event Start Time"]
        r_end_time = row["Rounded Event End Time"]
        r_exc_start_time = row["Rounded Exclusion Start Time"]
        r_exc_end_time = row["Rounded Exclusion End Time"]

        if r_exc_end_time > r_end_time:
            r_exc_end_time = r_end_time
            exc_start_time = start_time

        if r_exc_start_time > r_start_time:
            r_exc_start_time = r_start_time
            exc_end_time = end_time

        if component_type == "Site":
            irradiance_incident = production_irr_df.loc[r_start_time:r_end_time, curated_column]
            irradiance_incident_excludable = production_irr_df.loc[r_exc_start_time:r_exc_end_time, curated_column]
            # print(site_prod_incident_om)

            # print(site_prod_incident)

            active_hours_incident = len(irradiance_incident) / 4
            active_hours_incident_excludable = len(irradiance_incident_excludable) / 4
            active_hours_incident_contract = active_hours_incident - active_hours_incident_excludable

            print(row["ID"])
            print("Active hours incident :")
            print(active_hours_incident)
            print("Active hours incident Excludable:")
            print(active_hours_incident_excludable)
            print("Active hours incident Contract:")
            print(active_hours_incident_contract)


        else:
            irradiance_incident_excludable = production_irr_df.loc[r_exc_start_time:r_exc_end_time, curated_column]
            irradiance_incident = production_irr_df.loc[r_start_time:r_end_time, curated_column]

            # print(irradiance_incident)

            # irradiance_incident = irradiance_incident.loc[irradiance_incident[curated_column] >= irradiance_threshold]
            # irradiance_incident_excludable = irradiance_incident_excludable.loc[irradiance_incident_excludable[curated_column] >= irradiance_threshold]

            irradiance_incident = irradiance_incident[irradiance_incident >= irradiance_threshold]
            irradiance_incident_excludable = irradiance_incident_excludable[
                irradiance_incident_excludable >= irradiance_threshold]

            if irradiance_incident.empty:
                active_hours_incident = active_hours_incident_contract = 0

            elif irradiance_incident_excludable.empty:
                active_hours_incident = active_hours_incident_contract = len(irradiance_incident) / 4

            else:
                # Correct overlapping parents
                parents = (component_data_site.loc[component_data_site['Component'] == row['Related Component']]).loc[:,
                          component_data_site.columns.str.contains('Parent')].values.flatten().tolist()
                parents = [x for x in parents if str(x) != 'nan']

                parents_incidents = np_site_incidents[np_site_incidents['Related Component'].isin(parents)]

                relevant_parents_incidents = parents_incidents.loc[
                    ~(parents_incidents['Event End Time'] <= start_time) & ~(
                            parents_incidents['Event Start Time'] >= end_time) & ~(
                            parents_incidents['Failure Mode'] == "Curtailment")]

                if not relevant_parents_incidents.empty:
                    irradiance_incident_excludable, irradiance_incident = correct_overlapping_parents(component,
                                                                                                      irradiance_incident_excludable,
                                                                                                      irradiance_incident,
                                                                                                      relevant_parents_incidents)
                    irradiance_incident = irradiance_incident[irradiance_incident >= irradiance_threshold]
                    irradiance_incident_excludable = irradiance_incident_excludable[
                        irradiance_incident_excludable >= irradiance_threshold]

                if irradiance_incident.empty:
                    active_hours_incident = active_hours_incident_contract = 0

                elif irradiance_incident_excludable.empty:
                    active_hours_incident = active_hours_incident_contract = irradiance_incident.shape[0] / 4

                else:
                    active_hours_incident = len(irradiance_incident) / 4
                    active_hours_incident_excludable = len(irradiance_incident_excludable) / 4
                    active_hours_incident_contract = active_hours_incident - active_hours_incident_excludable

        np_site_incidents_par_exc.loc[index, "Active Hours (h)"] = active_hours_incident
        np_site_incidents_par_exc.loc[index, "Active Hours Contractual (h)"] = active_hours_incident_contract

    return np_site_incidents_par_exc


def contractual_availability_kpis(site, component_data_site, incidents, production_irr_df, periods_dict,
                                  contract_type, irradiance_threshold, curated_column, export_column):
    nominal_power_site = component_data_site.loc[component_data_site["Component"] == site]["Nominal Power DC"].values[0]

    # Granularity
    granularity = (production_irr_df.index[1] - production_irr_df.index[0])
    granularity_factor = granularity.seconds / (3600)  # number of seconds tranformed into hours

    for key in periods_dict.keys():

        start_time_period = periods_dict[key][0]
        end_time_period = periods_dict[key][1]
        period_str = str(start_time_period.date()) + " to " + str(end_time_period.date())

        print(start_time_period.date(), "  ", end_time_period.date())

        incidents_month = incidents.loc[~(incidents["Event Start Time"] >= end_time_period)
                                        & ~(incidents["Event End Time"] <= start_time_period)]

        # Correct start and end time
        incidents_month["Event Start Time"] = [time if time >= start_time_period else start_time_period for time in
                                               incidents_month["Event Start Time"]]
        incidents_month["Event End Time"] = [time if time <= end_time_period else end_time_period for time in
                                             incidents_month["Event End Time"]]

        # Prod irr dataframe in period filtered by irr_thresh
        production_irr_df_slice = production_irr_df.loc[start_time_period:end_time_period, :]

        # Incidents breakdown for exclusion
        np_site_incidents, np_site_incidents_non_exc, np_site_incidents_par_exc, np_site_incidents_sub_inv, np_site_incidents_curt = get_incidents_df_for_exclusions(
            incidents_month, site)

        if contract_type == "Energy-based":

            np_site_incidents_non_exc = energy_lost_nonexcludable(production_irr_df_slice, np_site_incidents_non_exc,
                                                                  component_data_site, granularity_factor,
                                                                  np_site_incidents)
            np_site_incidents_par_exc = energy_lost_excludable(production_irr_df_slice, np_site_incidents_par_exc,
                                                               component_data_site, granularity_factor,
                                                               np_site_incidents)
            np_site_incidents_sub_inv = energy_lost_excludable(production_irr_df_slice, np_site_incidents_sub_inv,
                                                               component_data_site, granularity_factor,
                                                               np_site_incidents)

            print("Non-excludable incidents")
            print("Energy Lost (MWh): ", np_site_incidents_non_exc["Energy Lost (MWh)"].sum())
            print("Energy Lost T40 (MWh): ", np_site_incidents_non_exc["Energy Lost T40 (MWh)"].sum())
            print("Energy Lost Contractual (MWh): ", np_site_incidents_non_exc["Energy Lost Contractual (MWh)"].sum())

            print("Excludable incidents")
            print("Energy Lost (MWh): ",
                  np_site_incidents_par_exc["Energy Lost (MWh)"].sum() + np_site_incidents_sub_inv[
                      "Energy Lost (MWh)"] + np_site_incidents_curt["Energy Lost (MWh)"].sum())
            print("Energy Lost T40 (MWh): ",
                  np_site_incidents_par_exc["Energy Lost T40 (MWh)"].sum() + np_site_incidents_sub_inv[
                      "Energy Lost T40 (MWh)"] + np_site_incidents_curt["Energy Lost (MWh)"].sum())
            print("Energy Lost Contractual (MWh): ", np_site_incidents_par_exc["Energy Lost Contractual (MWh)"].sum())

            final_incidents_df = pd.concat(
                [np_site_incidents_non_exc, np_site_incidents_par_exc, np_site_incidents_sub_inv,
                 np_site_incidents_curt]).drop_duplicates(subset="ID")

            kpis_site = {"Energy Produced": production_irr_df_slice[export_column].sum() / 1000,
                         "Energy Expected": production_irr_df_slice["Site Theoretical Production (kW)"].sum() / 4000,
                         "Energy Lost Modelled": final_incidents_df["Energy Lost (MWh)"].sum(),
                         "Energy Lost T40": final_incidents_df["Energy Lost T40 (MWh)"].sum(),
                         "Energy Lost Contractual": final_incidents_df["Energy Lost Contractual (MWh)"].sum()}

            kpis_site["Contractual Energy-based Availability"] = kpis_site["Energy Produced"] / (
                    kpis_site["Energy Produced"] + kpis_site["Energy Lost Contractual"])
            kpis_site["Raw Energy-based Availability"] = kpis_site["Energy Produced"] / kpis_site["Energy Expected"]

            kpis_site_df_period = pd.DataFrame.from_dict(kpis_site, orient='index', columns=[period_str])

        elif contract_type == "Time-based":

            np_site_incidents_par_exc = active_hours_excludable(production_irr_df_slice, curated_column,
                                                                np_site_incidents_par_exc, component_data_site,
                                                                granularity_factor, np_site_incidents,
                                                                irradiance_threshold)
            np_site_incidents_non_exc = active_hours_non_excludable(production_irr_df_slice, curated_column,
                                                                    np_site_incidents_non_exc, component_data_site,
                                                                    granularity_factor, np_site_incidents,
                                                                    irradiance_threshold)
            np_site_incidents_sub_inv = active_hours_excludable(production_irr_df_slice, curated_column,
                                                                np_site_incidents_sub_inv, component_data_site,
                                                                granularity_factor, np_site_incidents,
                                                                irradiance_threshold)

            print("Non-excludable incidents")
            print("Active Hours (h)", np_site_incidents_non_exc["Active Hours (h)"].sum())
            print("Active hours Contractual (h)", np_site_incidents_non_exc["Active Hours Contractual (h)"].sum())

            print("Excludable incidents")
            print("Active Hours (h)", np_site_incidents_par_exc["Active Hours (h)"].sum() + np_site_incidents_sub_inv[
                "Active Hours (h)"].sum() + np_site_incidents_curt["Active Hours (h)"].sum())
            print("Active Hours Contractual (h)", np_site_incidents_par_exc["Active Hours Contractual (h)"].sum())

            final_incidents_df = pd.concat(
                [np_site_incidents_non_exc, np_site_incidents_par_exc, np_site_incidents_sub_inv,
                 np_site_incidents_curt]).drop_duplicates(subset="ID")

            if not final_incidents_df.empty:
                final_incidents_df["Weighted Downtime"] = (final_incidents_df["Capacity Related Component"] *
                                                           final_incidents_df["Active Hours (h)"]) / nominal_power_site

                final_incidents_df["Weighted Downtime Contractual"] = (final_incidents_df[
                                                                           "Capacity Related Component"] *
                                                                       final_incidents_df[
                                                                           "Active Hours Contractual (h)"]) / nominal_power_site

                weighted_downtime = final_incidents_df["Weighted Downtime"].sum()
                weighted_downtime_contractual = final_incidents_df["Weighted Downtime Contractual"].sum()

                weighted_downtime_sub_inverter = \
                final_incidents_df.loc[final_incidents_df["Excludable Category"] == "Sub-Inverter Level"][
                    "Weighted Downtime"].sum()
                weighted_downtime_curt = \
                final_incidents_df.loc[final_incidents_df["Excludable Category"] == "Curtailment"][
                    "Weighted Downtime"].sum()

                weighted_downtime_contractual_sub_inv = weighted_downtime_contractual + weighted_downtime_sub_inverter


            else:
                weighted_downtime = weighted_downtime_contractual = weighted_downtime_sub_inverter = weighted_downtime_contractual_sub_inv = weighted_downtime_curt = 0

            kpis_site = {"Energy Produced": production_irr_df_slice[export_column].sum() / 1000,
                         "Energy Expected": production_irr_df_slice["Site Theoretical Production (kW)"].sum() / 4000,
                         "Active hours total": production_irr_df_slice.loc[
                                                   production_irr_df_slice[curated_column] >= irradiance_threshold][
                                                   curated_column].count() / 4,
                         "Site Eq. A.h. lost": weighted_downtime,
                         "Site Eq. A.h. lost Contractual": weighted_downtime_contractual,
                         "Site Eq. A.h. lost Sub-Inverter": weighted_downtime_sub_inverter}

            kpis_site["Contractual TBA"] = (kpis_site["Active hours total"] - kpis_site[
                "Site Eq. A.h. lost Contractual"]) / kpis_site["Active hours total"]
            kpis_site["Contractual Sub-Inv. TBA"] = (kpis_site[
                                                         "Active hours total"] - weighted_downtime_contractual_sub_inv) / \
                                                    kpis_site["Active hours total"]
            kpis_site["Raw TBA"] = (kpis_site["Active hours total"] - kpis_site["Site Eq. A.h. lost"]) / kpis_site[
                "Active hours total"]

            kpis_site_df_period = pd.DataFrame.from_dict(kpis_site, orient='index', columns=[period_str])

        try:
            site_kpis_df = site_kpis_df.join(kpis_site_df_period)
        except NameError:
            site_kpis_df = kpis_site_df_period

        try:
            site_incidents_per_period[site + period_str] = final_incidents_df
        except NameError:
            site_incidents_per_period = {site + period_str: final_incidents_df}

    return site_kpis_df, site_incidents_per_period

