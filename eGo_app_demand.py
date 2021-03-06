#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
logging.getLogger().setLevel(logging.WARNING)

from oemof import db
import pandas as pd
import argparse
from oemof.demandlib import demand as dm
import numpy as np
from oemof.db import tools
from matplotlib import pyplot as plt


def get_load_areas_table(schema, table, index_col, section, columns=None):
    r"""Retrieve load areas intermediate results table from oedb
    """
    # get engine for database connection
    conn = db.connection(section=section)

    # retrieve table with processed input data
    input_table = pd.read_sql_table(table, conn, schema=schema,
                                    index_col=index_col, columns=columns)

    return input_table


def normalized_random_sectoral_shares(seed, **kwargs):
    r"""Create list of floats
    """
    # create list of random ints with size of 'size'
    int_list = np.random.choice(seed * 11, kwargs['size'])

    # b is normalized list of a
    float_list = (int_list / np.sum(int_list)) * kwargs['overall_demand']

    return float_list


def fill_table_by_random_consuption(load_areas, index_col, size=3, overall_demand=1e5):
    r"""Generates sectoral consumption columns

    Adds three columns each for sectors of

    * residential
    * retail
    * industrial.

    Based on overall defined demand random consumption is determined.
    """
    column_list = ['sector_consumption_residential',
                  'sector_consumption_retail',
                  'sector_consumption_industrial',
                   'sector_consumption_agricultural']

    load_areas = pd.concat(
        [load_areas,pd.DataFrame(columns=column_list)])

    float_list = pd.Series(load_areas.reset_index()[index_col].apply(
        normalized_random_sectoral_shares,
        **{'size': size, 'overall_demand': overall_demand}
        ).values, index=load_areas.index)

    load_areas.loc[:, column_list] = float_list.tolist()

    return load_areas


def add_sectoral_peak_load(load_areas, mode, **kwargs):
    r"""Add peak load per sector based on given annual consumption
    """

    # define data year
    # TODO: in the future get this from somewhere else
    year = 2015

    # call demandlib
    tmp_peak_load = dm.electrical_demand(method='calculate_profile',
                                     year=year,
                                     ann_el_demand_per_sector= {
                                         'h0':
                                             load_areas['sector_consumption_residential'],
                                         'g0':
                                             load_areas['sector_consumption_retail'],
                                         'i0':
                                             load_areas['sector_consumption_industrial'],
                                        'l0':
                                            load_areas['sector_consumption_agricultural']}
                                     ).elec_demand

    if mode == 'peak_load':
        peak_load = tmp_peak_load.max(axis=0)
    elif mode == 'timeseries':
        peak_load = tmp_peak_load

    return peak_load


def peak_load_table(mode, schema, table, target_table, section, index_col,
                    db_group, dummy, file):
    r"""Calculates SLP based on input data from oedb

    The demandlib of oemof is applied to retrieve demand time-series based on
    standdard demand profiles

    Parameters
    ----------
    mode : {'peak_load', 'timeseries'}, str
        Declares modus that is used
    schema : {'calc_demand'}, str, optional
        Database schema where table containing intermediate resutls is located
    table : {'osm_deu_polygon_lastgebiet_100_spf'}
        Database table with intermediate resutls

    Notes
    -----
    Column names of resulting table are set to hard-coded.

    """
    columns_names = {'h0': 'residential',
                     'g0': 'retail',
                     'i0': 'industrial',
                     'l0': 'agricultural'}

    if dummy is True:
        # retrieve load areas table
        load_areas = get_load_areas_table(schema, table, index_col, section,
                                          columns=[index_col])

        # fill missing consumption data by random values
        load_areas = fill_table_by_random_consuption(load_areas, index_col)
    else:
        # retrieve load areas table
        columns = [index_col,
                   'sector_consumption_residential',
                   'sector_consumption_retail',
                   'sector_consumption_industrial',
                   'sector_consumption_agricultural']

        load_areas = get_load_areas_table(schema, table, index_col, section,
                                          columns=columns)
    # add sectoral peak load columns
    if dummy is True:
        results_table = load_areas.iloc[:5].apply(
            add_sectoral_peak_load, axis=1, args=(mode))
    else:
        if mode == 'peak_load':
            results_table = load_areas.fillna(0).apply(
                add_sectoral_peak_load, axis=1, args=(mode,))

        elif mode == 'timeseries':

            for la_id in load_areas.index.values:
                # retrieve timeseries for one loadarea
                timeseries = add_sectoral_peak_load(load_areas.loc[la_id][[
                    'sector_consumption_residential',
                    'sector_consumption_retail',
                    'sector_consumption_industrial',
                    'sector_consumption_agricultural']].fillna(0), mode)

                # reshape dataframe and concatenate
                timeseries['la_id'] = la_id
                timeseries.set_index(['la_id'], inplace=True, append=True)
                timeseries.index.names=['date', 'la_id']
                timeseries = timeseries.reorder_levels(['la_id', 'date'])
                timeseries.sort_index()
                # timeseries = timeseries.reindex(columns=['residential',
                #                                          'retail',
                #                                          'industrial',
                #                                          'agricultural'],
                #                                 fill_value=0)
                if 'results_table' not in locals():
                    results_table = timeseries
                else:
                    results_table = pd.concat([results_table,
                                               timeseries], axis=0)
                del(timeseries)
        else:
            raise NameError('Select mode out of `peak_load` and `timeseries`')

    # establish database connection
    conn = db.connection(section=section)

    # # create empty table with serial primary key
    # tools.create_empty_table_serial_primary(conn, schema, target_table,
    #                                         columns=list(
    #                                             results_table.columns.values))

    # rename column names
    results_table = results_table.rename(columns=columns_names)

    # save output
    if file is None:


        # replace NaN's by zeros
        results_table = results_table.fillna(0)

        # write results to new database table
        results_table.to_sql(target_table,
                             conn,
                             schema=schema,
                             index=True,
                             if_exists='fail')

        # grant access to db_group
        tools.grant_db_access(conn, schema, target_table, db_group)

        # change owner of table to db_group
        tools.change_owner_to(conn, schema, target_table, db_group)

        # add primary key constraint on id column
        tools.add_primary_key(conn, schema, target_table, index_col)
    else:
        results_table.to_hdf(file + '.h5', 'results_table')


def analyze_demand_data(file, schema, table, section, year=2013):
    r"""

    Parameters
    ----------
    file : str
        Filename that specifies location of hdf5 file containing demand data

    """

    # get slp based timeseries
    if file is not None:
        slp_demand_data = pd.read_hdf(file + '.h5')

        slp_annual_sum = slp_demand_data.sum().sum()

        # sum up across laod areas and sectors
        slp_demand_data_wo_industrial = slp_demand_data.sum(
            level='date')[['residential', 'retail', 'agricultural']].sum(axis=1)
        slp_demand_data = slp_demand_data.sum(level='date').sum(axis=1)

        # rename index: compability with entsoe data
        slp_demand_data.index = slp_demand_data.index.rename('timestamp')
        slp_demand_data_wo_industrial.index = (
            slp_demand_data_wo_industrial.index.rename('timestamp'))


    # get entsoe demand data for germany

    # establish database connection
    conn = db.connection(section=section)

    # retrieve demand data from oedb
    # returns only demand data for germany of year 2015
    entsoe_demand = pd.read_sql_table(table,
                                      conn,
                                      schema=schema,
                                      columns=['load_de'],
                                      index_col='timestamp')

    entsoe_demand_germany_2015 = entsoe_demand.loc['2015']

    # fill nan's by average demand
    average = entsoe_demand_germany_2015.mean()

    entsoe_demand_germany_2015 = entsoe_demand_germany_2015.fillna(average)

    # scale entsoe demand data by annual demand given by slp data
    entsoe_demand_germany_2015_scaled = (entsoe_demand_germany_2015 /
                                         entsoe_demand_germany_2015.sum() *
                                         slp_annual_sum)

    # put entsoe and slp data in one dataframe
    demand_data = slp_demand_data.to_frame(name='slp')

    # add slp without industrial
    demand_data['slp_wo_industrial'] = slp_demand_data_wo_industrial
    demand_data['entsoe'] = entsoe_demand_germany_2015_scaled

    # add industrial demand timeseries from diff to entsoe
    demand_data['industrial_slp_entsoe_diff'] = (demand_data['entsoe'] -
        demand_data['slp_wo_industrial'])

    # calculate hourly deviation
    demand_data['deviation'] = demand_data['entsoe'] - demand_data['slp']
    demand_data['slp_industrial'] = (demand_data['slp'] -
                                     demand_data['slp_wo_industrial'])

    # plot demand data of arbitrary chosen week
    # demand_data.loc['2015-03-20':'2015-03-26', ['slp', 'entsoe']].plot()

    # plot deviation as histogram
    demand_data['deviation'].hist(bins=500)
    plt.savefig('demand_timeseries_diff_hist.pdf')

    # plot timeseries in january
    demand_data.loc['2015-01', ['slp', 'entsoe', 'slp_wo_industrial']].plot()
    plt.savefig('demand_timeseries_slp_vs_entsoe.pdf')

    # plot timeseries for selected week
    weeks = [27, 32, 5, 12] # given in calender weeks

    for week in weeks:
        demand_data[demand_data.index.week == week][
            ['slp', 'entsoe', 'slp_wo_industrial']].plot()
        plt.ylabel('Electricity demand in GW')
        plt.savefig('demand_timeseries_slp_vs_entsoe_KW_' + str(week) + '.pdf')

        demand_data[demand_data.index.week == week][
            ['slp_industrial', 'industrial_slp_entsoe_diff']].plot()
        plt.ylabel('Electricity demand in GW')
        plt.savefig('industrial_demand_timeseries_slp_vs_diff_KW_' +
                    str(week) + '.pdf')

    # plt.show()


if __name__ == '__main__':

    # welcome message
    parser = argparse.ArgumentParser(description='This is the demandlib ' +
        'applied in the open_eGo project.' +
        'The demandlib founds on Standard Lastprofile of the BDEW.')

    parser.add_argument('mode', help='Selects mode of using `eGo_app_demand`.' +
                        'Select `peak_load` to obtain scalar peak demand of ' +
                        'a year.' +
                        'Choose `timeseries` to get a full timeseries in ' +
                        'temporal resolution of one hourly.')
    parser.add_argument('-t', '--table', nargs=1, help='Database table ' +
        'with input data', default='ego_deu_loads_consumption_spf')
    parser.add_argument('-s', '--schema', nargs=1, help='Database schema',
                        default='orig_ego')
    parser.add_argument('-tt', '--target-table', nargs=1, help='Database ' +
        'table for results data containing peak loads',
                        default='ego_deu_peak_load_spf')
    parser.add_argument('-ds', '--database-section', nargs=1, help='Section ' +
        'in `config.ini` containing database details',
                        default='oedb')
    parser.add_argument('-icol', '--index-column', nargs=1, help='Annual ' +
        'consumption data table index column',
                        default='id')
    parser.add_argument('-g', '--db-group', nargs=1, help='Database ' +
        'user group that rights are granted to',
                        default='oeuser')
    parser.add_argument('-f', '--file', nargs=1, help='Filename ' +
        'results are stored to (without extension)', default=None)
    parser.add_argument('--dummy', dest='dummy', action='store_true',
                        help='If set, dummy data is applied to annual ' +
                        'consumption.', default=False)
    args = parser.parse_args()

    # unpack lists
    if isinstance(args.schema, list):
        args.schema = args.schema[0]

    if isinstance(args.table, list):
        args.table = args.table[0]

    if isinstance(args.target_table, list):
        args.target_table = args.target_table[0]

    if isinstance(args.index_column, list):
        args.index_column = args.index_column[0]

    if isinstance(args.file, list):
        args.file = args.file[0]

    if (args.mode == 'peak_load' or args.mode == 'timeseries'):
        peak_load_table(args.mode,
                        args.schema,
                        args.table,
                        args.target_table,
                        args.database_section,
                        args.index_column,
                        args.db_group,
                        args.dummy,
                        args.file)
    elif args.mode == 'analyze_timeseries':
        analyze_demand_data(args.file,
                            args.schema,
                            args.table,
                            args.database_section)
