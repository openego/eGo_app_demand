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
                  'sector_consumption_industrial']
                  
    load_areas = pd.concat(
        [load_areas,pd.DataFrame(columns=column_list)])

    float_list = pd.Series(load_areas.reset_index()[index_col].apply(
        normalized_random_sectoral_shares,
        **{'size': size, 'overall_demand': overall_demand}
        ).values, index=load_areas.index)

    load_areas.loc[:, column_list] = float_list.tolist()
        
    return load_areas
    
    
def add_sectoral_peak_load(load_areas, **kwargs):
    r"""Add peak load per sector based on given annual consumption
    """

    # define data year
    # TODO: in the future get this from somewhere else                    
    year = 2015

    # call demandlib
    peak_load = dm.electrical_demand(method='calculate_profile',
                                     year=year,
                                     ann_el_demand_per_sector= {
                                         'h0':
                                             load_areas['sector_consumption_residential'],
                                         'g0':
                                             load_areas['sector_consumption_retail'],
                                         'i0':
                                             load_areas['sector_consumption_residential']}
                                        ).elec_demand.max(axis=0)

    return peak_load
    

def peak_load_table(mode, schema, table, target_table, section, index_col, db_group,
                    dummy):
    r"""Calculates SLP based on input data from oedb

    The demandlib of oemof is applied to retrieve demand time-series based on
    standdard demand profiles
    
    Parameters
    ----------
    mode : 'lastgebiete', str
        Declares modus that is used (currently only one available)
    schema : {'calc_demand'}, str, optional
        Database schema where table containing intermediate resutls is located
    table : {'osm_deu_polygon_lastgebiet_100_spf'}
        Database table with intermediate resutls
    
    """

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
        peak_demand = load_areas.iloc[:5].apply(
            add_sectoral_peak_load, axis=1)
    else:
        peak_demand = load_areas.apply(
            add_sectoral_peak_load, axis=1)

    # derive resulting table from peak_demand dataframe
    results_table = peak_demand

    # establish database connection
    conn = db.connection(section=section)

    # # create empty table with serial primary key
    # tools.create_empty_table_serial_primary(conn, schema, target_table,
    #                                         columns=list(
    #                                             results_table.columns.values))

    # write results to new database table
    results_table.to_sql(target_table,
                         conn,
                         schema=schema,
                         index=True,
                         if_exists='fail')

    tools.grant_db_access(conn, schema, target_table, db_group)


if __name__ == '__main__':

    # welcome message
    parser = argparse.ArgumentParser(description='This is the demandlib ' +
        'applied in the open_eGo project.')

    parser.add_argument('-t', '--table', nargs=1, help='Database table ' +
        'with input data', default='rli_deu_lastgebiete')
    parser.add_argument('-s', '--schema', nargs=1, help='Database schema',
                        default='orig_geo_rli')
    parser.add_argument('-tt', '--target-table', nargs=1, help='Database ' +
        'table for results data containing peak loads',
                        default='rl_deu_peak_load_spf')
    parser.add_argument('-ds', '--database-section', nargs=1, help='Section ' +
        'in `config.ini` containing database details',
                        default='oedb')
    parser.add_argument('-icol', '--index-column', nargs=1, help='Annual ' +
        'consumption data table index column',
                        default='la_id')
    parser.add_argument('-g', '--db-group', nargs=1, help='Database ' +
        'user group that rights are granted to',
                        default='oeuser')
    parser.add_argument('--dummy', dest='dummy', action='store_true',
                        help='If set, dummy data is applied to annual ' +
                        'consumption.', default=False)
    args = parser.parse_args()

    # unpack lists
    if isinstance(args.schema, list):
        args.schema = args.schema[0]

    if isinstance(args.table, list):
        args.table = args.table[0]
    
    
    peak_load_table(args.schema,
                    args.table,
                    args.target_table,
                    args.database_section,
                    args.index_column,
                    args.db_group,
                    args.dummy)