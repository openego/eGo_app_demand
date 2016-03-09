#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
logging.getLogger().setLevel(logging.WARNING)

from oemof import db
import pandas as pd
import argparse
from oemof.demandlib import demand as dm
import numpy as np


def get_load_areas_table(schema, table, columns=None):
    r"""Retrieve load areas intermediate results table from oedb
    """
    # get engine for database connection
    conn = db.connection(db_section='oedb')
    
    # retrieve table with processed input data
    input_table = pd.read_sql_table(table, conn, schema=schema,
                                    index_col='lgid', columns=columns)
    
    return input_table
    
    
def normalized_random_sectoral_shares(seed, **kwargs):
    r"""Create list of floats
    """
    # create list of random ints with size of 'size'
    int_list = np.random.choice(seed * 11, kwargs['size'])
    
    # b is normalized list of a
    float_list = (int_list / np.sum(int_list)) * kwargs['overall_demand']
    
    return float_list
    
    
def fill_table_by_random_consuption(load_areas, size=3, overall_demand=1e5):
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

    float_list = pd.Series(load_areas.reset_index()['lgid'].apply(
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
    # TODO: make this nicer when sectoral demand timeseries returns are
    # implemented in demandlib
    if kwargs['sector'] is 'residential':
        peak_load = dm.electrical_demand(method='calculate_profile',
                                          year=year,
                                          ann_el_demand_per_sector=[
                                          {'ann_el_demand': (
                                          load_areas['sector_consumption_residential']),
                                          'selp_type': 'h0'},
                                          {'ann_el_demand': (
                                          0),
                                          'selp_type': 'g0'},
                                          {'ann_el_demand': (
                                          0),
                                         'selp_type': 'i0'}]).elec_demand.max()
    elif kwargs['sector'] is 'retail':
        peak_load = dm.electrical_demand(method='calculate_profile',
                                          year=year,
                                          ann_el_demand_per_sector=[
                                          {'ann_el_demand': (
                                          0),
                                          'selp_type': 'h0'},
                                          {'ann_el_demand': (
                                          load_areas['sector_consumption_retail']),
                                          'selp_type': 'g0'},
                                          {'ann_el_demand': (
                                          0),
                                         'selp_type': 'i0'}]).elec_demand.max()
                                         
    elif kwargs['sector'] is 'industrial':
        peak_load = dm.electrical_demand(method='calculate_profile',
                                          year=year,
                                          ann_el_demand_per_sector=[
                                          {'ann_el_demand': (
                                          0),
                                          'selp_type': 'h0'},
                                          {'ann_el_demand': (
                                          0),
                                          'selp_type': 'g0'},
                                          {'ann_el_demand': (
                                          load_areas['sector_consumption_industrial']),
                                         'selp_type': 'i0'}]).elec_demand.max()
    else:
        raise KeyError('Wrong key provided.')
                                     
    return peak_load
    

def peak_load_table(schema, table, target_table, dummy):
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
        load_areas = get_load_areas_table(schema, table, columns=['lgid'])
        
        # fill missing consumption data by random values
        load_areas = fill_table_by_random_consuption(load_areas)
    else:
        # retrieve load areas table
        columns = ['lgid',
                   'sector_consumption_residential',
                   'sector_consumption_retail',
                   'sector_consumption_industrial',
                   'sector_consumption_agricultural']

        load_areas = get_load_areas_table(schema, table, columns=columns)

    # add sectoral peak load columns
    load_areas['peak_load_retail'] = load_areas.apply(
        add_sectoral_peak_load, axis=1, **{'sector': 'retail'})
    load_areas['peak_load_residential'] = load_areas.apply(
        add_sectoral_peak_load, axis=1, **{'sector': 'residential'})
    load_areas['peak_load_industrial'] = load_areas.apply(
        add_sectoral_peak_load, axis=1, **{'sector': 'industrial'})
    
    # derive resulting table from load_areas dataframe
    results_table = load_areas[['peak_load_residential',
                  'peak_load_retail',
                  'peak_load_industrial']].reset_index()
                  
    # write results to new database table
    conn = db.connection(db_section='oedb')
    if target_table == None:
        target_table = table.replace('lastgebiete', 'peak_load')

    results_table.to_sql(target_table,
                         conn,
                         schema=schema)
    
if __name__ == '__main__':

    # welcome message
    parser = argparse.ArgumentParser(description='This is the demandlib ' +
        'applied in the open_eGo project.')

    parser.add_argument('-t', '--table', nargs=1, help='Database table ' +
        'with input data', default='rli_deu_lastgebiete')
    parser.add_argument('-s', '--schema', nargs=1, help='Database schema',
                        default='orig_geo_rli')
    parser.add_argument('-tt', '--target-table', nargs=1, help='Database ' +
        'table for results data containing peak loads', default=None)
    parser.add_argument('--dummy', dest='dummy', action='store_true',
                        help='If set, dummy data is applied to annual ' +
                        'consumption.', default=False)
    args = parser.parse_args()

    # unpack lists
    if isinstance(args.schema, list):
        args.schema = args.schema[0]

    if isinstance(args.table, list):
        args.table = args.table[0]
    
    
    peak_load_table(args.schema, args.table, args.target_table, args.dummy)