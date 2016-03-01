#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
logging.getLogger().setLevel(logging.WARNING)

from oemof.core.network.entities import Bus
from oemof.core.network.entities.components import sinks as sink
from oemof.tools import helpers
from oemof import db
import pandas as pd
import argparse


def calculate_slp(mode, schema, table):
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

    # get engine for database connection
    conn = db.connection(db_section='oedb')
    
    # dummy annual demand data in GWh/a
    dummy_demand = {'residential': 0,
                    'retail': 0.001,
                    'industrial': 0,
                    'agricultural': 0}
    # define data year
    # TODO: in the future get this from somewhere else                    
    year = 2015
                    
    # initiate demandlib instances
    bel = Bus(uid="bel",
              type="el",
              excess=True)
    
    demand = sink.Simple(uid="demand", inputs=[bel])
    helpers.call_demandlib(demand,
                           method='calculate_profile',
                           year=year,
                           ann_el_demand_per_sector=[
                                {'ann_el_demand': (
                                    dummy_demand['residential'] * 1e6),
                                 'selp_type': 'h0'},
                                {'ann_el_demand': dummy_demand['retail'] * 1e6,
                                 'selp_type': 'g0'},
                                {'ann_el_demand': (
                                    dummy_demand['industrial'] * 1e6),
                                 'selp_type': 'i0'}])
                                 
    print(demand.val.loc['2015-01-13'], demand.val.sum())
#    demand.val.loc['2015-01-13'].to_csv('slp_20150113.csv')
    
    # different modi are foreseen
    # a) lastgebiete: iterate over whole table, calucalate slp for each row
    # (a row represents a lastgebiet) and write new results table with lgid as
    # foreign key for reference
    # b) calculate slp for a specific location based on
    #  i) geo location (lat, lon)
    #  ii) nuts code or similar
    
    # TODO: will be used when annual power demand is in database table
    if mode == 'lastgebiete':
        
        # retrieve table with processed input data
        input_table = pd.read_sql_table(table, conn, schema=schema)
    
        year = 2011



if __name__ == '__main__':
    # welcome message
    parser = argparse.ArgumentParser(description='This is the demandlib ' +
        'applied in the open_eGo project.')
    
    parser.add_argument('mode', help='Currently only "lastgebiete" is a  ' +
        'input!')
    parser.add_argument('-t', '--table', nargs=1, help='Database table ' +
        'with input data', default='osm_deu_polygon_lastgebiet_100_spf')
    parser.add_argument('-s', '--schema', nargs=1, help='Database schema',
                        default='calc_demand')
    args = parser.parse_args()
    
    calculate_slp(args.mode, args.schema, args.table)