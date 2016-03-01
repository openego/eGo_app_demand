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
    '''Calculates SLP based on input data from oedb'''

    # get engine for database connection
    conn = db.connection(db_section='oedb')

    # different modi are foreseen
    # a) lastgebiete: iterate over whole table, calucalate slp for each row
    # (a row represents a lastgebiet) and write new results table with lgid as
    # foreign key for reference
    # b) calculate slp for a specific location based on
    #  i) geo location (lat, lon)
    #  ii) nuts code or similar
    if mode == 'lastgebiete':
        # retrieve table with processed input data
        input_table = pd.read_sql_table(table, conn, schema=schema)
        print(input_table)
    
        year = 2011



#    ann_el_demand_per_sector = [
#        {'ann_el_demand': 1000,
#         'selp_type': 'h0'},
#        {'ann_el_demand': 0,
#         'selp_type': 'g0'},
#        {'ann_el_demand': 0,
#         'selp_type': 'i0'}]
#    
#    # ############################################################################
#    # Create demand object and relevant bus
#    # ############################################################################
#    
#    # Example 1: Calculate profile with annual electric demand per sector is known
#    
#    bel = Bus(uid="bel",
#              type="el",
#              excess=True)
#    
#    demand = sink.Simple(uid="demand", inputs=[bel])
#    helpers.call_demandlib(demand,
#                           method='calculate_profile',
#                           year=year,
#                           ann_el_demand_per_sector=ann_el_demand_per_sector)
#    
#    #print(demand.val, demand.val.sum())

if __name__ == '__main__':
    # welcome message
    parser = argparse.ArgumentParser(description='This is the demandlib ' +
        'applied in the open_eGo project.' +
        'Exemplary ')
    
    parser.add_argument('mode', help='Currently only "lastgebiete" is a  ' +
        'input!')
    parser.add_argument('-t', '--table', nargs=1, help='Database table ' +
        'with input data', default='osm_deu_polygon_lastgebiet_100_spf')
    parser.add_argument('-s', '--schema', nargs=1, help='Database schema',
                        default='calc_demand')
    args = parser.parse_args()
    
    calculate_slp(args.mode, args.schema, args.table)