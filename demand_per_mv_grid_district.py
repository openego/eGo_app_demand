#!/usr/bin/env python3

from demandlib import bdew as bdew, particular_profiles as profiles
from egoio.db_tables.calc_ego_loads import EgoDeuConsumptionArea as orm_loads,\
    EgoDemandPerTransitionPoint as orm_demand
from oemof import db
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from workalendar.europe import Germany
import pandas as pd
from datetime import time as settime

year = 2013

columns_names = {'h0': 'residential',
                 'g0': 'retail',
                 'i0': 'industrial',
                 'l0': 'agricultural'}

inv_columns_names = {v: k for k, v in columns_names.items()}

# The following dictionary is create by "workalendar"
# pip3 install workalendar

cal = Germany()
holidays = dict(cal.holidays(2010))

# Alternatively, define holidays manually
# holidays = {
#     datetime.date(2010, 5, 24): 'Whit Monday',
#     datetime.date(2010, 4, 5): 'Easter Monday',
#     datetime.date(2010, 5, 13): 'Ascension Thursday',
#     datetime.date(2010, 1, 1): 'New year',
#     datetime.date(2010, 10, 3): 'Day of German Unity',
#     datetime.date(2010, 12, 25): 'Christmas Day',
#     datetime.date(2010, 5, 1): 'Labour Day',
#     datetime.date(2010, 4, 2): 'Good Friday',
#     datetime.date(2010, 12, 26): 'Second Christmas Day'}

# retrieve sectoral demand from oedb
conn = db.connection(section='oedb')
Session = sessionmaker(bind=conn)
session = Session()

query_demand = session.query(orm_loads.subst_id,
                             func.sum(orm_loads.sector_consumption_residential).\
                             label('residential'),
                             func.sum(orm_loads.sector_consumption_retail).label('retail'),
                             func.sum(orm_loads.sector_consumption_industrial).\
                             label('industrial'),
                             func.sum(orm_loads.sector_consumption_agricultural).\
                             label('agricultural')).\
                             group_by(orm_loads.subst_id)

annual_demand_df = pd.read_sql_query(
    query_demand.statement, session.bind, index_col='subst_id').fillna(0)

# rename columns according to demandlib definitions
annual_demand_df.rename(columns=inv_columns_names, inplace=True)

# Delete current content from table
session.query(orm_demand).delete()

# iterate over substation retrieving sectoral demand at each of it
for it, row in annual_demand_df.iterrows():
    # read standard load profiles
    e_slp = bdew.ElecSlp(year, holidays=holidays)

    # multiply given annual demand with timeseries
    elec_demand = e_slp.get_profile(row.to_dict())

    # Add the slp for the industrial group
    ilp = profiles.IndustrialLoadProfile(e_slp.date_time_index, holidays=holidays)

    # Beginning and end of workday, weekdays and weekend days, and scaling factors
    # by default
    elec_demand['i0'] = ilp.simple_profile(
        row['i0'],
        am=settime(6, 0, 0),
        pm=settime(22, 0, 0),
        profile_factors=
            {'week': {'day': 0.8, 'night': 0.6},
            'weekend': {'day': 0.6, 'night': 0.6}})

    # Resample 15-minute values to hourly values and sum across sectors
    elec_demand = elec_demand.resample('H').mean().sum(axis=1)

    # Write to database
    demand2db = orm_demand(id=it, demand=elec_demand.tolist())
    session.add(demand2db)

    session.commit()

# orm_demand.__table__.create(conn)

