import pandas as pd
from argparse import ArgumentParser, ArgumentTypeError
from lib import cmdLineArgs, varsModule
import os
from lib import varHelpers
import operator
import numpy as np
from common import stringHelpers
import json
from lib.jsonWriter import roundPercent, writeJSON
import time
from collections import OrderedDict
import sys
import warnings
import copy

'''
This script takes uses the data in a YouthTruth report production directory to create a "synthesis report", which gives a bird's eye view of the data 
in all of a given client's reports. It summarizes a client's data and highlights trends across stakeholder groups. It outputs one school district-level 
report, a report for each school in the district, and one report for any 2 or more YouthTruth schools considered a multi-level school. These reports are 
contained in one JSON file which can be uploaded to the YouthTruth online reporting system and delivered to clients.
'''

parser = ArgumentParser()
parser.add_argument('-c', '--client_dir', metavar = 'client', help = 'top level report production dir for client to create a synthesis report for.', required = True)
parser.add_argument('-r', '--current_round', help = 'current round for your client', required = True)
parser.add_argument('-o', '--outDir', metavar = 'outDir', help = 'Use this if you want to write the synthesis report json somewhere other than the client directory you entered for -c.', required = False)
parser.add_argument('-t', '--testing', help = 'names file with testing and writes over other testing file if in same outdir.', action = 'store_true', required = False)
parser.add_argument('-d', '--district_report_only', help = 'this arg will make the script only produce a district-level report', action = 'store_true', required = False)
parser.add_argument('-m', '--multi_dict', metavar = 'multi_dict', help = "Use this argument if you want to create multilevel school reports but for some reason the multi_dict isn't in the client's survey admin dir. Point directly to file, not just dir." , required = False)

def grab_factor_names(client_dir, product_levels_list):
    #create a list of dictionaries where each dictionary has the factor variable name and the factor display name for every factor in the necessary reports
    factor_dict = {}
    factor_dict_by_product = {}
    for product in next(os.walk(client_dir))[1]:
        if product in product_levels_list:
            core_vars_path = os.path.abspath(os.path.join(client_dir, "..", "..", 'data/{product_level}/coreVars.py'.format(product_level=product.upper())))
            core_vars = varHelpers.importModule(core_vars_path, 'coreVars')
            for factor in core_vars.factors['execsum']:
                factor_dict[factor[0]] = factor[1]
            factor_dict_by_product[product] = factor_dict
            factor_dict = {}
    return factor_dict_by_product

def create_empty_dfs(dicts):
    #make empty dfs from dicts
    dfs = {}
    for key, value in dicts.items():
        dfs[key] = pd.DataFrame(data = value)
    return dfs

def create_empty_bar_dicts(dicts):
    #create item level bar dicts
    bar_dicts = {}
    for key, value in dicts.items():
        bar_dicts[key] = create_bar_dict(value)

    return bar_dicts

def make_theme_mapping_tables(common_factors):
    #cut things from common_factors_df to create theme mapping tables
    stakeholder_group = ['Student', 'Family', 'Staff']
    eng_theme = common_factors[common_factors['Survey Theme'] == 'Engagement'].drop(columns='Survey Theme')
    cult_theme = common_factors[common_factors['Survey Theme'] == 'Culture'].drop(columns='Survey Theme')
    rel_theme = common_factors[common_factors['Survey Theme'] == 'Relationships'].drop(columns='Survey Theme')
    cult_theme['Group'] = stakeholder_group
    rel_theme['Group'] = stakeholder_group
    return eng_theme, cult_theme, rel_theme

def create_all_factors_df(factor_dict_by_product, es_ose_ordered_factors_list, ms_ose_ordered_factors_list, hs_ose_ordered_factors_list,
    es_fam_ordered_factors_list, ms_fam_ordered_factors_list, hs_fam_ordered_factors_list, es_sta_ordered_factors_list,
    ms_sta_ordered_factors_list, hs_sta_ordered_factors_list):
    #create empty dfs with correct columns and varnames for each product_level for district report
    all_factors = pd.DataFrame(columns = ['Group', 'Survey Theme', 'Elementary', 'es_trend', 'Middle', 'ms_trend', 'High', 'hs_trend'],index=range(0,17))
    all_factors.iloc[:,0] = ['Student', '', '', '', '', '', '','Family', '', '', '', '', '', 'Staff', '', '', '']
    all_factors.iloc[:,1] = ['Engagement', 'Academic Rigor', 'Relationships', 'Culture', 'Belonging & Peer Collaboration', 'Instructional Methods', 
    'College & Career Readiness', 'Engagement', 'Relationships', 'Culture', 'Communication & Feedback', 'Resources', 'School Safety', 'Engagement', 
    'Relationships', 'Culture', 'Professional Development & Support']
    all_factors.iloc[0:len(es_ose_ordered_factors_list),2] = es_ose_ordered_factors_list
    all_factors.iloc[0:len(ms_ose_ordered_factors_list),4] = ms_ose_ordered_factors_list
    all_factors.iloc[0:len(hs_ose_ordered_factors_list),6] = hs_ose_ordered_factors_list
    all_factors.iloc[len(es_ose_ordered_factors_list):len(es_fam_ordered_factors_list)+len(es_ose_ordered_factors_list),2] = es_fam_ordered_factors_list
    all_factors.iloc[len(ms_ose_ordered_factors_list):len(ms_fam_ordered_factors_list)+len(ms_ose_ordered_factors_list),4] = ms_fam_ordered_factors_list
    all_factors.iloc[len(hs_ose_ordered_factors_list):len(hs_fam_ordered_factors_list)+len(hs_ose_ordered_factors_list),6] = hs_fam_ordered_factors_list
    all_factors.iloc[-len(es_sta_ordered_factors_list):,2] = es_sta_ordered_factors_list
    all_factors.iloc[-len(ms_sta_ordered_factors_list):,4] = ms_sta_ordered_factors_list
    all_factors.iloc[-len(hs_sta_ordered_factors_list):,6] = hs_sta_ordered_factors_list
    return all_factors

def create_all_factors_df_school(factor_dict_by_product, school_es_ose_ordered_factors_list, school_ms_ose_ordered_factors_list, school_hs_ose_ordered_factors_list,
    school_es_fam_ordered_factors_list, school_ms_fam_ordered_factors_list, school_hs_fam_ordered_factors_list, school_es_sta_ordered_factors_list,
    school_ms_sta_ordered_factors_list, school_hs_sta_ordered_factors_list):
    #create empty dfs with correct columns and varnames for each product_level for school reports
    school_es_all_factors = pd.DataFrame(columns = ['Survey Theme', 'Student', 'ose_trend', 'Family', 'fam_trend', 'Staff', 'sta_trend'],index=range(0,11))
    school_es_all_factors.iloc[:,0] = ['Engagement', 'Relationships', 'Culture', 'Academic Rigor', 'Belonging & Peer Collaboration', 'Instructional Methods', 
    'College & Career Readiness', 'Communication & Feedback', 'Resources', 'School Safety','Professional Development & Support']
    school_ms_all_factors = school_es_all_factors.copy(deep = True)
    school_hs_all_factors = school_es_all_factors.copy(deep = True)
    
    school_es_all_factors.iloc[0:len(school_es_ose_ordered_factors_list), 1] = school_es_ose_ordered_factors_list
    school_es_all_factors.iloc[0:len(school_es_fam_ordered_factors_list), 3] = school_es_fam_ordered_factors_list
    school_es_all_factors.iloc[0:len(school_es_sta_ordered_factors_list), 5] = school_es_sta_ordered_factors_list

    school_ms_all_factors.iloc[0:len(school_ms_ose_ordered_factors_list), 1] = school_ms_ose_ordered_factors_list
    school_ms_all_factors.iloc[0:len(school_ms_fam_ordered_factors_list), 3] = school_ms_fam_ordered_factors_list
    school_ms_all_factors.iloc[0:len(school_ms_sta_ordered_factors_list), 5] = school_ms_sta_ordered_factors_list
    
    school_hs_all_factors.iloc[0:len(school_hs_ose_ordered_factors_list), 1] = school_hs_ose_ordered_factors_list
    school_hs_all_factors.iloc[0:len(school_hs_fam_ordered_factors_list), 3] = school_hs_fam_ordered_factors_list
    school_hs_all_factors.iloc[0:len(school_hs_sta_ordered_factors_list), 5] = school_hs_sta_ordered_factors_list
    
    return school_es_all_factors, school_ms_all_factors, school_hs_all_factors

def get_schools_list(client_dir, product_level, client, current_round, multilevel_nameStems = False):
    #get list of nameStems for prod-level from agg/pct[genTarget]
    all_school_list = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'data', 'schoolMeta')
    district_school_list = all_school_list[(all_school_list['ClientName'] == client) & (all_school_list['round'] == current_round)]
    if district_school_list.empty:
        district_school_list = all_school_list[(all_school_list['genTarget'] == client) & (all_school_list['round'] == current_round)]
    if multilevel_nameStems:
        all_school_list = all_school_list['genTarget'].tolist()
        school_list = list(set(all_school_list) & set(multilevel_nameStems))
    else:
        if not district_school_list.empty:
            school_list = list(set(district_school_list.loc[:,'genTarget']))
        else:
            sys.exit("Couldn't find rows in {}/data/schoolMeta for this client and the given round.".format(product_level))
    return school_list


def read_in_csv(client_dir, client_dir_path, directory, csv_name):
    #read in cyan csvs and print warning if none is found
    if csv_name =='pct':
        #MDK: if we're looking for a pct file and it's not in top level agg then assume we're dealing with one school and get pct from school level agg
        #This also seems messy. 
        try:
            csv = pd.read_csv('{product_level}/{directory}/{csv_name}.csv'.format(product_level = client_dir_path, directory = directory, csv_name = csv_name))
        except FileNotFoundError:
            pass
            #MDK is below a useful warning? Commented it out bc of school-level stuff
            #print("Only 1 school in {product_level}. If that's not right check why there's no pct csv in {product_level}/{directory}.".format(product_level = client_dir.split('/')[2], directory=directory, csv_name=csv_name))
            pct_path = find('pct.csv', client_dir)
            csv = pd.read_csv(pct_path)
    else:
        try:
            csv = pd.read_csv('{product_level}/{directory}/{csv_name}.csv'.format(product_level = client_dir_path, directory = directory, csv_name = csv_name))
        except FileNotFoundError:
            print("\nNot finding a {csv_name} file in {product_level}/{directory}. Make sure CYAN has been run completely.".format(product_level = client_dir, directory = directory, csv_name = csv_name))
            csv = pd.DataFrame()
            raise
    return csv

def find(name, path):
    #search dir for a file and return the path to that file
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)   

def create_bar_dict(var_dict):
    #create dictionaries with variable names to create bar charts
    bar_dict = {
    'OSE_ES': var_dict['Elementary'][0], 'OSE_MS': var_dict['Middle'][0], 'OSE_HS': var_dict['High'][0], 
    'FAM_ES': var_dict['Elementary'][1], 'FAM_MS': var_dict['Middle'][1], 'FAM_HS': var_dict['High'][1],
    'STA_ES': var_dict['Elementary'][2], 'STA_MS': var_dict['Middle'][2], 'STA_HS': var_dict['High'][2]}
    return bar_dict

def add_trend_col(df, rnd_dict, column_name):
    #use rnd_dict to create a new col 'trend' in dfs
    for index, row in df.iterrows():
        for key, value in rnd_dict.items():
            if value[0] == str(row[column_name]).split(':')[-1]: 
                trend = key
        df.at[index, 'trend'] = trend
    return df

def determine_quartile(percentile):
    #given a percentile return a quartile 1-4
    if np.isnan(percentile):
         quartile = np.nan
    if percentile >= 75:
        quartile = 1
    elif 50 <= percentile < 75:
        quartile = 2
    elif 25 <= percentile < 50:
        quartile = 3
    elif percentile < 25:
        quartile = 4
    return quartile

def make_rnd_dict(district_mean, district_percentile, district_percent_pos, round_meta, product_level):
    #make a dict of rounds the client has data for with one being current and the most recent past being past
    rnd_dict = {}
    new_rnd_dict = {}
    if (district_mean.empty or district_percentile.empty or district_percent_pos.empty):
        print('Oops! No rows have the cohort name or nameStem. {} will be blank. Check cohort name.\n'.format(product_level))
    if not (district_mean.empty & district_percentile.empty & district_percent_pos.empty):
        #use round_meta to 1. add a column called trend with current, past, or old. 2. drop rows that are old
        #create dictionary of rounds for client
        rnd_list = district_mean.loc[:,'target'].tolist()
        rnd_list = [rnd.split(":")[-1] for rnd in rnd_list]
        for rnd in rnd_list:
            try:
                rnd_dict[int(round_meta.loc[round_meta['rnd'] == rnd,'RoundID'])] = (rnd, round_meta.loc[round_meta['rnd'] == rnd, "SurveyPeriod"].values[0])
            except TypeError:
                print("Oops! Rounds don't seem to match up across product/levels. Make sure this client dir only has data from the most recent round.\n")
                raise
        year = 0
        for key, value in list(rnd_dict.items()):
            new_rnd_dict[year] = rnd_dict.pop(max(rnd_dict))
            year +=1
    return new_rnd_dict

def add_trend_data_to_dfs(mean_df, percentile_df, percent_pos_df, rnd_dict):
    #use add_trend_col to add trend info to dfs and drop old rounds
    mean_df = add_trend_col(mean_df, rnd_dict, 'target')
    percentile_df = add_trend_col(percentile_df, rnd_dict, 'target')
    percent_pos_df = add_trend_col(percent_pos_df, rnd_dict, 'target')

    #drop old rounds
    mean_df = mean_df[mean_df.trend != 'old']
    mean_df = mean_df.reset_index(drop=True)
    percentile_df = percentile_df[percentile_df.trend != 'old']
    percentile_df = percentile_df.reset_index(drop = True)
    percent_pos_df = percent_pos_df[percent_pos_df.trend != 'old']
    percent_pos_df = percent_pos_df.reset_index(drop = True)
    return mean_df, percentile_df, percent_pos_df

def fill_in_df(product_level, df, mean_df, percentile_df, percent_pos_df, level_dict, trend_dict, mean = False):
    #fills in values in premade dfs matching column names in cyan dfs on variable names in cells of df 
    for column in mean_df.columns:
        for index, row in df.iterrows():
            if column == df.loc[index, level_dict[product_level.split('_')[1].lower()]]:
                percentile = round(float(percentile_df.loc[percentile_df['trend'] == 0, column]), 2)
                quartile = determine_quartile(percentile)
                if len(percentile_df) > 1:
                    last_percentile = round(float(percentile_df.loc[percentile_df['trend'] == 1, column]), 2)
                    last_percent_pos = roundPercent(float(percent_pos_df.loc[percent_pos_df['trend'] == 1, column]))
                    last_abs_score = round(float(mean_df.loc[mean_df['trend'] == 1, column]), 2)
                else:
                    last_percentile = False
                    last_percent_pos = False
                    last_abs_score = False
                if mean:
                    abs_score = round(float(mean_df.loc[mean_df['trend'] == 0, column]), 2) 
                    trend, difference = determine_trend(abs_score, last_abs_score)
                    df.loc[index, level_dict[product_level.split('_')[1].lower()]] = [abs_score, quartile]
                    df.loc[index, trend_dict[product_level.split('_')[1].lower()]] = [trend, difference]
                else:
                    percent_pos = roundPercent(float(percent_pos_df.loc[percent_pos_df['trend'] == 0, column]))
                    trend, difference = determine_trend(percent_pos, last_percent_pos)
                    if np.isnan(percentile):
                        df.loc[index, product_dict[product_level.split('_')[0]]] = np.nan
                        df.loc[index, trend_dict[product_level.split('_')[0]]] = np.nan
                    else:
                        df.loc[index, level_dict[product_level.split('_')[1].lower()]] = ['{percent_pos}%'.format(percent_pos = percent_pos), quartile]
                        df.loc[index, trend_dict[product_level.split('_')[1].lower()]] = [trend, difference]
    return df

def schools_fill_in_df(product_level, df, mean_df, percentile_df, percent_pos_df, level_dict, school_trend_dict, product_dict, mean = False):
    #fills in values in premade dfs matching column names in cyan dfs on variable names in cells of df 
    for column in mean_df.columns:
        for index, row in df.iterrows():
            if column == df.loc[index, product_dict[product_level.split('_')[0]]]:
                percentile = round(float(percentile_df.loc[percentile_df['trend'] == 0, column]), 2)
                quartile = determine_quartile(percentile)
                if len(percentile_df) > 1:
                    last_percentile = round(float(percentile_df.loc[percentile_df['trend'] == 1, column]), 2)
                    last_percent_pos = roundPercent(float(percent_pos_df.loc[percent_pos_df['trend'] == 1, column]))
                    last_abs_score = round(float(mean_df.loc[mean_df['trend'] == 1, column]), 2)
                else:
                    last_percentile = False
                    last_percent_pos = False
                    last_abs_score = False
                if mean:
                    abs_score = round(float(mean_df.loc[mean_df['trend'] == 0, column]), 2) 
                    trend, difference = determine_trend(abs_score, last_abs_score)
                    df.loc[index, product_dict[product_level.split('_')[0]]] = [abs_score, quartile]
                    df.loc[index, school_trend_dict[product_level.split('_')[0]]] = [trend, difference]
                else:
                    percent_pos = roundPercent(float(percent_pos_df.loc[percent_pos_df['trend'] == 0, column]))
                    trend, difference = determine_trend(percent_pos, last_percent_pos)
                    if np.isnan(percentile):
                        df.loc[index, product_dict[product_level.split('_')[0]]] = np.nan
                        df.loc[index, school_trend_dict[product_level.split('_')[0]]] = np.nan
                    else:  
                        df.loc[index, product_dict[product_level.split('_')[0]]] = ['{percent_pos}%'.format(percent_pos = percent_pos), quartile]
                        df.loc[index, school_trend_dict[product_level.split('_')[0]]] = [trend, difference]
    return df

def fill_in_bar_dict(bar_dict, product_level, district_percent_pos, rnd_dict):
    #similar too fill_in_df but for bar_dicts with data for bar charts. Difference is it adds multiple years of trend data
    percent_pos_list = []
    if bar_dict[product_level]:
        for k in rnd_dict.keys():
            percent_pos = roundPercent(float(district_percent_pos.loc[district_percent_pos['trend'] == float(k), bar_dict[product_level]]))
            if percent_pos == -1000:
                percent_pos = 'N/A'
            percent_pos_list.append(percent_pos)
        bar_dict[product_level] = percent_pos_list
    return bar_dict

def schools_fill_in_bar_dict(school, bar_dict, product_level, school_percent_pos, rnd_dict, schools_nameStems_dict, product_dict):
    #fills in school bar dicts. Very similar to fill_in_bar_dict
    #MDK improvement here would be to merge this with fill_in_bar_dict and generalize so it would work with both district and school bar dicts
    percent_pos_list = []
    if product_level in schools_nameStems_dict[school] and product_level in bar_dict.keys():
        for k in rnd_dict.keys():
            percent_pos = roundPercent(float(school_percent_pos.loc[school_percent_pos['trend'] == float(k), bar_dict[product_level]]))
            if percent_pos == -1000:
                percent_pos = 'N/A'
            percent_pos_list.append(percent_pos)
        bar_dict[product_level] = percent_pos_list
    return bar_dict

def determine_trend(score, last_score = False):
    #rule for creating trend data arrow
    if last_score == False or last_score == -1000 or score == -1000:
        trend = 3
        difference = ''
    else:
        if score > last_score:
            trend = 1
            difference = round(score - last_score, 2)
        elif last_score > score:
            trend = 2
            difference = round(last_score - score, 2)
        else:
            trend = 3
            difference = ''
    return trend, difference

def format_number(value, quartile = False):
    #rule for colouring numbers
    colour_dict = {1: '#0D47A1', 2: '#2e9fd0', 3: '#f67b33', 4: '#e62e00'}
    if quartile:
        formatted = '<span style =\"text-align:left; color:{colour};font-weight:bold\">{value}</span>'.format(value=value, colour=colour_dict[quartile])
    else:
        formatted = value
    return formatted

def create_arrow(trend_value):
    #take trend value 1, 2, or nothing and create appropriate arrow
    up = '&#8593'
    down = '&#8595'
    empty = '&nbsp;'
    if trend_value == 1:
        arrow = up
    elif trend_value == 2:
        arrow = down
    else:
        arrow = empty
    return arrow

def gen_rr_table(product_level, all_count, school_meta, rnd_dict, target, school_list, school = False):
    #return response count and response rate for each product level passed
    rr_table_row = {}
    responses = 0
    for school in school_list:
        school_count_row = all_count[(all_count['genTarget'] == school) & (all_count['target'].str.contains(rnd_dict[0][0]))]
        if not school_count_row.empty:
            school_count = int(school_count_row["total"])
            responses += school_count
            school_meta_row = school_meta[(school_meta['genTarget'] == school) & (school_meta['current'] == 1)]
            try:
                school_denom = int(school_meta_row['respTarget']) #Response rates denominator 
                school_rate = roundPercent(np.divide(school_count, school_denom))
            except ValueError:
                print("No response rate denom in School Meta. Using N/A's.")
                school_denom = 'N/A'
                school_rate = 'N/A'
            school_name = school_meta_row['SchoolName'].values[0]
            nameStem = school_meta_row['genTarget'].values[0]
            rr_table_row[school_name] = [school_denom, school_count, school_rate, nameStem]
    rr_table = pd.DataFrame.from_dict(rr_table_row, orient = 'index', columns = ["Survey Population", "Number of Responses Received", "Response Rate", "nameStem"])
    rr_table.index.name = 'School Name'
    rr_table = rr_table.reset_index()
    #add total row
    if 'N/A' in rr_table['Survey Population'].unique():
        total_denom = 'N/A'
    else:
        total_denom = rr_table["Survey Population"].sum()
    total_count = rr_table['Number of Responses Received'].sum()
    if total_denom == 'N/A':
        average_rate = 'N/A'
    else:
        average_rate = roundPercent(np.divide(total_count, total_denom))
    
    total_row = pd.DataFrame([['Total', total_denom, total_count, average_rate, "N/A"]], columns = list(("School Name", "Survey Population", "Number of Responses Received", "Response Rate", "nameStem")))
    rr_table = rr_table.append(total_row, ignore_index = True)

    #sort out types
    rr_table['Survey Population'] = rr_table['Survey Population'].astype(str)
    rr_table['Number of Responses Received'] = rr_table['Number of Responses Received'].astype(str)
    rr_table['Response Rate'] = rr_table['Response Rate'].astype(str) + '%'
    return rr_table, responses

def deal_with_trend_data_in_bars(bar_dict, level, rnd_dict, level_dict):  
    #adds trend data bars to bar charts
    bars_list = []
    
    #add n/as where number of rounds differ for different product levels
    for key, value in bar_dict.items():
        if isinstance(value, list) and len(value) < len(rnd_dict):
            difference = len(rnd_dict) - len(value)
            n_as = ['N/A'] * difference
            value.extend(n_as)

    for key, value in rnd_dict.items():
        bar = dict(name = '{level} - {round}'.format(level = level_dict[level.lower()], round = value[1]), data = [
            bar_dict['OSE_{}'.format(level)][key] if not isinstance(bar_dict['OSE_{}'.format(level)], str) else 'N/A',
            bar_dict['FAM_{}'.format(level)][key] if not isinstance(bar_dict['FAM_{}'.format(level)], str) else 'N/A',
            bar_dict['STA_{}'.format(level)][key] if not isinstance(bar_dict['STA_{}'.format(level)], str) else 'N/A'])
        bars_list.append(bar)
    bars_dict = dict(name = level_dict[level.lower()], series = bars_list)    
    return bars_dict

def read_in_multi_dict(path):
    #read in multi_dict file
    try:
        with open('{path_to_file}'.format(path_to_file = multi_dict_path)) as f:
            multi_dict = json.load(f)
        print('\nFound multi_dict json. Please make sure all schools listed in this dict should get combined school-level synthesis reports.')
    except FileNotFoundError:
        print("\nCANNOT FIND MULTI_DICT JSON to determine if there are any multilevel schools. Check if the client's survey admin folder doesn't have the same client name as their cyan folder or if that survey admin dir isn't where it usually is. For now assuming there are no multi-level schools.")
        multi_dict = False
        pass
    return multi_dict

def create_multilevel_school_report(combined_school, school_list, variables, schools_full_names_dict, schools_nameStems_dict, client_dir, current_round, multilevel_dfs, multilevel_bar_dicts, factor_dict_by_product):
    #create multilevel school report based on what is in the multi_dict. This function runs through the normal steps that are called during creation of a district report. It returns a nameStem_list so that these schools can be excluded from makinig normal school reports.
    print('\nMaking a multilevel report for {school_name}'.format(school_name = combined_school))
    nameStem_list = []
    for school_name in school_list:
        try:
            nameStem_list.append(invert_dict(schools_full_names_dict)[school_name])
        except KeyError:
            print("\nCannot find {school_name} in the CYAN data. If this is because the school's name in the multi_dict json doesn't match the name in CYAN, please edit the multi_dict json so that they match and rerun.\n".format(school_name = school_name))
            pass
    if len(nameStem_list) < 2:
        print("\nSkipping this multilevel school report because there are fewer than 2 schools listed that match the CYAN data.")
        multilevel_school_report = ''
        nameStem_list = []
    else:
        multilevel_dfs, multilevel_bar_dicts, rr_dict, rnd_dict, total_responses, nameStems_dict, school_meta = fill_in_data(multilevel_dfs, multilevel_bar_dicts, factor_dict_by_product, variables, client_dir, current_round, nameStem_list, schools_nameStems_dict)
        rr_dict['Total'] = gen_total_rr_df(rr_dict)
        multilevel_dfs = deal_with_nas_in_dfs(multilevel_dfs, school = False)
        multilevel_tables = {}
        for df_name, df in multilevel_dfs.items():
            multilevel_tables[df_name] = gen_html(df)
        multilevel_tables['response_rates'] = gen_rr_html(rr_dict)
        multilevel_bars = gen_bars(multilevel_bar_dicts, rnd_dict, variables.level_dict)
        multilevel_school_report = gen_report(combined_school, multilevel_tables, multilevel_bars, rnd_dict, total_responses, school = False)
    return multilevel_school_report, nameStem_list

def create_empty_structures(variables, client_dir):
    #first step is to create empty dfs and dictionaries where the data will go. 
    #This function creates those structures in the format that they will need to be in in the json.
    factor_dict_by_product = grab_factor_names(client_dir, variables.product_levels_list)
    dfs = create_empty_dfs(variables.dicts)
    
    #district structures
    all_factors = create_all_factors_df(factor_dict_by_product, variables.es_ose_ordered_factors_list, variables.ms_ose_ordered_factors_list, variables.hs_ose_ordered_factors_list,
    variables.es_fam_ordered_factors_list, variables.ms_fam_ordered_factors_list, variables.hs_fam_ordered_factors_list, variables.es_sta_ordered_factors_list,
    variables.ms_sta_ordered_factors_list, variables.hs_sta_ordered_factors_list)
    dfs['all_factors'] = all_factors
    dfs['all_factors_pct'] = all_factors.copy()
    common_factors = all_factors[all_factors['Survey Theme'].isin(variables.common_themes)]
    dfs['common_factors'] = common_factors
    
    eng_theme, cult_theme, rel_theme = make_theme_mapping_tables(common_factors)
    dfs['eng_theme'] = eng_theme
    dfs['rel_theme'] = rel_theme
    dfs['cult_theme'] = cult_theme
    bar_dicts = create_empty_bar_dicts(variables.dicts)
    bar_dicts['eng_theme_bar'] = variables.bar_dicts['eng_theme_bar'].copy()
    bar_dicts['rel_theme_bar'] = variables.bar_dicts['rel_theme_bar'].copy()
    bar_dicts['cult_theme_bar'] = variables.bar_dicts['cult_theme_bar'].copy() 
    #school structures
    school_es_all_factors, school_ms_all_factors, school_hs_all_factors = create_all_factors_df_school(factor_dict_by_product, variables.school_es_ose_ordered_factors_list, variables.school_ms_ose_ordered_factors_list, variables.school_hs_ose_ordered_factors_list,
    variables.school_es_fam_ordered_factors_list, variables.school_ms_fam_ordered_factors_list, variables.school_hs_fam_ordered_factors_list, variables.school_es_sta_ordered_factors_list,
    variables.school_ms_sta_ordered_factors_list, variables.school_hs_sta_ordered_factors_list)
    school_dfs = create_empty_dfs(variables.school_dicts)
    school_dfs['school_es_all_factors'] = school_es_all_factors
    school_dfs['school_ms_all_factors'] = school_ms_all_factors
    school_dfs['school_hs_all_factors'] = school_hs_all_factors
    school_dfs['school_es_all_factors_pct'] = school_es_all_factors.copy()
    school_dfs['school_ms_all_factors_pct'] = school_ms_all_factors.copy()
    school_dfs['school_hs_all_factors_pct'] = school_hs_all_factors.copy()
    school_es_common_factors = school_es_all_factors[school_es_all_factors['Survey Theme'].isin(variables.common_themes)]
    school_ms_common_factors = school_ms_all_factors[school_ms_all_factors['Survey Theme'].isin(variables.common_themes)]
    school_hs_common_factors = school_hs_all_factors[school_hs_all_factors['Survey Theme'].isin(variables.common_themes)]
    school_dfs['school_es_common_factors'] = school_es_common_factors
    school_dfs['school_ms_common_factors'] = school_ms_common_factors
    school_dfs['school_hs_common_factors'] = school_hs_common_factors
    school_dfs['school_es_eng_theme'] = school_es_common_factors[school_es_common_factors['Survey Theme'] == 'Engagement']
    school_dfs['school_ms_eng_theme'] = school_ms_common_factors[school_ms_common_factors['Survey Theme'] == 'Engagement']
    school_dfs['school_hs_eng_theme'] = school_hs_common_factors[school_hs_common_factors['Survey Theme'] == 'Engagement']
    school_dfs['school_es_cult_theme'] = school_es_common_factors[school_es_common_factors['Survey Theme'] == 'Culture']
    school_dfs['school_ms_cult_theme'] = school_ms_common_factors[school_ms_common_factors['Survey Theme'] == 'Culture']
    school_dfs['school_hs_cult_theme'] = school_hs_common_factors[school_hs_common_factors['Survey Theme'] == 'Culture']
    school_dfs['school_es_rel_theme'] = school_es_common_factors[school_es_common_factors['Survey Theme'] == 'Relationships']
    school_dfs['school_ms_rel_theme'] = school_ms_common_factors[school_ms_common_factors['Survey Theme'] == 'Relationships']
    school_dfs['school_hs_rel_theme'] = school_hs_common_factors[school_hs_common_factors['Survey Theme'] == 'Relationships']


    #make school bar dicts from district bar dicts
    school_bar_dicts = {}
    #engagement
    school_bar_dicts['school_es_eng_theme_bar'] = {k: v for k, v in bar_dicts['eng_theme_bar'].items() if k.endswith('_ES')}
    school_bar_dicts['school_ms_eng_theme_bar'] = {k: v for k, v in bar_dicts['eng_theme_bar'].items() if k.endswith('_MS')}
    school_bar_dicts['school_hs_eng_theme_bar'] = {k: v for k, v in bar_dicts['eng_theme_bar'].items() if k.endswith('_HS')}

    #relationships
    school_bar_dicts['school_es_rel_theme_bar'] = {k: v for k, v in bar_dicts['rel_theme_bar'].items() if k.endswith('_ES')}
    school_bar_dicts['school_ms_rel_theme_bar'] = {k: v for k, v in bar_dicts['rel_theme_bar'].items() if k.endswith('_MS')}
    school_bar_dicts['school_hs_rel_theme_bar'] = {k: v for k, v in bar_dicts['rel_theme_bar'].items() if k.endswith('_HS')}

    #culture
    school_bar_dicts['school_es_cult_theme_bar'] = {k: v for k, v in bar_dicts['cult_theme_bar'].items() if k.endswith('_ES')}
    school_bar_dicts['school_ms_cult_theme_bar'] = {k: v for k, v in bar_dicts['cult_theme_bar'].items() if k.endswith('_MS')}
    school_bar_dicts['school_hs_cult_theme_bar'] = {k: v for k, v in bar_dicts['cult_theme_bar'].items() if k.endswith('_HS')}
    return dfs, bar_dicts, school_dfs, school_bar_dicts, factor_dict_by_product

def fill_in_data(dfs, bar_dicts, factor_dict_by_product, variables, client_dir, current_round, multilevel_nameStems = False, schools_nameStems_dict = False):
    #run through empty dfs, bar_dicts, and response rate tables and fill in data. This function also reads in csvs and generally does the bulk of the actual data work of creating a district report

    #create empty dict to store response rates data
    rr_dict = OrderedDict()
    total_responses = 0
    #create empty list to store rnd dicts in
    rnd_dict_list = []

    #create empty dict for namestems in each product level
    nameStems_dict = {}
    #do this one product - level at a time
    product_levels = []
    if multilevel_nameStems:
        for nameStem in multilevel_nameStems:
            product_levels += schools_nameStems_dict[nameStem]
    else:
        product_levels = [f.name for f in os.scandir(client_dir) if f.is_dir()]
    for product_level in product_levels:
        if product_level in variables.product_levels_list:  
            print('Found a directory for {product_level}. Running.'.format(product_level=product_level))
            client=client_dir.strip('/').split('/')[-1]
            nameStems_dict[product_level] = get_schools_list(client_dir, product_level, client, current_round, multilevel_nameStems)
            if len(nameStems_dict[product_level]) == 1:
                #MDK: if it's just one school at this product -level some things need to change. So below i'm reading in different csvs. 
                #This seems messy but couldn't think of a better way
                all_mean = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'agg', 'allmean')
                district_mean = all_mean[(all_mean['genTarget'] == nameStems_dict[product_level][0])].reset_index(drop = True)

                #get percentiles for both all factors and common factors tables
                all_percentile = read_in_csv(client_dir, os.path.join(client_dir, product_level, nameStems_dict[product_level][0]), 'agg', 'pct')
                district_percentile = all_percentile[(all_percentile['genTarget'] == nameStems_dict[product_level][0])].reset_index(drop = True)

                #get percent positives for common factors table
                all_percent_pos = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'agg', 'highprop')
                district_percent_pos = pd.DataFrame()
                for i, row in all_percent_pos.iterrows():
                    if str(row['target']).split(":")[0] == nameStems_dict[product_level][0]:
                        district_percent_pos = district_percent_pos.append(row)                
            else:
                #get means for all factors table
                all_mean = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'agg', 'allmean')
                district_mean = pd.DataFrame()
                for i, row in all_mean.iterrows():
                    if str(row['target']).split(":")[0] == client:
                        district_mean = district_mean.append(row)

                #get percentiles for both all factors and common factors tables
                all_percentile = read_in_csv(client_dir, os.path.join(client_dir, product_level, client), 'agg', 'pct')
                district_percentile = pd.DataFrame()
                for i, row in all_percentile.iterrows():
                    if str(row['target']).split(":")[0] == client:
                        district_percentile = district_percentile.append(row)

                #get percent positives for common factors table
                all_percent_pos = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'agg', 'highprop')
                district_percent_pos = pd.DataFrame()
                for i, row in all_percent_pos.iterrows():
                    if str(row['target']).split(":")[0] == client and row['type'] == 'district':
                        district_percent_pos = district_percent_pos.append(row)

            #make round dict for this product level and add to list
            round_meta = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'data', 'roundMeta')
            rnd_dict = make_rnd_dict(district_mean, district_percentile, district_percent_pos, round_meta, product_level)
            rnd_dict_list.append(rnd_dict)
            district_mean, district_percentile, district_percent_pos = add_trend_data_to_dfs(district_mean, district_percentile, district_percent_pos, rnd_dict)
 
            dfs['all_factors'] = fill_in_df(product_level, dfs['all_factors'], district_mean, district_percentile, district_percent_pos, variables.level_dict, variables.trend_dict, mean=True)
            for df in dfs.values():
                df = fill_in_df(product_level, df, district_mean, district_percentile, district_percent_pos, variables.level_dict, variables.trend_dict, mean = False)
            #fill in dicts for bar charts with percents
            for name, bar_dict in bar_dicts.items():
                bar_dict = fill_in_bar_dict(bar_dict, product_level, district_percent_pos, rnd_dict)
            #generate table with response counts and rates
            all_count = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'agg', 'allcount')
            school_meta = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'data', 'schoolMeta')

            #create response rates dataframe and add it to response rates dictionary
            rr_df, responses = gen_rr_table(product_level, all_count, school_meta, rnd_dict, client, nameStems_dict[product_level]) #Response rates
            level = variables.level_dict[product_level.split('_')[1].lower()]
            product = variables.product_dict[product_level.split('_')[0]]
            rr_dict['{level} School {product} Responses'.format(level = level, product = product)] = rr_df

            #set round dict to be the longest of the ones in the list
            max_rnd_dict_len = max(map(len, rnd_dict_list))
            max_rnd_dicts = dict(i for i in enumerate(rnd_dict_list) if len(i[-1]) == max_rnd_dict_len)
            rnd_dict = next(iter(max_rnd_dicts.values()))
        total_responses += responses
    return dfs, bar_dicts, rr_dict, rnd_dict, total_responses, nameStems_dict, school_meta

def invert_dict(old_dict):
    #make keys values and make values keys in a dict. Useful when making school reports when we need to know which product_levels a specific school has done as opposed to what schools are in a product_level for a district. 
    inverse = dict()
    for key in old_dict:
        if isinstance(old_dict[key], list):
            for item in old_dict[key]:
                if item not in inverse:
                    inverse[item] = [key] 
                else:
                    inverse[item].append(key) 
        if isinstance(old_dict[key], str):
            inverse[old_dict[key]] = key
    return inverse

def grab_school_name(school, school_meta):
    #grab full school name from schoolMeta using nameStem
    school_name = school_meta.loc[school_meta['genTarget'] == school, 'SchoolName'].values[0]
    return school_name

def schools_fill_in_data(empty_school_dfs, empty_school_bar_dicts, factor_dict_by_product, variables, client_dir, schools_nameStems_dict, rnd_dict):
    #fills in data for school reports. Reads in CYAN csvs and fills in previously empty dfs, bar_dicts, and response rate dfs. Generally does bulk of the data work nevessary for creating a school report
    #MDK improvement here would be to merge and generalize with fill_in_data function. A lot of repetitive code. 
    rr_dict = {}
    school_dfs = {}
    school_bar_dicts = {}
    school_bars = {}
    schools_full_names_dict = {}
    rnd_dict_list = []
    for school, product_levels in schools_nameStems_dict.items():
        school_dfs[school] = copy.deepcopy(empty_school_dfs)
        school_bar_dicts[school] = copy.deepcopy(empty_school_bar_dicts)
        print('Found data for {school}. Running.'.format(school=school))
        client=client_dir.strip('/').split('/')[-1]
        for product_level in product_levels:
            all_mean = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'agg', 'allmean')
            school_mean = pd.DataFrame()
            for i, row in all_mean.iterrows():
                if str(row['target']).split(":")[0] == school:
                    school_mean = school_mean.append(row)

            all_percentile = read_in_csv(client_dir, os.path.join(client_dir, product_level, school), 'agg', 'pct')
            school_percentile = pd.DataFrame()
            for i, row in all_percentile.iterrows():
                if str(row['target']).split(":")[0] == school:
                    school_percentile = school_percentile.append(row)

            all_percent_pos = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'agg', 'highprop')
            school_percent_pos = pd.DataFrame()
            for i, row in all_percent_pos.iterrows():
                if str(row['target']).split(":")[0] == school and row['type'] == 'school':
                    school_percent_pos = school_percent_pos.append(row)

            round_meta = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'data', 'roundMeta')
            rnd_dict = make_rnd_dict(school_mean, school_percentile, school_percent_pos, round_meta, product_level)
            rnd_dict_list.append(rnd_dict)
            school_mean, school_percentile, school_percent_pos = add_trend_data_to_dfs(school_mean, school_percentile, school_percent_pos, rnd_dict)
            school_dfs[school]['school_es_all_factors'] = schools_fill_in_df(product_level, school_dfs[school]['school_es_all_factors'], school_mean, school_percentile, school_percent_pos, variables.level_dict, variables.school_trend_dict, variables.product_dict, mean=True)
            school_dfs[school]['school_ms_all_factors'] = schools_fill_in_df(product_level, school_dfs[school]['school_ms_all_factors'], school_mean, school_percentile, school_percent_pos, variables.level_dict, variables.school_trend_dict, variables.product_dict, mean=True)
            school_dfs[school]['school_hs_all_factors'] = schools_fill_in_df(product_level, school_dfs[school]['school_hs_all_factors'], school_mean, school_percentile, school_percent_pos, variables.level_dict, variables.school_trend_dict, variables.product_dict, mean=True)
            for df_name, df in school_dfs[school].items():
                df = schools_fill_in_df(product_level, df, school_mean, school_percentile, school_percent_pos, variables.level_dict, variables.school_trend_dict, variables.product_dict, mean = False)
            for bar_dict in school_bar_dicts[school].values():
                bar_dict = schools_fill_in_bar_dict(school, bar_dict, product_level, school_percent_pos, rnd_dict, schools_nameStems_dict, variables.product_dict)

            school_meta = read_in_csv(client_dir, os.path.join(client_dir, product_level), 'data', 'schoolMeta')
            schools_full_names_dict[school] = grab_school_name(school, school_meta)
        #set round dict to be the longest of the ones in the list
        max_rnd_dict_len = max(map(len, rnd_dict_list))
        max_rnd_dicts = dict(i for i in enumerate(rnd_dict_list) if len(i[-1]) == max_rnd_dict_len)
        rnd_dict = next(iter(max_rnd_dicts.values()))
        #delete empty bar_dicts
        for bars in school_bar_dicts.values():
            for name in list(bars.keys()):
                if all(isinstance(value, str) for value in bars[name].values()):
                    del bars[name]
            #generate school bars and save them in dict
            school_bars[school] = school_gen_bars(school_bar_dicts[school], rnd_dict, variables.level_dict)
    return school_dfs, school_bars, schools_full_names_dict

def add_up_responses(table):
    #function that adds up columns in response rate tables to help create the total table
    #should deal with N/A's when a client hasn't given us a denominator
    
    denom = 0
    count = 0

    if 'N/A' in table['Survey Population'].unique():
        denom = np.nan
        count += int(table['Number of Responses Received'].tail(1))
    else:
        denom += int(table['Survey Population'].tail(1))
        count += int(table['Number of Responses Received'].tail(1))
    return denom, count

def gen_total_rr_df(rr_dict):
    #function that loops through all response rate tables and creates a total table
    ose_denom = 0
    ose_count = 0
    ose_rate = np.nan
    fam_denom = 0 
    fam_count = 0
    fam_rate = np.nan
    sta_denom = 0
    sta_count = 0
    sta_rate = np.nan

    for name, table in rr_dict.items():
        if 'Student' in name:
            denom, count = add_up_responses(table)
            ose_denom += denom
            ose_count += count
            ose_rate = int(roundPercent(np.divide(ose_count, ose_denom)))
        if 'Family' in name:
            denom, count = add_up_responses(table)
            fam_denom += denom
            fam_count += count
            fam_rate = int(roundPercent(np.divide(fam_count, fam_denom)))
        if 'Staff' in name:
            denom, count = add_up_responses(table)
            sta_denom += denom
            sta_count += count
            sta_rate = int(roundPercent(np.divide(sta_count, sta_denom)))

    total_rr_dict = {'Group': ['Student', 'Family', 'Staff'], "Survey Population": [ose_denom, fam_denom, sta_denom], 
    "Number of Responses Received": [ose_count, fam_count, sta_count], 
    "Response Rate": [ose_rate, fam_rate, sta_rate]}
    total_rr_df = pd.DataFrame.from_dict(total_rr_dict, dtype = int)

    #sort out types
    total_rr_df['Survey Population'] = total_rr_df['Survey Population'].astype(str)
    total_rr_df['Number of Responses Received'] = total_rr_df['Number of Responses Received'].astype(str)
    
    #change type of rate column to string and add % sign. Change back to nan if it was a nan before
    total_rr_df['Response Rate'] = total_rr_df['Response Rate'].astype(str) + '%'
    total_rr_df['Response Rate'] = total_rr_df['Response Rate'].replace('nan%', np.nan)
    return total_rr_df

def gen_school_rr_dict(rr_dict, nameStem):
    #function that loops through all response rate tables and creates each school's table
    ose_denom = 0
    ose_count = 0
    ose_rate = np.nan
    fam_denom = 0 
    fam_count = 0
    fam_rate = np.nan
    sta_denom = 0
    sta_count = 0
    sta_rate = np.nan

    for name, table in rr_dict.items():
        if 'Student' in name:
            table = table[(table['nameStem'] == nameStem)]
            if not table.empty:
                ose_count += int(table['Number of Responses Received'].tail(1))
                try:
                    ose_denom += int(table["Survey Population"].tail(1))
                    ose_rate = int(roundPercent(np.divide(ose_count, ose_denom)))
                except ValueError:
                    print("No response rate denom in School Meta for {nameStem}. Using N/A's.".format(nameStem = nameStem))
                    ose_denom = 'N/A'
                    ose_rate = 'N/A'
        if 'Family' in name:
            table = table[(table['nameStem'] == nameStem)]
            if not table.empty:
                fam_count += int(table['Number of Responses Received'].tail(1))
                try:
                    fam_denom += int(table["Survey Population"].tail(1))
                    fam_rate = int(roundPercent(np.divide(fam_count, fam_denom)))
                except ValueError:
                    print("No response rate denom in School Meta for {nameStem}. Using N/A's.".format(nameStem = nameStem))
                    fam_denom = 'N/A'
                    fam_rate = 'N/A'
        if 'Staff' in name:
            table = table[(table['nameStem'] == nameStem)]
            if not table.empty:
                sta_count += int(table['Number of Responses Received'].tail(1))
                try:
                    sta_denom += int(table["Survey Population"].tail(1))
                    sta_rate = int(roundPercent(np.divide(sta_count, sta_denom)))
                except ValueError:
                    print("No response rate denom in School Meta for {nameStem}. Using N/A's.".format(nameStem = nameStem))
                    sta_denom = 'N/A'
                    sta_rate = 'N/A'
    total_responses = (ose_count + fam_count + sta_count)
    school_rr_dict = {'Group': ['Student', 'Family', 'Staff'], "Survey Population": [ose_denom, fam_denom, sta_denom], 
    "Number of Responses Received": [ose_count, fam_count, sta_count], 
    "Response Rate": [ose_rate, fam_rate, sta_rate]}
    school_rr_df = pd.DataFrame.from_dict(school_rr_dict, dtype = int)
    
    #sort out types
    school_rr_df['Survey Population'] = school_rr_df['Survey Population'].astype(str)
    school_rr_df['Number of Responses Received'] = school_rr_df['Number of Responses Received'].astype(str)
    
    #change type of rate column to string and add % sign. Change back to nan if it was a nan before
    school_rr_df['Response Rate'] = school_rr_df['Response Rate'].astype(str) + '%'
    school_rr_df['Response Rate'] = school_rr_df['Response Rate'].replace('nan%', np.nan)
    return school_rr_df, total_responses

def deal_with_nas_in_dfs(dfs, school = True):
    #enforces different rules for na's depending on which column
    if school:
        for df in dfs.values():
            df[['Student', 'Family', 'Staff']] = df[['Student', 'Family', 'Staff']].applymap(lambda x: np.nan if isinstance(x, str) or np.all(pd.isnull(x)) else x)
            df[['ose_trend', 'fam_trend', 'sta_trend']] = df[['ose_trend', 'fam_trend', 'sta_trend']].applymap(lambda x: [3, ''] if np.all(pd.isnull(x)) else x)
    else:
        for df in dfs.values():
            df[['Elementary', 'Middle', 'High']] = df[['Elementary', 'Middle', 'High']].applymap(lambda x: np.nan if isinstance(x, str) or np.all(pd.isnull(x)) else x)
            df[['es_trend', 'ms_trend', 'hs_trend']] = df[['es_trend', 'ms_trend', 'hs_trend']].applymap(lambda x: [3, ''] if np.all(pd.isnull(x)) else x)
    return dfs

def gen_html(df, school = False):
    #takes a dataframe and generates an HTML table

    header = '<table class="reporttable">  <thead>    <tr style="color: rgb(102, 102, 102); background-color: rgb(255, 187, 128); font-weight: normal">'
    rows = [header]
    columns = []
    for col in df.columns:
        if 'trend' in col:
            columns.append('<th col width="40">''</th>')
        elif col in ['Elementary', 'Middle', 'High'] or ((col in ['Student', 'Family', 'Staff']) & (school == True)):
            columns.append('<th style="text-align:right">{}</th>'.format(col))
        else:
            columns.append('<th col width="190">{}</th>'.format(col))
    rows.append(' '.join(columns) + ' </tr>  </thead>  <tbody> ')
    rowColour = 'odd'
    for index in df.index:
        row = '<tr class="{}">'.format(rowColour)
        rVals = [df[c][index] for c in df.columns]
        cols = []
        for v in rVals:
            #MDK this is messy and seems like poor practice to just be using types like this?
            if isinstance(v, list):
                if v[0] == '-1000%':
                    cols.append('<td style="color: #808080;font-size:11px">N/A</td>')
                elif isinstance(v[0], float) or isinstance(v[0], str):
                    cols.append('<td style="text-align:left">{value}</td>'.format(value = format_number(v[0], v[1])))
                elif isinstance(v[0], int):
                    cols.append('<td style="color: #808080;font-size:11px">{arrow}&nbsp;{difference}</td>'.format(arrow = create_arrow(v[0]), difference = v[1]))
            elif isinstance(v, str):
                cols.append('<td>{}</td>'.format(stringHelpers.removeUTF(v)))
            elif np.isnan(v):
                cols.append('<td style="color: #808080;font-size:11px">N/A</td>')
        row = row + ' '.join(cols)
        row = row + ' </tr>'
        if rowColour == 'odd':
            rowColour = 'even'
        else:
            rowColour = 'odd'
        rows.append(row)
    rows.append('</tbody></table>')
    df = ' '.join(rows)
        # tables[df_name] = df
    return df

def gen_rr_html(rr_dict):
    #generate html for each response rates df and add them to tables
    rr_tables = ''
    rr_tables += '<b>{name}</b>{df}<br/>'.format(name = 'Total', df = gen_html(rr_dict['Total']))
    for df_name, df in rr_dict.items():
        if df_name != 'Total':
            if 'nameStem' in df.columns.to_list():
                df_new = df.drop('nameStem', axis = 1)
            rr_tables += '<b>{name}</b>{df}<br/>'.format(name = df_name, df = gen_html(df_new))
    return rr_tables

def gen_bars(bar_dicts, rnd_dict, level_dict):
    #function to make a bar chart from bar_dicts
    bars = {}
    for bar_name, bar_dict in bar_dicts.items():
        el = {}
        el['type'] = 'toggledBarChart'
        el['dataType'] = 'percent'
        el['categories'] = ['Student', 'Family', 'Staff']
        el['current'] = []
        el['comparative'] = []
        el['past-results'] = []
        el['cohort'] = []
        elementary = deal_with_trend_data_in_bars(bar_dict, 'ES', rnd_dict, level_dict)
        middle = deal_with_trend_data_in_bars(bar_dict, 'MS', rnd_dict, level_dict)
        high = deal_with_trend_data_in_bars(bar_dict, 'HS', rnd_dict, level_dict)
        el['segmentations'] = [elementary, middle, high]
        bars[bar_name] = el
    return bars

def school_gen_bars(school_bar_dicts, rnd_dict, level_dict):
    #makes a bar charts from school_bar_dicts
    #MDK improvement is to merge and generalize this with gen_bars
    bars = {}
    for bar_name, bar_dict in school_bar_dicts.items():
        level = list(bar_dict.keys())[0].split('_')[1]
        el = {}
        el['type'] = 'toggledBarChart'
        el['dataType'] = 'percent'
        el['categories'] = ['Student', 'Family', 'Staff']
        el['current'] = []
        el['comparative'] = []
        el['past-results'] = []
        el['cohort'] = []
        el['segmentations'] = [deal_with_trend_data_in_bars(bar_dict, level, rnd_dict, level_dict)]
        bars[convert_school_object_names(bar_name)] = el
    return bars

def drop_wrong_level_school_dfs(school_dfs, level):
    #removes dfs from school_dfs that are not from the right level
    new_school_dfs = {}
    for df_name, df in school_dfs.items():
        if df_name.split("_")[1] == level.lower():
            new_school_dfs[df_name] = df
    return new_school_dfs

def convert_school_object_names(name):
    #removes school_hs for example from beginning of df or bar name for school report objects. Converts the names to what the report template expects. Same names used in the district report.
    name = name.split("_")[2:]
    name = "_".join(name)
    print(name)
    return name

def gen_report(report_name, tables, bars, rnd_dict, total_responses, school = False):
    #put html tables into a json and write json
    colour_key = "<table align='center'; class='reporttable skinny'> <thead>  <tr style=\"font-weight:bold\"><th col width=\"80\">Color Key</th></tr></thead><tbody> <tr class='odd'> <td><span style =\"color:#e62e00;font-weight:bold\">0th-24th<br> percentile</span></td><td><span style =\"color:#f67b33;font-weight:bold\">25th-49th<br> percentile</span></td> <td><span style =\"color:#2e9fd0;font-weight:bold\">50th-74th<br> percentile</span></td> <td><span style =\"color:#0D47A1;font-weight:bold\">75th-100th<br> percentile</span></td></tr></tbody></table>"
    school_district = 'school' if school else 'district'
    tables.update({'colour_key': colour_key, 'client_name': report_name, 'total_responses': str(total_responses), 'school_district': school_district})
    elements = dict(tables = dict(type = 'textElement', substitutions = tables))
    for key, value in bars.items():
        elements[key] = value
    report = dict(name = 'Batch Title', title = '{client} - Synthesis Report - {round}'.format(client = report_name, round = rnd_dict[0][1]), elements = elements)
    return report
            
def write_json(json, client_dir, outDir = False, testing = False):
    client_name = client_dir.split("/")[-2]
    if testing:
        test = '.TESTING'
    else:
        test = ''
    fileName = 'Synthesis Report_' + client_name + test +'.json'
    if not outDir:
        outDir = client_dir
    fileName = os.path.join(outDir, fileName)
    writeJSON(json,fileName)
    print('\nsaved json as {}'.format(fileName))

if __name__ == "__main__":
    #argument and general set up
    args = parser.parse_args()
    client_dir = args.client_dir
    outDir = args.outDir
    testing = args.testing
    current_round = args.current_round
    multi_dict_path = args.multi_dict
    district_report_only = args.district_report_only
    district_name = client_dir.split("/")[-2]
    final_json = {}
    final_json['version'] = '2.0'
    final_json['reports'] = []
    vars_path =  os.path.abspath(os.path.join(client_dir, '..', '..', 'data/synthesis_report_vars.py'))
    variables = varHelpers.importModule(vars_path, 'synthesis_report_vars')

    #beginning of district report set up
    empty_dfs, empty_bar_dicts, empty_school_dfs, empty_school_bar_dicts, factor_dict_by_product = create_empty_structures(variables, client_dir)
    print('\nStarting with the district report.')
    dfs, bar_dicts, rr_dict, rnd_dict, total_responses, nameStems_dict, school_meta = fill_in_data(empty_dfs, empty_bar_dicts, factor_dict_by_product, variables, client_dir, current_round)    
    
    #this part checks if this is a one school district. If it is, this will just generate a school report and will skip the district report
    school_report_only = True
    for product_level, nameStem in nameStems_dict.items():
        if len(nameStem) > 2:
            school_report_only = False

    if school_report_only:
        print("\nOnly found 1 school for this client so skipping the district report.")
    
    #actually fill in district report data
    if not school_report_only:
        rr_dict['Total'] = gen_total_rr_df(rr_dict)
        dfs = deal_with_nas_in_dfs(dfs, school = False)
        tables = {}
        for df_name, df in dfs.items():
            tables[df_name] = gen_html(df)
        tables['response_rates'] = gen_rr_html(rr_dict)
        bars = gen_bars(bar_dicts, rnd_dict, variables.level_dict)
        district_report = gen_report(district_name, tables, bars, rnd_dict, total_responses, school = False)
        final_json['reports'].append(district_report)
    
    #school report set up
    if not district_report_only:
        schools_nameStems_dict = invert_dict(nameStems_dict)
        print('\nMoving on to school reports.')
        school_dfs_dict, school_bars, schools_full_names_dict = schools_fill_in_data(empty_school_dfs, empty_school_bar_dicts, factor_dict_by_product, variables, client_dir, schools_nameStems_dict, rnd_dict)
        school_rr_dict = {}
        school_tables = {}

        #create district-like reports for multi-level schools
        if not multi_dict_path:
            client_name = client_dir.split("/")[-2]
            multi_dict_path = os.path.abspath(os.path.join(client_dir, "..", "..", "..", "..", "..", 'YouthTruth/Survey Administration/clients/{client_name}/multi_dict.json'.format(client_name = client_name)))
        multi_dict = read_in_multi_dict(multi_dict_path)
        
        multilevel_nameStems_list = []
        if multi_dict:
            for combined_school, school_list in multi_dict.items():
                multilevel_empty_dfs = empty_dfs.copy()
                multilevel_dfs, multilevel_bar_dicts, empty_school_dfs, empty_school_bar_dicts, factor_dict_by_product = create_empty_structures(variables, client_dir)
                multilevel_school_report, nameStem_list = create_multilevel_school_report(combined_school, school_list, variables, schools_full_names_dict, schools_nameStems_dict, client_dir, current_round, multilevel_dfs, multilevel_bar_dicts, factor_dict_by_product)
                multilevel_nameStems_list += nameStem_list
                if not multilevel_school_report:
                    pass
                else:
                    final_json['reports'].append(multilevel_school_report)
        
        #finishes off school report data and appends reports to json
        for nameStem, school_dfs in school_dfs_dict.items():
            #skips multilevel schools when making normal school reports
            if nameStem in multilevel_nameStems_list:
                pass
            else:
                full_school_name = schools_full_names_dict[nameStem]
                school_dfs = deal_with_nas_in_dfs(school_dfs, school = True)
                level = schools_nameStems_dict[nameStem][0].split("_")[1]
                school_dfs = drop_wrong_level_school_dfs(school_dfs, level)
                school_tables[nameStem] = {}
                for df_name, df in school_dfs.items():
                    df_name = convert_school_object_names(df_name)
                    school_tables[nameStem][df_name] = gen_html(df, school = True)
                school_rr_dict, total_responses = gen_school_rr_dict(rr_dict, nameStem)
                school_tables[nameStem]['response_rates'] = gen_html(school_rr_dict)
                school_report = gen_report(full_school_name, school_tables[nameStem], school_bars[nameStem], rnd_dict, total_responses, school = True)
                final_json['reports'].append(school_report)

    write_json(final_json, client_dir, outDir, testing)