import pytest
import pandas as pd
import numpy as np
import json
import os,sys
import synthesis_report
from lib import varHelpers
from pandas.util.testing import assert_frame_equal
from numpy.testing import assert_approx_equal, assert_equal

'''
This script uses pytest to test the functions in synthesis_report.py. 
'''


dataDir = os.path.join(os.path.dirname(__file__), 'test_data', 'synthesis_report_test_data')

@pytest.fixture(scope='module')
def hs_all_mean():
    return pd.read_csv(os.path.join(dataDir, 'test_hs_allmean.csv'))

@pytest.fixture(scope='module')
def school_all_mean():
    return pd.read_csv(os.path.join(dataDir, 'test_school_allmean.csv'))

@pytest.fixture(scope='module')
def hs_all_percent_pos():
    return pd.read_csv(os.path.join(dataDir, 'test_hs_highprop.csv'))

@pytest.fixture(scope='module')
def school_all_percent_pos():
    return pd.read_csv(os.path.join(dataDir, 'test_school_highprop.csv'))

@pytest.fixture(scope='module')
def hs_all_percentile():
    return pd.read_csv(os.path.join(dataDir, 'test_hs_pct.csv'))

@pytest.fixture(scope='module')
def school_all_percentile():
    return pd.read_csv(os.path.join(dataDir, 'test_school_pct.csv'))

@pytest.fixture(scope='module')
def variables():
	vars_path =  os.path.join(dataDir, 'synthesis_report_vars.py')
	variables = varHelpers.importModule(vars_path, 'synthesis_report_vars')
	return variables

@pytest.fixture(scope='module')
def roundmeta():
    return pd.read_csv(os.path.join(dataDir, 'test_roundmeta.csv'))

@pytest.fixture(scope='module')
def tables():
	with open(os.path.join(dataDir, 'test_tables.json')) as json_file:
		data = json.load(json_file)
		return data

@pytest.fixture(scope='module')
def bars():
	with open(os.path.join(dataDir, 'test_bars.json')) as json_file:
		data = json.load(json_file)
		return data

@pytest.fixture(scope='module')
def common_factors():
	with open(os.path.join(dataDir, 'test_common_factors.json')) as json_file:
		data = json.load(json_file)
		return data

@pytest.fixture(scope = 'module')
def rnd_dict():
	return dict({0: ('19O', 'October 2019'), 1: ('18O', 'October 2018'), 2: ('17O', 'October 2017'), 3: ('16O', 'October 2016'), 4: ('15O', 'October 2015')})

@pytest.fixture(scope = 'module')
def school_rnd_dict():
	return dict({0: ('19N', 'November 2019'), 1: ('19F', 'February 2019')})

@pytest.fixture(scope = 'module')
def total_responses():
	return 7023

@pytest.fixture(scope = 'module')
def expectations_df():
	csv_file = pd.read_csv(os.path.join(dataDir, 'test_expectations.csv'))
	return csv_file

@pytest.fixture(scope = 'module')
def factor_dict_by_product():
	with open(os.path.join(dataDir, 'test_factor_dict_by_product.json')) as json_file:
		data = json.load(json_file)
		return data

@pytest.fixture(scope= 'module')
def schools_nameStems_dict():
	return dict(
		{'TLA Elementary': ['STA_ES', 'OSE_ES'], 
		'TLA Middle': ['OSE_MS'],
		'LV': ['STA_ES', 'OSE_ES', 'FAM_ES']}
		)

@pytest.fixture(scope = 'module')
def dfs(variables, factor_dict_by_product):
	dfs = synthesis_report.create_empty_dfs(variables.dicts)
	all_factors = synthesis_report.create_all_factors_df(factor_dict_by_product, variables.es_ose_ordered_factors_list, variables.ms_ose_ordered_factors_list, variables.hs_ose_ordered_factors_list, variables.es_fam_ordered_factors_list, variables.ms_fam_ordered_factors_list, variables.hs_fam_ordered_factors_list, variables.es_sta_ordered_factors_list, variables.ms_sta_ordered_factors_list, variables.hs_sta_ordered_factors_list)
	dfs['all_factors'] = all_factors
	dfs['all_factors_pct'] = all_factors.copy()
	common_factors = all_factors[all_factors['Survey Theme'].isin(variables.common_themes)]
	dfs['common_factors'] = common_factors
	eng_theme, cult_theme, rel_theme = synthesis_report.make_theme_mapping_tables(common_factors)
	dfs['rel_theme'] = rel_theme
	dfs['cult_theme'] = cult_theme
	return dfs

@pytest.fixture(scope = 'module')
def bar_dicts(variables):
    bar_dicts = synthesis_report.create_empty_bar_dicts(variables.dicts)
    bar_dicts['eng_theme_bar'] = variables.bar_dicts['eng_theme_bar']
    bar_dicts['rel_theme_bar'] = variables.bar_dicts['rel_theme_bar']
    bar_dicts['cult_theme_bar'] = variables.bar_dicts['cult_theme_bar']
    return bar_dicts

@pytest.fixture(scope = 'module')
def school_bar_dicts(variables, bar_dicts):
	school_bar_dicts = {}
	school_bar_dicts['school_es_eng_theme_bar'] = {k: v for k, v in bar_dicts['eng_theme_bar'].items() if k.endswith('_ES')}
	school_bar_dicts['school_es_rel_theme_bar'] = {k: v for k, v in bar_dicts['rel_theme_bar'].items() if k.endswith('_ES')}
	school_bar_dicts['school_hs_cult_theme_bar'] = {k: v for k, v in bar_dicts['cult_theme_bar'].items() if k.endswith('_ES')}
	return school_bar_dicts

@pytest.mark.parametrize('cmpRange, expVal', [
	[[574,577], '67%']
	])

def test_gen_html(expectations_df, cmpRange, expVal):
	client_dir = 'clients/Davis Joint Unified School District'
	df_new = expectations_df.drop(expectations_df.columns[0], axis=1)
	expectations_html = synthesis_report.gen_html(df_new)
	if cmpRange:
		val = expectations_html[cmpRange[0]: cmpRange[1]]
	assert_equal(val, expVal)

@pytest.mark.parametrize('subPath, cmpRange, expVal', [ 
    [['elements', 'differentcult', 'segmentations', 2, 'series', 0, 'data'], [], [80, 79, "N/A"]],
 	[['elements', 'rel_theme_bar', 'segmentations', 0, 'series', 0, 'data'], [], [80, 88, "N/A"]],
 	[['elements', 'tables', 'substitutions', 'common_factors'], [3363, 3366], '83%'],
 	[['elements', 'tables', 'substitutions', 'all_factors'], [636, 652], '&#8595&nbsp;0.03'],
 	[['elements', 'tables', 'substitutions', 'all_factors'], [612, 619], '#808080'],
 	[['elements', 'tables', 'substitutions', 'all_factors'], [1493, 1497], '3.74'],
 	[['elements', 'tables', 'substitutions', 'rr_table_OSE_HS'], [721, 724], '307']
 	])

def test_gen_report(tables, bars, rnd_dict, total_responses, subPath, cmpRange, expVal, school = False):
    client_dir = 'clients/Davis Joint Unified School District'
    report = synthesis_report.gen_report(client_dir.split("/")[-2], tables, bars, rnd_dict, total_responses, school = False)
    print(report)
    for pathPart in subPath:
    	report = report[pathPart]
    if cmpRange:
        report = report[cmpRange[0]: cmpRange[1]]
    assert_equal(report, expVal)

@pytest.mark.parametrize('product_level, df_name, loc, expVal', [
	['OSE_HS', 'edqual',[0, 'High'], ['79%', 2]],
	['OSE_HS', 'all_factors', [4, 'High'], [3.56, 2]],
	['OSE_HS', 'all_factors', [2, 'hs_trend'], [1, 0.01]],
	['OSE_HS', 'all_factors_pct', [6, 'High'], ['27%', 4]],
	['OSE_HS', 'respect_stu', [0, 'hs_trend'], [2, 4]],
	['OSE_HS', 'respect_sta', [0, 'High'], ['46%', 2]],
	['OSE_HS', 'discipline', [0, 'High'], ['50%', 2]],
	['OSE_HS', 'expectations', [0, 'hs_trend'], [1, 4]],
	['OSE_HS', 'differentcult', [0, 'High'], ['80%', 2]]
	])

def test_fill_in_df(product_level, dfs, bar_dicts, variables, factor_dict_by_product, rnd_dict, hs_all_mean, hs_all_percent_pos, hs_all_percentile, df_name, loc, expVal):
	#this test tests the function fill_in_df with hs ose data. It also tests some of what is in the function fill_in_data in order to set up the test for fill_in_df. Finally it tests add_trend_data_to_dfs. 
	#MDK to do is to split this test up so tests really only test one function
	client_dir = 'clients/Davis Joint Unified School District'
	client = client_dir.strip('/').split('/')[-1]
	district_mean = pd.DataFrame()
	for i, row in hs_all_mean.iterrows():
		if str(row['target']).split(":")[0] == client:
			district_mean = district_mean.append(row)

	district_percentile = pd.DataFrame()
	for i, row in hs_all_percentile.iterrows():
		if str(row['target']).split(":")[0] == client:
			district_percentile = district_percentile.append(row)
	
	district_percent_pos = pd.DataFrame()
	for i, row in hs_all_percent_pos.iterrows():
		if str(row['target']).split(":")[0] == client and row['type'] == 'district':
			district_percent_pos = district_percent_pos.append(row)
	district_mean, district_percentile, district_percent_pos = synthesis_report.add_trend_data_to_dfs(district_mean, district_percentile, district_percent_pos, rnd_dict)
	dfs['all_factors'] = synthesis_report.fill_in_df(product_level, dfs['all_factors'], district_mean, district_percentile, district_percent_pos, variables.level_dict, variables.trend_dict, mean=True)
	for df in dfs.values():
		df = synthesis_report.fill_in_df(product_level, df, district_mean, district_percentile, district_percent_pos, variables.level_dict, variables.trend_dict, mean = False)
	print(dfs[df_name])
	assert_equal(dfs[df_name].loc[loc[0], loc[1]], expVal)

@pytest.mark.parametrize('percentile, expVal', [
	[63, 2],
	[0, 4],
	[100, 1],
	[25, 3],
	[50, 2]
	])

def test_determine_quartile(percentile, expVal):
	quartile = synthesis_report.determine_quartile(percentile)
	assert_equal(quartile, expVal)

@pytest.mark.parametrize('trend_value, expVal', [
	[1, '&#8593'],
	[2, '&#8595'],
	[3, '&nbsp;'],
	['', '&nbsp;']
	])

def test_create_arrow(trend_value, expVal):
	arrow = synthesis_report.create_arrow(trend_value)
	assert_equal(arrow, expVal)

@pytest.mark.parametrize('score, last_score, expVal', [
	[10, False, (3, '')],
	[10, 5, (1, 5)],
	[95, 30, (1, 65)],
	[95, 95, (3, '')],
	[95, 96, (2, 1)],
	])

def test_determine_trend(score, last_score, expVal):
	trend, difference = synthesis_report.determine_trend(score, last_score)
	assert_equal((trend, difference), expVal)

@pytest.mark.parametrize('value, quartile, expVal', [
	[13, '', 13],
	[13, 2, '<span style ="text-align:left; color:#2e9fd0;font-weight:bold">13</span>'],
	[93, 4, '<span style ="text-align:left; color:#e62e00;font-weight:bold">93</span>'],
	[50, 1, '<span style ="text-align:left; color:#0D47A1;font-weight:bold">50</span>'],
	[100, 3, '<span style ="text-align:left; color:#f67b33;font-weight:bold">100</span>']
	])

def test_format_number(value, quartile, expVal):
	formatted = synthesis_report.format_number(value, quartile)
	print(formatted)
	assert_equal(formatted, expVal)

@pytest.mark.parametrize('bar_dict, product_level, key, expVal', [
	['eng_theme_bar', 'OSE_HS', 'OSE_HS', [57, 50, 52, 54, 53]]
	])

def test_fill_in_bar_dict(bar_dicts, bar_dict, product_level, hs_all_mean, hs_all_percent_pos, hs_all_percentile, rnd_dict, key, expVal):
	#function to test fill_in_bar_dict but it also includes some code from fill_in_data and it calls add_trend_data_to_dfs so also tests some of those
	#MDK to do is to split this test up so tests really only test one function
	client_dir = 'clients/Davis Joint Unified School District'
	client = client_dir.strip('/').split('/')[-1]
	district_mean = pd.DataFrame()
	for i, row in hs_all_mean.iterrows():
		if str(row['target']).split(":")[0] == client:
			district_mean = district_mean.append(row)

	district_percentile = pd.DataFrame()
	for i, row in hs_all_percentile.iterrows():
		if str(row['target']).split(":")[0] == client:
			district_percentile = district_percentile.append(row)
	
	district_percent_pos = pd.DataFrame()
	for i, row in hs_all_percent_pos.iterrows():
		if str(row['target']).split(":")[0] == client and row['type'] == 'district':
			district_percent_pos = district_percent_pos.append(row)
	district_mean, district_percentile, district_percent_pos = synthesis_report.add_trend_data_to_dfs(district_mean, district_percentile, district_percent_pos, rnd_dict)
	bar_dict = synthesis_report.fill_in_bar_dict(bar_dicts[bar_dict], product_level, district_percent_pos, rnd_dict)
	print(bar_dict)
	assert_equal(bar_dict[key], expVal)

@pytest.mark.parametrize('bar_dict, product_level, school, key, expVal', [
	['school_es_eng_theme_bar', 'OSE_ES', 'LV', 'OSE_ES', [91, 86]],
	['school_es_rel_theme_bar', 'OSE_ES', 'LV', 'OSE_ES', [83, 79]]
	])

def test_schools_fill_in_bar_dict(school, school_bar_dicts, bar_dict, product_level, school_all_mean, school_all_percent_pos, school_all_percentile, school_rnd_dict, schools_nameStems_dict, variables, key, expVal):
	client_dir = 'clients/Lancaster School District (CA)'
	client = client_dir.strip('/').split('/')[-1]
	school_mean = pd.DataFrame()
	for i, row in school_all_mean.iterrows():
		if str(row['target']).split(":")[0] == school:
			school_mean = school_mean.append(row)

	school_percentile = pd.DataFrame()
	for i, row in school_all_percentile.iterrows():
		if str(row['target']).split(":")[0] == school:
			school_percentile = school_percentile.append(row)
	
	school_percent_pos = pd.DataFrame()
	for i, row in school_all_percent_pos.iterrows():
		if str(row['target']).split(":")[0] == school and row['type'] == 'school':
			school_percent_pos = school_percent_pos.append(row)
	school_mean, school_percentile, school_percent_pos = synthesis_report.add_trend_data_to_dfs(school_mean, school_percentile, school_percent_pos, school_rnd_dict)
	school_bar_dict = synthesis_report.schools_fill_in_bar_dict(school, school_bar_dicts[bar_dict], product_level, school_percent_pos, school_rnd_dict, schools_nameStems_dict, variables.product_dict)
	print(school_bar_dict)
	assert_equal(school_bar_dict[key], expVal)