#This script pulls data from YouthTruth's Salesforce account and outputs a sheet displaying the upcoming stream of work for clients, estimating hours of required capacity, and assisting in resource planning. 

from simple_salesforce import Salesforce
import pandas as pd
import calendar
from dateutil import rrule
import datetime
from argparse import ArgumentParser, ArgumentTypeError
import os
from helpers.creds import salesforce_creds
pd.low_memory=False
pd.options.mode.chained_assignment = None  # default='warn'
parser = ArgumentParser()

parser.add_argument('-o', '--outFile', metavar='outfile', help='where to save the capacity planning csv', required=True)
parser.add_argument('-y', '--schoolYear', metavar='schoolYear', help='school year in format YY-YY', required=True)

username = salesforce_creds['user']
password =  salesforce_creds['pwd']
security_token = salesforce_creds['security']

one_day = datetime.timedelta(days = 1)
two_weeks = datetime.timedelta(weeks = 2)
one_week = datetime.timedelta(weeks = 1)
three_days = datetime.timedelta(days = 3)

#function that calculates the points for a specific step of a client's lifecycle
def points_calculator(step, products, levels, school_reports, fft):
    if step == 'survey_admin':              
            return round(2*school_reports**.5*(.5*(products-fft)+fft)+3*fft,0)
    elif step == 'report_production':
            return round(2+products*levels+school_reports**.5*(2*fft+1),0)

def find_monday(date):
    date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    monday = date - datetime.timedelta(days=date.weekday())
    return monday

def find_friday(date):
    date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    friday = date + datetime.timedelta( (4-date.weekday()) % 7)
    return friday

def get_sf_info(username, password, security_token, school_year):

    sf = Salesforce(username, password, security_token)
    query = """
        SELECT 
            Name, 
            Youth_Truth_Opportunity_Type__c, 
            Survey_Window__c, 
            Survey_Start_Date__c, 
            Manual_SIG_Due_Date__c,
            SIG_Due_Date__c,
            Survey_Close_Date__c, 
            Final_Report_Due__c, 
            YouthTruth_Customization__c, 
            Custom_Subgroups__c,
            Overall_Survey_Elementary_School__c,
            Overall_Survey_Middle_School__c,
            Overall_Survey_High_School__c,
            Teacher_Survey_Elementary_School__c,
            Teacher_Survey_Middle_School__c,
            Teacher_Survey_High_School__c,
            Family_SR_ES__c,
            Family_SR_MS__c,
            Family_SR_HS__c,
            Staff_SR_ES__c,
            Staff_SR_MS__c,
            Staff_SR_HS__c,
            StageName
        FROM 
            Opportunity 
        WHERE 
            Name LIKE 'SY {school_year}%' 
            and 
            StageName IN ('4', '5') 
            and 
            RecordTypeID = '012i0000000Pk27AAC'
        """.format(school_year = school_year)

    client_data = sf.query(query)['records']
    rows = []
    cols = list(client_data[0].keys())[1:]
    for client in client_data:
        rows.append(list(client.values())[1:])

    sf_df = pd.DataFrame(rows, columns=cols)
    return sf_df

def build_opp_list(sf_df):

    products = []
    levels = []
    school_reports = []
    FFT = []

    #remove opps without a survey window
    sf_df = sf_df[sf_df.Survey_Window__c.notnull()]

    for i in sf_df.index:   
            num_products = 0
            num_levels = 0
            if not (pd.isnull(sf_df["Overall_Survey_Elementary_School__c"][i]) and pd.isnull(sf_df["Overall_Survey_Middle_School__c"][i]) and pd.isnull(sf_df["Overall_Survey_High_School__c"][i])):
                    num_products+=1
            if not (pd.isnull(sf_df["Teacher_Survey_Elementary_School__c"][i]) and pd.isnull(sf_df["Teacher_Survey_Middle_School__c"][i]) and pd.isnull(sf_df["Teacher_Survey_High_School__c"][i])):
                    num_products +=1
            if not (pd.isnull(sf_df["Family_SR_ES__c"][i]) and pd.isnull(sf_df["Family_SR_MS__c"][i]) and pd.isnull(sf_df["Family_SR_HS__c"][i])):
                    num_products +=1
            if not (pd.isnull(sf_df["Staff_SR_ES__c"][i]) and pd.isnull(sf_df["Staff_SR_MS__c"][i]) and pd.isnull(sf_df["Staff_SR_HS__c"][i])):
                    num_products +=1
            products.append(num_products)
            if not (pd.isnull(sf_df["Overall_Survey_Elementary_School__c"][i]) and pd.isnull(sf_df["Teacher_Survey_Elementary_School__c"][i]) and pd.isnull(sf_df["Family_SR_ES__c"][i]) and pd.isnull(sf_df["Staff_SR_ES__c"][i])):
                    num_levels+=1
            if not (pd.isnull(sf_df["Overall_Survey_Middle_School__c"][i]) and pd.isnull(sf_df["Teacher_Survey_Middle_School__c"][i]) and pd.isnull(sf_df["Family_SR_MS__c"][i]) and pd.isnull(sf_df["Staff_SR_MS__c"][i])):
                    num_levels +=1
            if not (pd.isnull(sf_df["Overall_Survey_High_School__c"][i]) and pd.isnull(sf_df["Teacher_Survey_High_School__c"][i]) and pd.isnull(sf_df["Family_SR_HS__c"][i]) and pd.isnull(sf_df["Staff_SR_HS__c"][i])):
                    num_levels +=1
            levels.append(num_levels)
            school_reports_count = sf_df[["Overall_Survey_Elementary_School__c","Overall_Survey_Middle_School__c","Overall_Survey_High_School__c","Teacher_Survey_Elementary_School__c","Teacher_Survey_Middle_School__c","Teacher_Survey_High_School__c","Family_SR_ES__c","Family_SR_MS__c","Family_SR_HS__c","Staff_SR_ES__c","Staff_SR_MS__c","Staff_SR_HS__c"]].loc[i].sum(skipna=True)
            
            school_reports.append(school_reports_count)
            if not (pd.isnull(sf_df["Teacher_Survey_Elementary_School__c"][i]) and pd.isnull(sf_df["Teacher_Survey_Middle_School__c"][i]) and pd.isnull(sf_df["Teacher_Survey_High_School__c"][i])):
                    FFT.append(1)
            else:
                    FFT.append(0)

    opp_list = sf_df[['Name','Survey_Start_Date__c','Manual_SIG_Due_Date__c','SIG_Due_Date__c','Survey_Close_Date__c','Final_Report_Due__c','Survey_Window__c','YouthTruth_Customization__c','Youth_Truth_Opportunity_Type__c','StageName']]
    opp_list = opp_list.assign(Products=products,Levels=levels,School_Reports=school_reports,FFT=FFT) 
    opp_list["Survey Admin Weeks"] = ''
    opp_list["Report Production Weeks"] = ''

    for i in opp_list.index:
            if opp_list["Survey_Start_Date__c"][i] is None:
                    opp_list["Survey Admin Weeks"][i] = None
                    opp_list["Report Production Weeks"][i] = None
            else:
                    #check if there is a manually over-ridden sig due date and use that if there is
                    try:
                            survey_admin_start = str((datetime.datetime.strptime(opp_list["SIG_Due_Date__c"][i], '%Y-%m-%d') - one_week).date())
                    except TypeError:
                            if opp_list["StageName"][i]==5:
                                    print(("MISSING DATA:{} has no SIG Due Date so their survey admin will be missing".format(opp_list["Name"][i])))
                    opp_list["Survey Admin Weeks"][i] = find_monday(survey_admin_start)
                    try:
                            final_report_due = str((datetime.datetime.strptime(opp_list["Final_Report_Due__c"][i], '%Y-%m-%d') - (one_week)).date())
                    except TypeError:
                            if opp_list["StageName"][i]=='5':
                                    print(("MISSING DATA:{} has invalid/missing Final Report Due Date so their report production will be missing".format(opp_list["Name"][i])))
                    opp_list["Report Production Weeks"][i] = find_monday(final_report_due)
    return opp_list

def build_cal(school_year):
    start_year = int(str(20) + school_year.split('-')[0])
    end_year = int(str(20) + school_year.split('-')[1])
    start_date = find_monday(str(datetime.datetime(start_year, 8, 1).date()))
    end_date = find_friday(str(datetime.datetime(end_year, 7, 15).date()))

    week_of = []
    week = []
    count = 0
    client = []

    for dt in rrule.rrule(rrule.WEEKLY, dtstart = start_date, until=end_date, wkst = 0):
            week_of.append(dt.date())
            count+=1
            week.append(count)

    calendar = pd.DataFrame({'Week Number': week,'Week of:': week_of})
    calendar['Survey Window'] = ''
    calendar['Stage'] = ''
    calendar['Opportunity'] = ''
    calendar['Step'] = ''
    calendar['Products'] = ''
    calendar["Customization"] = ''
    calendar["Points"] = ''
    return calendar

def separate_steps(calendar,opp_list):
    confirmed_windows = []
    unconfirmed_windows = [['Opportunities in Unconfirmed Survey Windows:','','','','','','']]
    for i in calendar.index:
        for j in opp_list.index:
            if calendar["Week of:"][i] == opp_list["Survey Admin Weeks"][j]:
                if calendar["Opportunity"][i]:
                    points = points_calculator('survey_admin', opp_list["Products"][j], opp_list["Levels"][j],opp_list["School_Reports"][j], opp_list["FFT"][j]) 
                    new_row = [calendar['Week Number'][i],calendar['Week of:'][i],opp_list["Survey_Window__c"][j],opp_list["StageName"][j],opp_list["Name"][j], "Survey Admin",opp_list["Youth_Truth_Opportunity_Type__c"][j],opp_list["YouthTruth_Customization__c"][j], points]
                    confirmed_windows.append(new_row)
                else: 
                    calendar["Opportunity"][i] = opp_list["Name"][j]
                    calendar["Step"][i] = "Survey Admin"
                    calendar["Products"][i] = opp_list["Youth_Truth_Opportunity_Type__c"][j]
                    calendar["Points"][i] = points_calculator('survey_admin', opp_list["Products"][j], opp_list["Levels"][j],opp_list["School_Reports"][j], opp_list["FFT"][j]) 
                    calendar["Survey Window"][i] = opp_list["Survey_Window__c"][j]
                    calendar["Customization"][i] = opp_list["YouthTruth_Customization__c"][j]
                    calendar["Stage"][i] = opp_list["StageName"][j]
            elif calendar["Week of:"][i] == opp_list["Report Production Weeks"][j]:
                if calendar["Opportunity"][i]:
                    points = points_calculator('report_production', opp_list["Products"][j], opp_list["Levels"][j],opp_list["School_Reports"][j], opp_list["FFT"][j]) 
                    new_row = [calendar['Week Number'][i],calendar['Week of:'][i],opp_list["Survey_Window__c"][j],opp_list["StageName"][j],opp_list["Name"][j], "Report Production",opp_list["Youth_Truth_Opportunity_Type__c"][j],opp_list["YouthTruth_Customization__c"][j],points]
                    confirmed_windows.append(new_row)
                else:
                    calendar["Opportunity"][i] = opp_list["Name"][j]
                    calendar["Step"][i] = "Report Production"
                    calendar["Products"][i] = opp_list["Youth_Truth_Opportunity_Type__c"][j]
                    calendar["Points"][i] = points_calculator('report_production', opp_list["Products"][j], opp_list["Levels"][j],opp_list["School_Reports"][j], opp_list["FFT"][j]) 
                    calendar["Survey Window"][i] = opp_list["Survey_Window__c"][j]
                    calendar["Customization"][i] = opp_list["YouthTruth_Customization__c"][j]
                    calendar["Stage"][i] = opp_list["StageName"][j]
    for j in opp_list.index:
        if opp_list[ "Survey_Start_Date__c"][j] is None:
            unconfirmed_row = ['','',opp_list["Survey_Window__c"][j],opp_list["StageName"][j],opp_list["Name"][j],'',opp_list["Youth_Truth_Opportunity_Type__c"][j],'',opp_list["YouthTruth_Customization__c"][j]]
            unconfirmed_windows.append(unconfirmed_row)
    return (confirmed_windows,unconfirmed_windows)

def add_opps_to_cal(confirmed_windows, unconfirmed_windows, calendar):
    scheduled_opps = pd.DataFrame(confirmed_windows, columns=calendar.columns.values.tolist())
    calendar = calendar.append(scheduled_opps,ignore_index=True, sort=False)
    calendar.sort_values('Week Number', inplace=True)

    unscheduled_opps = pd.DataFrame(unconfirmed_windows, columns = calendar.columns.values.tolist())
    for i in unscheduled_opps.index:
            if unscheduled_opps["Survey Window"][i] == 'September':
                    unscheduled_opps["Survey Window"][i] = '1. ' + unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'October':
                    unscheduled_opps["Survey Window"][i] = '2. ' + unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'November':
                    unscheduled_opps["Survey Window"][i] = '3. ' + unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'December':
                    unscheduled_opps["Survey Window"][i] = '4. '+ unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'January':
                    unscheduled_opps["Survey Window"][i] = '5. '+ unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'February':
                    unscheduled_opps["Survey Window"][i] = '6. '+ unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'March':
                    unscheduled_opps["Survey Window"][i] = '7. '+ unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'April':
                    unscheduled_opps["Survey Window"][i] = '8. '+ unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'May':
                    unscheduled_opps["Survey Window"][i] = '9. '+ unscheduled_opps["Survey Window"][i]
            if unscheduled_opps["Survey Window"][i] == 'June':
                    unscheduled_opps["Survey Window"][i] = '10. '+ unscheduled_opps["Survey Window"][i]
    unscheduled_opps.sort_values("Survey Window", inplace=True)
    calendar = calendar.append(unscheduled_opps, ignore_index=True, sort=False)

    #calculate total points for week
    total_points = calendar.groupby(['Week Number'],as_index=False)["Points"].sum()
    final_df = calendar.merge(total_points, how='left', on="Week Number", suffixes= 'ab')
    final_df.rename(columns = {"Pointsa":"Points", "Pointsb":"Total Points for Week"},inplace=True)
    return final_df


if __name__ == "__main__":
    args = parser.parse_args()
    school_year = args.schoolYear
    sf_df = get_sf_info(username, password, security_token, school_year)
    opp_list = build_opp_list(sf_df)
    calendar = build_cal(school_year)
    confirmed_windows, unconfirmed_windows = separate_steps(calendar,opp_list)
    final_df = add_opps_to_cal(confirmed_windows,unconfirmed_windows,calendar)
    final_df.to_csv(os.path.join(args.outFile,'capacity_planning.csv'), index=False)
    print("File written to {}".format(os.path.join(args.outFile)))