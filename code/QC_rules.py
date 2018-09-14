import pandas as pd
import numpy as np
from datetime import datetime
import os,re
import time
import warnings
warnings.filterwarnings("ignore")

#All functions

# The function describes the data type of column in the data frame
def column_data_type(data_frame,column):
    sample_data = data_frame[column].head(100).dropna()
    col_dtype = str(pd.Series(sample_data).dtype)
    try:
        if (col_dtype == 'object') or (col_dtype == 'bool'):
            date_sample = pd.to_datetime(sample_data, errors='coerce')
            date_nan_per = np.sum(pd.isnull(date_sample)) * 1.0 / len(date_sample)
            if date_nan_per < 0.5 :
                sample_type = "date"
            else:
                sample_type = "string"
        elif 'datetime' in col_dtype:
            sample_type = "date"
        else:
            sample_type = "numeric"
    except:
        sample_type = "error"
    return sample_type
      
# Validate the date_text format with the given pattern    
def validate_date(date_text,pattern):
    try:
        datetime.strptime(date_text, pattern)
    except ValueError:
        return False
    return True

# Validate the string text with the given patern
def match_pattern(word,pattern):
    try:
        if re.match(pattern,word):
            return True
        else:
            return False
    except:
        return False

# Supply dataframe and columns to look into.
# 1) arg = NULL - Gives you nan % of the columns
# 2) arg = unique - Gives you unique value % of the columns
# 3) arg = summary - Gives you simple summary based on the type of columns
def data_quality(data_frame,columns,arg):
    if arg=="NULL":
        column_null_check = {}
        for column in columns:
            nan_rate = str(round((1 - (float(len(data_frame[column].dropna()))/len(data_frame[column]))),4)*100)+"%"
            column_null_check[column] = nan_rate
        return column_null_check
    if arg=="unique":
        unq_val_check = {}
        for column in columns:
            unique_val_rate = str(float(len(data_frame[column].unique()))/len(data_frame[column])*100)+"%"
            unq_val_check[column] = unique_val_rate
        return unq_val_check
    if arg=="summary":
        summary_columns = {}
        data_frame = data_frame.dropna()
        for column in columns:
            value_dic = {}
            value_dic['type'] = column_data_type(data_frame,column)
            if value_dic['type'] == "numeric":
                value_dic['minimum'] = data_frame[column].min()
                value_dic['maximum'] = data_frame[column].max()
                value_dic['mean'] = data_frame[column].mean()
                value_dic['median'] = data_frame[column].median()   
            if value_dic['type'] == "string":
                value_dic['levels'] = data_frame[column].nunique()
            if value_dic['type'] == "date":
                value_dic['min_date']= pd.to_datetime(data_frame[column], errors='coerce').min()
                value_dic['max_date']= pd.to_datetime(data_frame[column], errors='coerce').max()
            summary_columns[column] = value_dic
        return summary_columns
    
# Uses validate functions to check the format of the column 
def format_check(data_frame,column,format_to_check):
    data_frame = data_frame.dropna()
    if column_data_type(data_frame,column) == "string" or column_data_type(data_frame,column) == "numeric":
        data_frame[column] = [re.sub(' +',' ',str(word)).strip() for word in data_frame[column]]
        if False in list(set([match_pattern(str(word),format_to_check) for word in data_frame[column]])):
            return False
    if column_data_type(data_frame,column) == "date":
        if False in list(set([validate_date(str(word),format_to_check) for word in data_frame[column]])):
            return False
    else:
        return "Try giving column instead of list of columns"
    return True
            
# Give dataframe and column you want to look into.
# range_args - parameters to check the range of the column
# 1) arg = levels - For string column, gives agg. count of all levels, No need to use range_args
# 2) arg = range - depdending on type of columns - checks if the column is bound by range_args values
# 3) arg = sum,count - for given column, checks if the sum is bound by range_args values

def agg_check(data_frame,column,range_args,arg):
    if arg == "levels":
        return pd.DataFrame({'count of '+column : data_frame.groupby(column).size()}).reset_index()
#        count_list = []
#        for column in desired_columns:
#            count_list.append(pd.DataFrame({'count of '+column : data_frame.groupby(column).size()}).reset_index())
#        return count_list
    if arg == "range":
        if column_data_type(data_frame,column) == "numeric":
            if False in list(set([value > range_args[0] for value in data_frame[column]])):
                output_file = data_frame[data_frame[column] < range_args[0]]
                output_file.to_csv("../results/{}_{}_{}_{}_{}.csv".format(arg,column,"greater than",range_args[0],"fail"))
            if False in list(set([value < range_args[1] for value in data_frame[column]])):
                output_file = data_frame[data_frame[column] > range_args[1]]
                output_file.to_csv("../results/{}_{}_{}_{}_{}.csv".format(arg,column,"less than",range_args[1],"fail"))                
            range_dic ={'Test':[column+' '+arg+' greater than '+str(range_args[0]),column+' '+arg+' less than '+str(range_args[1])], 'Result':[data_frame[column].min() > range_args[0],data_frame[column].max() < range_args[1]]}                
        if column_data_type(data_frame,column) == "date":
            min_date = datetime.strptime(range_args[0], '%Y-%m-%d')
            max_date = datetime.strptime(range_args[1], '%Y-%m-%d')
            if False in list(set([pd.to_datetime(value) > pd.Timestamp(min_date) for value in data_frame[column]])):
                output_file = data_frame[pd.to_datetime(data_frame[column]) < min_date]
                output_file.to_csv("../results/{}_{}_{}_{}_{}.csv".format(arg,column,"greater than",range_args[0],"fail"))
            if False in list(set([pd.to_datetime(value) < pd.Timestamp(max_date) for value in data_frame[column]])):
                output_file = data_frame[pd.to_datetime(data_frame[column]) > max_date]
                output_file.to_csv("../results/{}_{}_{}_{}_{}.csv".format(arg,column,"less than",range_args[1],"fail"))            
            range_dic ={'Test':[column+' '+arg+' greater than '+str(range_args[0]),column+' '+arg+' less than '+str(range_args[1])],'Result':[pd.to_datetime(data_frame[column], errors='coerce').min() > pd.Timestamp(min_date),pd.to_datetime(data_frame[column], errors='coerce').max() < pd.Timestamp(max_date)]}
        range_frame = pd.DataFrame(data=range_dic)
        return range_frame
    if arg == "sum":
        agg_dic ={'Test':[column+' '+arg+' less than '+str(range_args[0]),column+' '+arg+' greater than '+str(range_args[1])],'Result':[  data_frame[column].sum() < range_args[0],data_frame[column].sum() > range_args[1]]}
    if arg == "count":
        agg_dic ={'Test':[column+' '+arg+' less than '+str(range_args[0]),column+' '+arg+' greater than '+str(range_args[1])],'Result':[  data_frame[column].count() < range_args[0],data_frame[column].count() > range_args[1]]}
    agg_frame = pd.DataFrame(data=agg_dic)
    return agg_frame
        

# Supply dataframe, desired columns ( columns you want to look into),
# by_columns ( columns you want to do group by), func_to_check (function to perform - sum,count,mean)

def groupby_check(data_frame,desired_columns,by_columns,func_to_check):
    if len(by_columns) > 0:
        if func_to_check == "sum":
            return pd.DataFrame(data_frame.groupby(by_columns)[desired_columns].sum())
        if func_to_check == "count":
            return pd.DataFrame(data_frame.groupby(by_columns)[desired_columns].count())
        if func_to_check == "mean":
            return pd.DataFrame(data_frame.groupby(by_columns)[desired_columns].mean())
    else:
        nogroup_frame = pd.DataFrame(columns=["column",func_to_check])
        for column in desired_columns:
            if func_to_check == "sum":
                single_frame = pd.DataFrame([{'column': column,'sum':data_frame[column].sum()}])
            if func_to_check == "count":
                single_frame = pd.DataFrame([{'column': column,'count':data_frame[column].count()}])
            if func_to_check == "mean":
                single_frame = pd.DataFrame([{'column': column,'mean':data_frame[column].mean()}])
            nogroup_frame = nogroup_frame.append(single_frame)
        return nogroup_frame

def validate_book_on_load(file_path):
    file_name = os.path.basename(file_path)
    if file_name.split(".")[-1] == "csv":
        data_frame = pd.read_csv(file_path)
    else:
        data_frame = "Supports only csv format"
    return data_frame

def Check_with_time(data_frame,type_of_check,desired_columns,by_columns,threshold_value,func_to_check):
    if type_of_check == "quality":
        start_time = time.time()
        quality_output = data_quality(data_frame,desired_columns,func_to_check)
        time_taken = time.time() - start_time
        time_frame = pd.DataFrame([{'Test':"{} of {} ".format(func_to_check,str(desired_columns)),"Result": "Completed","Time": time_taken}])
        return time_frame, quality_output
    if type_of_check == "format":
        start_time = time.time()
        format_output = format_check(data_frame,desired_columns,func_to_check)
        time_taken = time.time() - start_time
        time_frame = pd.DataFrame([{'Test':"check {} of {} is {} ".format(type_of_check,desired_columns,func_to_check),"Result":format_output ,"Time": time_taken}])
        return time_frame, format_output
    if type_of_check == "aggregate":
        start_time = time.time()
        agg_output = agg_check(data_frame,desired_columns,threshold_value,func_to_check)
        time_taken = time.time() - start_time
        if func_to_check == "levels":
            time_frame = pd.DataFrame([{'Test':"{} of {} ".format(func_to_check,desired_columns),"Result":"Completed","Time": time_taken}]) 
        else:
            time_frame = pd.DataFrame(columns=['Test','Result','Time'])
            for index,row in agg_output.iterrows():                
                time_frame = time_frame.append(pd.DataFrame([{'Test': row['Test'],"Result":row['Result'] ,"Time": time_taken}]))      
        return time_frame,agg_output
    if type_of_check == "groupby":
        start_time = time.time()
        group_output = groupby_check(data_frame,desired_columns,by_columns,func_to_check)
        time_taken = time.time() - start_time
        if len(by_columns) > 0 :
            time_frame = pd.DataFrame([{'Test':"{} of {} by {}".format(func_to_check , str(desired_columns), str(by_columns)),'Result':"Completed",'Time':time_taken}])
        else:
            time_frame = pd.DataFrame([{'Test':"{} of {}".format(func_to_check , str(desired_columns)),'Result':"Completed",'Time':time_taken}])
        return time_frame, group_output

def notebook_summary(file_path,final_time_frame):
    file_name = os.path.basename(file_path)
    file_size = os.stat(file_path).st_size
    total_time = final_time_frame['Time'].sum()
    return pd.DataFrame([{'Name of file':file_name,'Size of file(bytes)':file_size,'Total time':total_time}])
