import matplotlib
import pandas as pd
import numpy as np
from datetime import datetime
import os,re
import time
import warnings
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

#All functions
def notebook_summary(file_path,final_time_frame):
    file_name = os.path.basename(file_path)
    file_size = os.stat(file_path).st_size
    total_time = final_time_frame['Time'].sum()
    return pd.DataFrame([{'Name of file':file_name,'Size of file(bytes)':file_size,'Total time':total_time}])

def validate_book_on_load(file_path):
    file_name = os.path.basename(file_path)
    if file_name.split(".")[-1] == "csv":
        data_frame = pd.read_csv(file_path)
    else:
        data_frame = "Supports only csv format"
    return data_frame

def format_text(values):
    def each_text(pct):
        total = sum(values)
        val = int(round(pct*total/100.0))
        return '{p:.2f}%  ({v:d})'.format(p=pct,v=val)
    return each_text

def test_status_plot(final_time_frame):
    graph_data = pd.DataFrame({'count':final_time_frame.groupby(['Result']).size()}).reset_index()
    labels = [ str(word)+ " tests" for word in graph_data['Result']]
    values = graph_data['count']
    colors = ['red','green']
    explode = (0.1,0)
    plt.pie(values, explode=explode,
            labels=labels, colors=colors,
            autopct=format_text(values),startangle=90)
#    plt.set_title("Job status report")
    plt.axis('equal')
    return plt