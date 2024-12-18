#!/usr/bin/env python3

import pandas as pd
import os

output_excel = r'/home/nhansp/stock/all_excels.xlsx'

#List all excel files in folder
excel_folder= r'/home/nhansp/stock/all/'
excel_files = [os.path.join(root, file) for root, folder, files in os.walk(excel_folder) for file in files if file.endswith(".xlsx")]

with pd.ExcelWriter(output_excel) as writer:
    for excel in excel_files: #For each excel
        sheet_name = pd.ExcelFile(excel).sheet_names[0] #Find the sheet name
        df = pd.read_excel(excel) #Create a dataframe
        df.to_excel(writer, sheet_name=sheet_name, index=False) #Write it to a sheet in the output excel