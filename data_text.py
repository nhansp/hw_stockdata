#!/usr/bin/env python3

# import libs
from vnstock3 import Vnstock
import pandas as pd
import threading
import os 
import time
import sys

# error print so we can suppress logging to /dev/null if we want
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

# execution time clock
clocker = time.time()

# define sources
# !!! requires <...>/codes file or will fail to run
sources = ['HNX', 'UPCOM']

# main function
def fire(source_name):
    # convert to list of codes from source file
    source_read = open(source_name + '/codes', 'r')
    source_codes = source_read.read().split('\n')

    # get information
    def hnx(el):
        # object for writing to file
        source_write = open(source_name + '/txt/' + el + '.txt', 'w')
            
        # fetch from vnstock
        stock = Vnstock().stock(symbol=el, source='VCI')
        
        source_data = [
            (stock.finance.balance_sheet(period='year', lang='vi')),
            (stock.finance.income_statement(period='year', lang='vi')),
            (stock.finance.cash_flow(period='year', lang='vi')),
            (stock.finance.ratio(period='year', lang='vi'))
        ]
        source_data_names = [
            'balance sheet', 
            'income statement', 
            'cash flow', 
            'ratio'
        ]
        # display full dataframe
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'dropna', True), pd.ExcelWriter(source_name + '/xlsx/' + el + '.xlsx') as ewriter:
            
            for source_idx in range(len(source_data)):
                print(source_data[source_idx], file=source_write)
                source_data[source_idx].to_excel(ewriter, sheet_name=source_data_names[source_idx], index=False)
                
            # immediately close write object after we've done file writings
            source_write.close()
        
        pass

    # initialize for multithreading
    nthreads = os.cpu_count()
    print(f'running on {nthreads} threads')
    threads = []
    source_code_index = -1
    source_code_counter = 0

    # sanity check (1): prevent source_codes out of bound accessing 
    while (source_code_index < len(source_codes)): 
        # sanity check (2)
        if (source_code_index >= len(source_codes)):
            break
        
        for i in range(nthreads):
            source_code_index += 1
            
            # sanity check (3)
            if (source_code_index >= len(source_codes)):
                break
            
            # append currently iterated code to thread and count it
            source_code_counter += 1
            current_code = source_codes[source_code_index]
            eprint(f'{source_code_index}. {current_code} -> #{i}')
            t = threading.Thread(target=hnx, args=(current_code,))
            threads.append(t)
            t.start()

        for t in threads:
            # sanity check (4)
            if (source_code_index >= len(source_codes)):
                break
            
            # join the executed thread
            t.join()

    print(f'total {len(source_codes)}, got {source_code_counter}, missed {len(source_codes) - source_code_counter}')
    source_read.close()
    
# real shit here
for _ in sources: 
    print(f'working on {_}')
    fire(_)
    print('\n\n')

# print executed time after we've done everything
print('executed in %s seconds' % (time.time() - clocker))