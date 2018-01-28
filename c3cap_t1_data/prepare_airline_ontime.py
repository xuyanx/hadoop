'''
Created on Jan 20, 2018

@author: xuyanx
'''

import sys
import os
import zipfile
import csv
import multiprocessing


def extract(output_file, input_zfile):
    try :
        z = zipfile.ZipFile(input_zfile) 
        for filename in z.namelist() :
            if filename.endswith('.csv'):  
                f = z.open(filename)
                reader = csv.DictReader(f)
                for row in reader :
                    cancelled = int(float(row['Cancelled'])) > 0
                    diverted = int(float(row['Diverted'])) > 0
                    if not cancelled and not diverted:             
                        newline = ','.join([row['DayOfWeek'],
                                            row['FlightDate'],
                                            row['UniqueCarrier'],
                                            row['FlightNum'],
                                            row['Origin'],
                                            row['Dest'],
                                            row['DepTime'],
                                            row['DepDelay'],
                                            row['ArrTime'],
                                            row['ArrDelay']]) + '\n'       
                        output_file.write(newline)
                output_file.flush()
                f.close()
    except :
        print ('[%s] failed to open file %s' % (multiprocessing.current_process(), input_zfile))


def process(year_folder, output_folder): 
    print("[%s] processing folder: %s, output folder: %s" % (multiprocessing.current_process(), year_folder, output_folder))
    output_file_path = os.path.join(output_folder, os.path.basename(year_folder) + '.txt')
    output_file = open(output_file_path, 'w+')
    
    files = os.listdir(year_folder)
    progress = 0
    total = len(files) 
    for filename in files:
        print ('[%s] processing file: %s' % (multiprocessing.current_process(), filename))       
        progress += 1
        if filename.endswith('.zip') :
            extract(output_file, os.path.join(year_folder, filename))
        print ('[%s] progress is %.2f%%' % (multiprocessing.current_process(), progress * 100.0 / total))

    output_file.close()


def main():
    if(len(sys.argv) < 3):
        print 'usage: python prepare_airline_ontime.py <airline ontime data folder> <output folder>'
        return 
    
    airline_ontime_folder = sys.argv[1]
    output_folder = sys.argv[2]
    
    pool = multiprocessing.Pool()
    for path in os.listdir(airline_ontime_folder):
        year_folder = os.path.join(airline_ontime_folder, path)
        if os.path.isdir(year_folder) :
            pool.apply_async(process, args=(year_folder, output_folder,))
    
    print 'It may take minutes to process data, please wait...'
    
    pool.close()
    pool.join()
    
    print 'done!'


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'Interrupted'
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
