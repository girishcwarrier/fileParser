#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import logging
import queue
import argparse
import threading
import itertools

from datetime import datetime
from collections import defaultdict


class FileParser:
    """
    Description : This class parses files with csv extension and generates a unified csv
    """
    def __init__(self):
        """
        Description : Initializing the class
        """
        # initialize the arg parser
        parser = argparse.ArgumentParser(description='File parser module')
        parser.add_argument('--filepath',
                            help='Provide filenames to be parsed here')
        args = parser.parse_args()
        if not args.filepath:
            raise Exception('Please provide the filepath for parsing.')
        # checking whether single/multiple files passed in cli args
        if args.filepath.split(','):
            self.file_list = args.filepath.split(',')
        else:
            self.file_list = [args.filepath]
        # initialize the logger
        self.setup_logging()
        # initialize the queue
        self.file_queue = queue.Queue()
        # shared variable containing data of processed files.
        self.data_list = {}

    def setup_logging(self):
        logging.basicConfig(filename='file_parser.log', level=logging.DEBUG, filemode='w',
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')

    def add_files_queue(self):
        """
        Description : This method adds input files to a queue
        """
        for filename in self.file_list:
            self.file_queue.put(filename)
        logging.debug('{} files have been added to queue'.format(self.file_queue.qsize()))

    def process_queued_files(self):
        """
        Description : This method processes files which are queued
        """
        while True:
            try:
                # retrieve the element for queue
                filename = self.file_queue.get(block=False)
            except queue.Empty:
                return
            else:
                # call the file parser factory method
                self.file_parser_factory_method(filename)

    def file_parser_factory_method(self, filename):
        """
        Description : This method is a factory method for processing different file extensions
        """
        if filename.endswith('.csv'):
            self.csv_parser(filename)
        elif filename.endswith('.xml'):
            # to be implemented(TBD)
            pass
        elif filename.endswith('.json'):
            # to be implemented(TBD)
            pass
        else:
            raise Exception('Error. Cannot parse this type of file.')

    def csv_parser(self, filename):
        """
        Description : This module parses a csv file
        """
        logging.debug('Processing file-{} in - {}'.format(filename, threading.current_thread().name))
        columns = defaultdict(list)
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)  # read rows into a dictionary format
            for row in reader:  # read a row as {column1: value1, column2: value2,...}
                for (k, v) in row.items():  # go over each column name and value
                    columns[k].append(v)
        # if column names are euro and cents then combine it into a single column 'amount'
        if columns.get('euro', None) and columns.get('cents', None):
            columns['amount'] = ['{}.{}'.format(k, y) for k, y in zip(columns.get('euro'), columns.get('cents'))]
            del columns['euro']
            del columns['cents']
        # if column name is timestamp move it to 'date' key
        if columns.get('timestamp', None):
            columns['date'] = columns.get('timestamp', None)
            del columns['timestamp']
        # if column name is date_readable move it to 'date' key
        if columns.get('date_readable', None):
            columns['date'] = columns.get('date_readable', None)
            del columns['date_readable']
        # if column name is type move it to 'transaction' key
        if columns.get('type', None):
            columns['transaction'] = columns.get('type', None)
            del columns['type']
        # if column name is amounts move it to 'amount' key
        if columns.get('amounts', None):
            columns['amount'] = columns.get('amounts', None)
            del columns['amounts']
        # change the rows in column 'date' into unified date format
        date_list = []
        for row in columns['date']:
            for fmt in ('%b %d %Y', '%d %b %Y', '%d-%m-%Y'):
                try:
                    date_obj = datetime.strptime(row, fmt)
                    date_list.append(date_obj.strftime('%d-%m-%Y'))
                except ValueError:
                    pass
        columns['date'] = date_list
        # generate a common shared variable self.data_list for putting into csv
        if not self.data_list:
            self.data_list = columns
        else:
            for i, j in columns.items():
                if i in self.data_list:
                    self.data_list[i].extend(j)
        logging.debug('Finished processing file-{} in - {}'.format(filename, threading.current_thread().name))

    def generate_csv(self):
        """
        Description : Generate a unified csv file out of all processed csv parsed
        """
        filename = 'bank_unified_data.csv'
        logging.debug('Generating unified csv file - {} after processing of all files in queue.'.format(filename))
        keys = self.data_list.keys()
        csvrows = itertools.zip_longest(*[self.data_list[k] for k in keys])
        with open(filename, 'w', newline='') as csvfile:
            # creating a csv writer object
            csvwriter = csv.writer(csvfile, )
            csvwriter.writerow(self.data_list.keys())
            for row in csvrows:
                csvwriter.writerow(row)

    def run(self):
        """
        Description : This method initializes and started thread based on the input files added to queue
        """
        try:
            logging.debug('Started execution of File parser module.')
            # add files to queue
            self.add_files_queue()
            threads = []
            # initialize and start the threads
            for i in range(self.file_queue.qsize()):
                thread_obj = threading.Thread(target=self.process_queued_files)
                threads.append(thread_obj)
                thread_obj.start()
            # wait for the threads to finish execution
            for index, thread in enumerate(threads, start=1):
                thread.join()
            # generate_csv
            self.generate_csv()
            logging.debug('Completed execution of File parser module.')
        except Exception as err:
            raise Exception('Failed to execute the file parser module - {}'.format(err))

if __name__ == '__main__':
    obj = FileParser()
    obj.run()
