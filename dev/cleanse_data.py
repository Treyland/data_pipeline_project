#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import ast
import numpy as np
import logging

pd.options.mode.chained_assignment = None

#Configure Logger
logging.basicConfig(filename="./dev/cleanse_db.log",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    filemode='w',
                    level=logging.DEBUG,
                    force=True)
logger = logging.getLogger(__name__)

#Cleanse Functions
def cleanse_student_table(df):

    df['job_id'] = df['job_id'].astype(float)
    df['current_career_path_id'] = df['current_career_path_id'].astype(float)
    df['num_course_taken'] = df['num_course_taken'].astype(float)
    df['time_spent_hrs'] = df['time_spent_hrs'].astype(float)

    now = pd.to_datetime('today')
    df['age'] = ((now - pd.to_datetime(df['dob']))/np.timedelta64(1, 'Y')).astype('float').round(0)
    df['age_group'] = np.int64((df['age']/10))*10

    df['contact_info'] = df["contact_info"].apply(lambda x: ast.literal_eval(x))
    explode_contact = pd.json_normalize(df['contact_info'])
    df = pd.concat([df.drop('contact_info', axis=1).reset_index(drop=True), explode_contact], axis=1)

    split_mailing = df['mailing_address'].str.split(',', expand=True)
    split_mailing.columns = ['street', 'city', 'state', 'zip_code']
    df = pd.concat([df.drop('mailing_address', axis=1), split_mailing], axis=1)

    missing_data = pd.DataFrame()
    missing_course_taken = df[df[['num_course_taken']].isnull().any(axis=1)]
    missing_data = pd.concat([missing_data, missing_course_taken])
    df = df.dropna(subset=['num_course_taken'])

    missing_job_id = df[df[['job_id']].isnull().any(axis=1)]
    missing_data = pd.concat([missing_data, missing_job_id])
    df = df.dropna(subset=['job_id'])

    df['current_career_path_id'] = np.where(df['current_career_path_id'].isnull(), 0, df['current_career_path_id'])
    df['time_spent_hrs'] = np.where(df['time_spent_hrs'].isnull(), 0, df['time_spent_hrs'])

    return(df, missing_data)

def cleanse_courses_data(df):
    none = {
    'career_path_id': 0,
    'career_path_name': 'none',
    'hours_to_complete': 0}
    df.loc[len(df)] = none
    return(df)

def cleanse_student_jobs(df):
    return(df.drop_duplicates())


#Unit Tests
#Null Test
def test_nulls(df):
    df_missing = df[df.isnull().any(axis=1)]
    count_missing = len(df_missing)

    try:
        assert count_missing == 0, 'There are' + str(count_missing) + ' nulls in the table'
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else: print('No null rows found')

#Data Type Test (local_df = df we working with, db_df = df already upserted)
def test_schema(local_df, db_df):
    errors = 0
    for col in db_df:
        try:
            if local_df[col].dtypes != db_df[col].dtypes:
                errors += 1
        except NameError as ne:
            logger.exception(ne)
            raise ne
    if errors > 0:
        assert_err_msg = str(errors) + ' column(s) dtypes are not the same'
        logger.exception(assert_err_msg)
    assert errors == 0, assert_err_msg

#Same Number of Columns test
def test_num_columns(local_df, db_df):
    try:
        assert len(local_df.columns) == len(db_df.columns)
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else: print('Number of columns are the same') 

#Join keys test
def test_for_course_id(students, courses):
    student_table = students['current_career_path_id'].unique()
    is_subset = np.isin(student_table, courses['career_path_id'].unique())
    missing_id = student_table[~is_subset]

    try:
        assert len(missing_id) == 0, "Missing career_path_id(s)" + str(list(missing_id)) + " in 'courses' table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('All career_path_ids are present.')

def test_for_job_id(students, student_jobs):
    student_table = students['job_id'].unique()
    is_subset = np.isin(student_table, student_jobs['job_id'].unique())
    missing_id = student_table[~is_subset]

    try:
        assert len(missing_id) == 0, "Missing job_id(s)" + str(list(missing_id)) + " in 'student_jobs' table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('All job_ids are present.')

#Main Method
def main():

    logger.info('Start Log')

    #Calculate Version for changelog
    with open('./dev/changelog.md') as f:
        lines = f.readlines()
    #X.Y.Z
    next_ver = int(lines[0].split('.')[2][0])+1
    
    #Connect to dev database and read in tables
    con = sqlite3.connect('./dev/cademycode.db')
    students = pd.read_sql_query("SELECT * FROM cademycode_students", con)
    courses = pd.read_sql_query("SELECT * FROM cademycode_courses", con)
    student_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)
    con.close()

    #Get current prod tables (if any)
    try: 
        con = sqlite3.connect('./prod/cademycode_cleansed.db')
        clean_db = pd.read_sql_query("SELECT * FROM cademycode_cleansed", con)
        missing_db = pd.read_sql_query("SELECT * FROM missing_data", con)
        con.close()

        #filter for students that aren't in prod db
        new_students = students[~np.isin(students['uuid'].unique(), clean_db['uuid'].unique())]
    except:
        new_students = students
        clean_db = []
    #Clean the new students 
    clean_new_students, missing_data = cleanse_student_table(new_students)

    try: 
        #Filter for incomplete rows that don't exist in missing data table
        new_missing_data = missing_data[~np.isin(missing_data['uuid'].unique(), missing_db['uuid'].unique())]
    except:
        new_missing_data = missing_data

    #upsert new incomplete data (if any)
    if len(new_missing_data) > 0:
        con = sqlite3.connect('./dev/cademycode_cleansed.db')
        new_missing_data.to_sql('missing_data', con, if_exists='append', index=False)
        con.close()
    #move forward if there is new student data
    if len(clean_new_students) > 0:
        clean_courses = cleanse_courses_data(courses)
        clean_student_jobs = cleanse_student_jobs(student_jobs)

        ## Unit Test before joins ##
        test_for_job_id(clean_new_students, clean_student_jobs)
        test_for_course_id(clean_new_students, clean_courses)

        clean_new_students['job_id'] = clean_new_students['job_id'].astype(int)
        clean_new_students['current_career_path_id'] = clean_new_students['current_career_path_id'].astype(int)

        ## Join dfs ##
        df_clean = clean_new_students.merge(
            clean_courses,
            left_on = 'current_career_path_id',
            right_on = 'career_path_id',
            how = 'left'
        )

        df_clean = df_clean.merge(
            clean_student_jobs,
            on = 'job_id',
            how = 'left' 
        )

        ## Test Schema, columns, and nulls before upserting ##
        if len(clean_db) > 0:
            test_num_columns(df_clean, clean_db)
            test_schema(df_clean, clean_db)
        test_nulls(df_clean)

        ## Connect to cademycode_cleansed.db and upsert new cleaned data ##
        con = sqlite3.connect('./dev/cademycode_cleansed.db')
        df_clean.to_sql('cademycode_cleansed', con, if_exists = 'append', index=False)
        clean_db = pd.read_sql_query('SELECT * FROM cademycode_cleansed', con)
        con.close()

        ## Write new cleaned data to a CSV file ##
        clean_db.to_csv('./dev/cademycode_cleansed.csv')

        ## Create changelog entry ##
        new_lines = [
            '## 0.0.' + str(next_ver) + '\n' +
            '### Added\n' +
            '- ' + str(len(df_clean)) + ' more data to database of clean data\n' +
            '- ' + str(len(new_missing_data)) + ' new missing data to missing_data table\n' +
            '\n'
        ]
        w_lines = ''.join(new_lines + lines)

        ## Update Changelog ##
        with open('./dev/changelog.md', 'w') as f:
            for line in w_lines:
                f.write(line)

    else:
        print('No new data')
        logger.info('No new data')
    logger.info('End log')

## Run main function in our driver ##
if __name__ == '__main__':
    main()








    


