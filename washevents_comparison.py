import pyspark
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utility import *

# CREATING A SPARK SESSION
sc = SparkSession.builder.appName('washvents_verification').getOrCreate()

counter_sample = -1
counter_aurora = -1
list_of_dates_sample_missing = []
list_of_dates_aurora_missing = []

# MVP1 VERIFICATIONS
def mvp1_washevents_comparison(washevents_path, output_path):

    print('************** MVP1 WASHEVENTS VERIFICATION STARTED **************')

    # LOADING THE DATASETS
    print('1. loading the datasets')
    gp_washevents = getWashevents(sc, washevents_path['gp_washevents'])
    aurora_washevents = getWashevents(sc, washevents_path['aurora_washevents'])

    # COMPARING AND GETTING THE UNIQUE RECORDS FROM BOTH THE DATASETS
    print('2. Getting the unique records')
    df_gp = gp_washevents.join(aurora_washevents, ['esn', 'wash_date'], 'anti')
    df_aurora = aurora_washevents.join(
        gp_washevents, ['esn', 'wash_date'], 'anti')

    # WRITING THE UNIQUE WASHEVENTS INTO CSV FILES
    print('3. Writing the unique records into CSV files')
    writeToCSV(df_gp, output_path['gp_mismatched_washevents'])
    writeToCSV(df_aurora, output_path['aurora_mismatched_washevents'])

    # CHANGING THE COLUMN NAMES OF UNIQUE WASHEVENTS DATASET
    df_gp = df_gp.withColumnRenamed('wash_date', 'wash_date_gp').withColumnRenamed(
        'source_type', 'source_type_gp').withColumnRenamed('engine_family', 'engine_family_gp')
    df_aurora = df_aurora.withColumnRenamed('wash_date', 'wash_date_aurora').withColumnRenamed(
        'source_type', 'source_type_aurora').withColumnRenamed('engine_family', 'engine_family_aurora')

    # MERGING THE GP AND AURORA UNIQUE WASHEVENTS
    df = df_gp.join(df_aurora, ['esn', 'wash_type'], 'outer')

    # CALCULATING THE DATE DIFFERENCE AND SEPARATLY STORING THE DATE DIFFERENCE ROWS
    print('4. ROUND-1 of Timezone Difference Elimination has started')
    df = df.withColumn('date_diff', abs(datediff(to_date(
        df.wash_date_gp, 'yyyy-MM-dd'), to_date(df.wash_date_aurora, 'yyyy-MM-dd'))))
    df_timezone = df.filter(df.date_diff < 7).distinct()

    # RENAMING THE COLUMNS OF THE TIMEZONE DIFFERENCE ROWS
    df_timezone = df_timezone.withColumnRenamed('wash_date_aurora', 'wash_date_aurora1').withColumnRenamed(
        'source_type_aurora', 'source_type_aurora1').withColumnRenamed('engine_family_aurora', 'engine_family_aurora1')

    # TAKING THE AURORA COLUMNS FROM THE TIMEZONE DIFFERENCE ROWS
    print('5. ROUND-2 of Timezone Difference Elimination has started')
    df_round_2_timezone_difference = df_timezone.select(
        'esn', 'wash_date_aurora1', 'wash_type', 'source_type_aurora1', 'engine_family_aurora1')

    # AGAIN MERGING THE ROUND 2 WASHEVENTS AND UNIQUE AURORA WASHEVENTS AND ADDING THE DATE DIFFERENCE COLUMN
    df_round_2_timezone_difference = df_aurora.join(
        df_round_2_timezone_difference, ['esn', 'wash_type'], 'outer')
    df_round_2_timezone_difference = df_round_2_timezone_difference.withColumn('date_diff', abs(datediff(to_date(
        df_round_2_timezone_difference.wash_date_aurora, 'yyyy-MM-dd'), to_date(df_round_2_timezone_difference.wash_date_aurora1, 'yyyy-MM-dd'))))

    # SEPARATING THE GP AND AURORA RECORDS WHICH ARE NOT HAVING THE TIMEZONE DIFFERENCE
    df_gp_timezone_eliminated = df_gp.join(
        df_timezone, ['esn', 'wash_date_gp', 'wash_type'], 'left_anti')
    df_aurora_timezone_eliminated = df_aurora.join(df_round_2_timezone_difference, [
                                                   'esn', 'wash_date_aurora', 'wash_type'], 'left_anti')

    # RENAMING THE ENGINE FAMILIES IN GP WASHEVENTS BEFORE STARTING THE COLUMN WISE WASHEVENTS DIFFERENCE
    gp_washevents = gp_washevents.withColumn(
        'engine_family', trim(split("engine_family", "-")[0]))

    # COLUMN WISE MISMATCH DIFFERENCE FUNCTION
    print('6. Column wise washevents mismatch detection started')

    def calculate_mismatched_records(list_of_columns):
        df_gp = gp_washevents.join(aurora_washevents, list_of_columns, 'anti')
        df_aurora = aurora_washevents.join(
            gp_washevents, list_of_columns, 'anti')
        return (df_gp.count() + df_aurora.count())

    # CALCULATING THE COLUMN WISE DIFFERENCE
    total_records = gp_washevents.count() + aurora_washevents.count()
    list_of_all_columns = gp_washevents.schema.names
    list_of_input_columns = []
    list_of_mismatching_columns = []
    list_of_mismatching_counts = []
    previous_mismatching_records = 0
    total_mismatching_records = 0

    # ITERATING THROUGH ALL THE COLUMNS ONE BY ONE
    for column in list_of_all_columns:

        mismatching_records = 0
        net_mismatching_record = 0

        if not list_of_mismatching_columns:
            list_of_input_columns.append(column)
            mismatching_records = calculate_mismatched_records(
                list_of_input_columns)

            if mismatching_records != total_records:
                previous_mismatching_records = mismatching_records
                net_mismatching_record = previous_mismatching_records
                list_of_mismatching_columns.append(column)
                list_of_mismatching_counts.append(net_mismatching_record)
                total_mismatching_records += net_mismatching_record

            total_records = mismatching_records
        else:
            list_of_input_columns.append(column)
            mismatching_records = calculate_mismatched_records(
                list_of_input_columns)

            if mismatching_records != total_records:
                list_of_mismatching_columns.append(column)
                net_mismatching_record = mismatching_records - previous_mismatching_records
                previous_mismatching_records += mismatching_records
                list_of_mismatching_counts.append(net_mismatching_record)
                total_mismatching_records += net_mismatching_record

            total_records = mismatching_records

    # CREATING THE SUMMARY DATAFRAME
    print('7. Generating the summary of washevents verification')
    verification_categories = [
        ('Number of records in GP before verification', gp_washevents.count()),
        ('Number of records in AURORA before verification', aurora_washevents.count()),
        ('Number of records present in GP and not in AURORA', df_gp.count()),
        ('Number of records present in AURORA and not in GP', df_aurora.count()),
        ('Total Mismatching records', total_mismatching_records),
    ]

    for i in range(0, len(list_of_mismatching_columns)):
        element = ('Mismatched records in column ({})'.format(
            list_of_mismatching_columns[i].upper), list_of_mismatching_counts[i])
        verification_categories.append(element)

    verification_categories.append(
        ('Timezone difference records', df_timezone.count()))
    verification_categories.append(
        ('Final number of records present in GP and not in AURORA', df_gp_timezone_eliminated.count()))
    verification_categories.append(
        ('Final number of records present in AURORA and not in GP', df_aurora_timezone_eliminated.count()))

    # CREATING A SUMMARY DATAFRAME
    summary = sc.createDataFrame(verification_categories, [
                                 'Verification Categories', 'Counts'])

    # STORING THE FINAL RESULTS IN THE CSV
    print('8. Storing the final results into CSV files')
    writeToCSV(df_gp_timezone_eliminated, output_path['gp_timezone_eliminated'])
    writeToCSV(df_aurora_timezone_eliminated, output_path['aurora_timezone_eliminated'])
    writeToCSV(summary, output_path['summary'])

    return summary


# MVP2 CALCULATIONS VERIFICATIONS
def mvp2_washevents_comparison(washevents_path, output_washevents_path, parameters, separate_store, difference_calculation_parameters):
    global list_of_dates_sample_missing
    global list_of_dates_aurora_missing

    print('************** MVP2 WASHEVENTS VERIFICATION STARTED **************')

    # IMPORTING DATASETS
    print ('1. READING THE DATASETS')
    sample_output = getWashevents(sc, washevents_path['sample_output'])
    aurora_output = getWashevents(sc, washevents_path['aurora_output'])

    # RENAMING THE COLUMNS AND GETTING THE COMMON COLUMNS
    print ('2. GETTING THE COMMON COLUMNS')
    sample_output = sample_output.drop('_c0')
    aurora_output = aurora_output.drop('_c0')

    list_of_columns_sample = set(sample_output.schema.names)
    list_of_columns_aurora = set(aurora_output.schema.names)

    common_columns = list_of_columns_sample.intersection(list_of_columns_aurora)
    list_of_common_columns = list(common_columns)

    sample_output_comp_df = sample_output.select(list_of_common_columns)
    aurora_output_comp_df = aurora_output.select(list_of_common_columns)

    # SORTING THE RECORDS
    sample_output_comp_df = sample_output_comp_df.orderBy( sample_output_comp_df.esn, sample_output_comp_df.event_date)
    aurora_output_comp_df = aurora_output_comp_df.orderBy( aurora_output_comp_df.esn, aurora_output_comp_df.event_date)

    # REMOVING THE TIME FROM DATES
    sample_output_comp_df = sample_output_comp_df.withColumn( 'event_date', substring('event_date', 1, 10))
    sample_output_comp_df = sample_output_comp_df.withColumn( 'flight_dttm', substring('flight_dttm', 1, 10))

    # GETTING THE COMMON DATES AND STORING INTO THE LIST
    list_of_dates_aurora = [date['event_date'] for date in aurora_output_comp_df.select('event_date').collect()]
    list_of_dates_sample = [date['event_date'] for date in sample_output_comp_df.select('event_date').collect()]

    # GETTING THE UNIQUE RECORDS
    missing_sample = sample_output_comp_df.join( aurora_output_comp_df, ['esn', 'event_date'], 'leftanti')
    missing_aurora = aurora_output_comp_df.join( sample_output_comp_df, ['esn', 'event_date'], 'leftanti')

    # CALCULATING THE DATE DIFFERENCE BETWEEN MISSING RECORDS
    print ('3. Calculating the difference between missing events')
    missing_sample_dates = missing_sample.select('esn', 'event_date')
    missing_aurora_dates = missing_aurora.select('esn', 'event_date')

    missing_sample_dates = missing_sample_dates.withColumnRenamed('event_date', 'event_date_sample')
    missing_aurora_dates = missing_aurora_dates.withColumnRenamed('event_date', 'event_date_aurora')

    date_diff_df = missing_sample_dates.join(missing_aurora_dates, 'esn', 'outer')
    date_diff_df = date_diff_df.withColumn('date_diff', datediff(to_date(col('event_date_sample')), to_date(col('event_date_aurora'))))

    # GETTING THE COMMON DATE RECORDS
    print ('4. GETTING THE RECORDS HAVING THE SAME EVENT DATES')
    sample_output_comp_df = sample_output_comp_df.filter(to_date('event_date').isin(list_of_dates_aurora))
    aurora_output_comp_df = aurora_output_comp_df.filter(to_date('event_date').isin(list_of_dates_sample))

    # SORTING THE DATASETS WITH ESN AND EVENT DATE
    sample_output_comp_df = sample_output_comp_df.orderBy(sample_output_comp_df.esn, sample_output_comp_df.event_date)
    aurora_output_comp_df = aurora_output_comp_df.orderBy(aurora_output_comp_df.esn, aurora_output_comp_df.event_date)

    # RENAMING THE COLUMNS
    print ('5. RENAMING THE COLUMNS')
    list_of_common_columns_sample = ['esn', 'event_date']
    list_of_common_columns_aurora = ['esn', 'event_date']

    for column in sample_output_comp_df.schema.names:
        if column == 'esn' or column == 'event_date':
            continue
        else:
            column_name = column + '_sample'
            sample_output_comp_df = sample_output_comp_df.withColumnRenamed(column, column_name)
            list_of_common_columns_sample.append(column_name)

    for column in aurora_output_comp_df.schema.names:
        if column == 'esn' or column == 'event_date':
            continue
        else:
            column_name = column + '_aurora'
            aurora_output_comp_df = aurora_output_comp_df.withColumnRenamed(column, column_name)
            list_of_common_columns_aurora.append(column_name)


    # CONSOLIDATED DATASET TO USE IN COMPARISON
    print ('6. MERGING THE DATASETS')
    comp_df = sample_output_comp_df.join(aurora_output_comp_df, ['esn', 'event_date'], 'left')

    # CALLING THE FUNCTION
    print ('7. COMPARING THE COLUMNS')
    final_comp_df = comparison(comp_df, parameters, output_washevents_path, separate_store, difference_calculation_parameters)

    # SUMMARY
    print ('8. GENERATING THE SUMMARY OF COMPARISON')
    verification_categories = [
        ('Number of sample events', sample_output.count()),
        ('Number of aurora events', aurora_output.count()),
        ('Number of matching events (ESN, EVENT_DATE)', sample_output_comp_df.count()),
        ('Total missing events in sample output', missing_sample.count()),
        ('Total missing events in aurora output', missing_aurora.count()),
    ]

    for parameter in parameters:
        
        comparison_param = parameter + '_comparison'

        element1 = ('Number of mismatched events in column ({}):'.format(parameter.upper()), final_comp_df.filter(col(comparison_param) == 'Not Matched').count())
        element2 = ('Number of matched events in column ({}):'.format(parameter.upper()), final_comp_df.filter(col(comparison_param) == 'Matched').count())
        verification_categories.append(element1)
        verification_categories.append(element2)

    # CREATING A SUMMARY DATAFRAME
    summary = sc.createDataFrame(verification_categories, ['Verification Categories', 'Counts'])

    # STORING THE SUMMARY INTO THE CSV FILE
    print ('9. STORING THE RESULTS INTO CSV FILE')
    writeToCSV(final_comp_df, output_washevents_path['output_path'] + output_washevents_path['common_washevents'])
    writeToCSV(date_diff_df, output_washevents_path['output_path'] + output_washevents_path['date_diff_df'])
    writeToCSV(missing_sample, output_washevents_path['output_path'] + output_washevents_path['missing_events_sample_output'])
    writeToCSV(missing_aurora, output_washevents_path['output_path'] + output_washevents_path['missing_events_aurora_output'])
    writeToCSV(summary, output_washevents_path['output_path'] + output_washevents_path['summary'])

    return verification_categories


# COMPARISON FUNCTION
def comparison(df, parameters, output_washevents_path, separate_store, difference_calculation_parameters):

    # ITERATING THROUGH THE LIST OF PARAMETERS
    for parameter in parameters:
        print ("-------- Comparing {}".format(parameter.upper()))
        sample_column = parameter + '_sample'
        aurora_column = parameter + '_aurora'
        comparison_column = parameter + '_comparison'
        column_difference = parameter + '_difference'

        # COMPARISON OF COLUMN VALUES
        df = df.withColumn(comparison_column, when(col(sample_column) == col(aurora_column), 'Matched').otherwise('Not Matched'))

        if separate_store == 'True':
            if parameter in difference_calculation_parameters:
                df = df.withColumn(column_difference, when(((df[sample_column].isNull()) & (df[aurora_column].isNotNull())), df[aurora_column]).otherwise(
                    when(((df[sample_column].isNotNull()) & (df[aurora_column].isNull())), df[sample_column]).otherwise(
                        when(((df[sample_column].isNotNull()) & (df[aurora_column].isNotNull())), ((abs(df[sample_column] - df[aurora_column]))/df[aurora_column])*100))
                ))

            df_mismatched = df.filter(df[comparison_column] == 'Not Matched').select('esn', 'event_date', sample_column, aurora_column, comparison_column, column_difference)
            df_final_param = df.select('esn', 'event_date', sample_column, aurora_column, comparison_column, column_difference)

            if df_final_param.count() > 0:
                file_name = output_washevents_path['output_path'] + 'mismatching_events_' + parameter + '.csv'
                writeToCSV(df_final_param, file_name)
        else:
            df_mismatched = df.filter(df[comparison_column] == 'Not Matched').select('esn', 'event_date', sample_column, aurora_column, comparison_column)
            df_final_param = df.select('esn', 'event_date', sample_column, aurora_column, comparison_column)

            if df_final_param.count() > 0:
                file_name = output_washevents_path['output_path'] + 'mismatching_events_' + parameter + '.csv'
                writeToCSV(df_final_param, file_name)
    return df
