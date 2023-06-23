import json
import numpy as np
import pandas as pd
import sqlalchemy as sd
import sys
import os
import boto3
import urllib3
from io import StringIO

def lambda_handler(event, context):
    ssm = boto3.client("ssm")
    s3_client = boto3.client('s3')
    slack_fail = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
    slack_pass = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
    pd.options.mode.chained_assignment = None  # default='warn'
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    sql_column_df, engine, conn = connect_to_sql_db(host_client, user_name, user_password, "seronetdb-Vaccine_Response")
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    file_path = event["Records"][0]["s3"]["object"]["key"]
    file_path = file_path.replace("+", " ")
    error_list = pd.DataFrame(columns=("Comorbidity", "Disease_Name"))
    try:
        #norm_table = pd.read_csv(file_path, encoding='windows-1252')
        resp = s3_client.get_object(Bucket=bucket, Key=file_path)
        print(resp['Body'])
        if file_path.endswith(".csv"):
            data = resp['Body'].read()
            data_string = data.decode('windows-1252')
            #norm_table = pd.read_csv(resp['Body'], na_filter=False, encoding='windows-1252')
            norm_table = pd.read_csv(StringIO(data_string), na_filter=False, sep=',')
            print(norm_table)
        elif file_path.endswith(".xlsx"):
            norm_table = pd.read_excel(resp['Body'].read(), sheet_name="Cancer_Cohort_harmonized", engine='openpyxl')
        if "Comorbidity" in file_path:   # Ex: Reported_Comorbidity_Names_Release_2.0_harmonized 17Aug2022
            result_msg, error_list = Normalized_Comorbidities(norm_table, pd, conn)
        if "Cancer" in file_path: # Ex:  Cancer_Cohort_harmonized 11Aug2021
            result_msg = Normalized_Cancer(norm_table, pd, conn)
        if "Treatment" in file_path:  # Ex:  Reported_Condition_Treatment_Release_1.0_harmonized_16Aug2022
            result_msg = Normalized_Treatment(norm_table, pd, conn)
        conn.connection.commit()
        file_name = os.path.basename(file_path)
        message_slack_pass = f"Start uploading file {file_name} to the dictionary database \n"
        for msg in result_msg:
            message_slack_pass = message_slack_pass + msg + "\n"
        if len(error_list) > 0:
            error_list = error_list.drop_duplicates()
            csv_buffer = error_list.to_csv(index=False).encode()
            s3_client = boto3.client('s3')
            s3_client.put_object(Body=csv_buffer, Bucket='seronet-trigger-submissions-passed', Key='Dictionary_Files/error_list.csv')
            message_slack_pass = message_slack_pass + f"Terms that are not found in Normalized_Description are saved in aws s3 seronet-trigger-submissions-passed/Dictionary_Files/error_list.csv"
        print(message_slack_pass)
        write_to_slack(message_slack_pass, slack_pass)
    except Exception as e:
        display_error_line(e)
        file_name = os.path.basename(file_path)
        message_slack_fail = f"Start uploading file {file_name} to the dictionary database \n" + str(e)
        write_to_slack(message_slack_fail, slack_fail)

    finally:
        print('## Database has been checked.  Closing the connections ##')

        if conn:
            conn.close()


    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def connect_to_sql_db(host_client, user_name, user_password, file_dbname):

    sql_column_df = pd.DataFrame(columns=["Table_Name", "Column_Name", "Var_Type", "Primary_Key", "Autoincrement",
                                          "Foreign_Key_Table", "Foreign_Key_Column"])
    creds = {'usr': user_name, 'pwd': user_password, 'hst': host_client, "prt": 3306, 'dbn': file_dbname}
    connstr = "mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}"
    engine = sd.create_engine(connstr.format(**creds))
    engine = engine.execution_options(autocommit=False)
    conn = engine.connect()
    metadata = sd.MetaData()
    metadata.reflect(engine)

    for t in metadata.tables:
        try:
            curr_table = metadata.tables[t]
            curr_table = curr_table.columns.values()
            for curr_row in range(len(curr_table)):
                curr_dict = {"Table_Name": t, "Column_Name": str(curr_table[curr_row].name),
                             "Var_Type": str(curr_table[curr_row].type),
                             "Primary_Key": str(curr_table[curr_row].primary_key),
                             "Autoincrement": False,
                             "Foreign_Key_Count": 0,
                             "Foreign_Key_Table": 'None',
                             "Foreign_Key_Column": 'None'}
                curr_dict["Foreign_Key_Count"] = len(curr_table[curr_row].foreign_keys)
                if curr_table[curr_row].autoincrement is True:
                    curr_dict["Autoincrement"] = True
                if len(curr_table[curr_row].foreign_keys) == 1:
                    key_relation = list(curr_table[curr_row].foreign_keys)[0].target_fullname
                    key_relation = key_relation.split(".")
                    curr_dict["Foreign_Key_Table"] = key_relation[0]
                    curr_dict["Foreign_Key_Column"] = key_relation[1]


                sql_column_df = pd.concat([sql_column_df, pd.DataFrame.from_records([curr_dict])])
        except Exception as e:
            display_error_line(e)
    print("## Sucessfully Connected to " + file_dbname + " ##")
    sql_column_df.reset_index(inplace=True, drop=True)
    return sql_column_df, engine, conn

def clean_table(norm_table):
    norm_table.fillna("No Data", inplace=True)
    for col_name in norm_table:
        if "Index" not in col_name:
            norm_table[col_name] = norm_table[col_name].str.strip()
    norm_table = norm_table.drop_duplicates()
    return norm_table

def get_to_add(input_data, sql_df, prim_key):
    merge_data = input_data.merge(sql_df, how="left", indicator=True)
    merge_data = merge_data.query("_merge == 'left_only'")  # if both then already done
    merge_data = merge_data[input_data.columns]
    merge_data.reset_index(inplace=True, drop=True)
    z = merge_data[prim_key].merge(sql_df, how="left", on=prim_key, indicator=True)

    new_data = z.query("_merge not in ['both']")
    if len(new_data) > 0:
        new_data = merge_data.iloc[new_data.index]        # primary key not in db (new record)
        new_data.drop_duplicates(inplace=True)

    update_data = z.query("_merge in ['both']")
    if len(update_data) > 0:

        update_data = merge_data.iloc[update_data.index]  # primary key found but record changed (update)
        update_data.drop_duplicates(inplace=True)

    return new_data, update_data


def Normalized_Comorbidities(norm_table, pd, conn):
    result_msg = []
    error_list = pd.DataFrame(columns=("Comorbidity", "Disease_Name"))
    table_name = "Normalized_Comorbidity_Dictionary"
    norm_table = norm_table[['Comorbid_Name', 'Orgional_Description_Or_ICD10_codes', 'Normalized_Description']]
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Comorbidity_Dictionary;"), conn)
    norm_table = clean_table(norm_table)
    norm_dict = clean_table(norm_dict)

    new_data, update_data = get_to_add(norm_table, norm_dict, ['Comorbid_Name', 'Orgional_Description_Or_ICD10_codes'])
    col_names = ", ".join(["`" + i + "`" for i in norm_dict.columns])
    add_count, result_msg = add_new_rows(conn, new_data, table_name, col_names, result_msg)

    update_count, result_msg = update_tables(conn, ['Comorbid_Name', 'Orgional_Description_Or_ICD10_codes'], update_data, "Normalized_Comorbidity_Dictionary", result_msg)

    if add_count == -1 or update_count == -1:   #error updating dictionary, do not make tables
        #return add_count, update_count, table_name
        result_msg.append("error updating dictionary, do not make tables")
        return result_msg, error_list

    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Comorbidity_Dictionary;"), conn)                          #normalized dictoinary
    norm_db = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Normalized_Comorbidity_Names;"), conn)    #harmonized output table

    org_names = pd.read_sql(("SELECT * FROM Comorbidities_Names;"), conn)           #orional Names of the Comorbidities
    org_cond = pd.read_sql(("SELECT * FROM Participant_Comorbidities;"), conn)      #yes/no/ condition status flag

    normalized_df = pd.DataFrame()
    normalized_df["Visit_Info_ID"] = org_names["Visit_Info_ID"]
    norm_dict["Normalized_Description"] = norm_dict["Normalized_Description"].str.strip()
    norm_dict.fillna("No Data", inplace=True)
    org_names.fillna("No Data", inplace=True)
    comorbid_cols = org_cond.columns.tolist()

    for curr_cond in comorbid_cols:
        col_name = [i for i in org_names.columns if curr_cond in i]
        x = norm_dict.query("Comorbid_Name in @curr_cond")

        if len(x) > 0:
            try:
                x["Orgional_Description_Or_ICD10_codes"] = x["Orgional_Description_Or_ICD10_codes"].str.lower()
                normalized_df[curr_cond + "_Description_Normalized"] = ""
                org_names[col_name[0]] = org_names[col_name[0]].str.lower()
                org_names[col_name[0]] = org_names[col_name[0]].str.replace("crohns", "crohn's")
                org_names[col_name[0]] = org_names[col_name[0]].str.replace("hashimotos", "hashimoto's")

                for index in org_names.index:
                    split_names = org_names[col_name[0]][index].split("|")
                    split_names = [i.strip() for i in split_names]
                    norm_term = x.query("Orgional_Description_Or_ICD10_codes in @split_names")["Normalized_Description"]
                    norm_term = list(set(norm_term))
                    norm_term.sort()
                    if len(norm_term) == 0:
                        print(f"{split_names} was not found in {curr_cond}")
                        err_df = pd.DataFrame({"Comorbidity":[curr_cond]*len(split_names), "Disease_Name":split_names})
                        error_list = pd.concat([error_list, err_df])
                    normalized_df[curr_cond + "_Description_Normalized"][index] = " | ".join(norm_term)
            except Exception as e:
                print(e)

    normalized_df.replace("No Data", np.nan, inplace=True)
    normalized_df.rename(columns={"Viral_Infection_Description_Normalized": "Viral_Infection_Normalized"}, inplace=True)
    normalized_df.rename(columns={"Bacterial_Infection_Description_Normalized": "Bacterial_Infection_Normalized"}, inplace=True)
    normalized_df.fillna('Participant Does Not Have', inplace=True)

    new_data, update_data = get_to_add(normalized_df, norm_db, ['Visit_Info_ID'])
    col_names = ", ".join(["`" + i + "`" for i in new_data.columns])
    add_count, result_msg = add_new_rows(conn, new_data, "Normalized_Comorbidity_Names", col_names, result_msg)
    update_count, result_msg = update_tables(conn,  ['Visit_Info_ID'], update_data, "Normalized_Comorbidity_Names", result_msg)
    return result_msg, error_list

def Normalized_Treatment(norm_table, pd, conn):
    table_name = "Normalized_Treatment_Dict"
    norm_table = norm_table[['Treatment', 'Harmonized Treatment']]
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Treatment_Dict;"), conn)
    norm_table = clean_table(norm_table)
    norm_dict = clean_table(norm_dict)
    result_msg = []
    new_data, update_data = get_to_add(norm_table, norm_dict, ["Treatment"])

    add_count, result_msg = add_new_rows(conn, new_data, "Normalized_Treatment_Dict", "`Treatment`, `Harmonized Treatment`", result_msg)
    update_count, result_msg = update_tables(conn, ["Treatment"], update_data, "Normalized_Treatment_Dict", result_msg)

    if add_count == -1 or update_count == -1:   #error updating dictionary, do not make tables
        #return add_count, update_count, table_name
        result_msg.append("error updating dictionary, do not make tables")
        return result_msg

    org_names = pd.read_sql(("SELECT Visit_Info_ID, Health_Condition_Or_Disease, Treatment FROM Treatment_History;"), conn)
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Treatment_Dict;"), conn)
    curr_norm = pd.read_sql(("SELECT * FROM Normalized_Treatment_Names"), conn)

    org_names.rename(columns={"Treatment": "Original Treatment Name"}, inplace=True)
    norm_dict.rename(columns={"Treatment": "Original Treatment Name"}, inplace=True)

    merge_data = org_names.merge(norm_dict, how="left")
    merge_data.drop("Normalized_Index", axis=1, inplace=True)
    new_data, update_data = get_to_add(merge_data, curr_norm, ["Visit_Info_ID", "Original Treatment Name"])

    col_names = ", ".join(["`" + i + "`" for i in new_data.columns])
    add_count, result_msg = add_new_rows(conn, new_data, "Normalized_Treatment_Names", col_names, result_msg)
    update_count, result_msg = update_tables(conn, ["Visit_Info_ID", "Original Treatment Name"], update_data, "Normalized_Treatment_Names", result_msg)
    return result_msg


def Normalized_Cancer(norm_table, pd, conn):
    result_msg = []
    table_name = "Normalized_Cancer_Dictionary"
    norm_table = norm_table[['Cancer', 'Harmonized Cancer Name', 'SEER Category']]
    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Cancer_Dictionary;"),conn)
    norm_table = clean_table(norm_table)
    norm_dict = clean_table(norm_dict)

    new_data, update_data = get_to_add(norm_table, norm_dict, ["Cancer"])

    add_count = add_new_rows(conn, new_data, "Normalized_Cancer_Dictionary", "`Cancer`, `Harmonized Cancer Name`, `SEER Category`", result_msg)
    update_count = update_tables(conn, ["Cancer"], update_data, "Normalized_Cancer_Dictionary", result_msg)

    if add_count == -1 or update_count == -1:   #error updating dictionary, do not make tables
      #return add_count, update_count, table_name
      result_msg.append("error updating dictionary, do not make tables")
      return result_msg

    org_names = pd.read_sql(("SELECT Visit_Info_ID, Cancer_Description_Or_ICD10_codes FROM `seronetdb-Vaccine_Response`.Comorbidities_Names;"), conn)
    has_cancer = pd.read_sql(("SELECT Visit_Info_ID, Cancer FROM `seronetdb-Vaccine_Response`.Participant_Comorbidities where Cancer not in ('No');"), conn)
    org_names = org_names.merge(has_cancer)
    org_names.fillna("Not Reported", inplace=True)

    norm_dict = pd.read_sql(("SELECT * FROM Normalized_Cancer_Dictionary;"), conn)
    curr_norm = pd.read_sql(("SELECT * FROM Normalized_Cancer_Names_v2"), conn)

    org_names.rename(columns={"Cancer_Description_Or_ICD10_codes": "Original Cancer Name"}, inplace=True)
    norm_dict.rename(columns={"Cancer": "Original Cancer Name"}, inplace=True)

    merge_data = org_names.merge(norm_dict, how="left")
    merge_data.drop("Cancer", axis=1, inplace=True)
    new_data, update_data = get_to_add(merge_data, curr_norm, ["Visit_Info_ID", "Original Cancer Name"])

    x = new_data.query("`{0}` != `{0}`".format("Harmonized Cancer Name"))
    if len(x) > 0:
        print(f"There are {len(x)} new terms missing")
        result_msg.append(f"There are {len(x)} new terms missing")

    col_names = ", ".join(["`" + i + "`" for i in new_data.columns])
    add_count, result_msg = add_new_rows(conn, new_data, "Normalized_Cancer_Names_v2", col_names, result_msg)
    update_count, result_msg = update_tables(conn, ["Visit_Info_ID", "Original Cancer Name"], update_data, "Normalized_Cancer_Names_v2", result_msg)
    return result_msg



def add_new_rows(sql_connect, new_data, table_name, col_names, result_msg):
    add_count = len(new_data)
    print("There are " + str(add_count) + " new records to add to the database")
    result_msg.append("There are " + str(add_count) + " new records to add to the database")

    if len(new_data) == 0:
        return add_count, result_msg
    for index in new_data.index:
        try:
            curr_data = new_data.loc[index].values.tolist()
            curr_data = ["\"" + str(s) + "\"" for s in curr_data]
            curr_data = ', '.join(curr_data)

            sql_query = sd.text(f"INSERT INTO {table_name} ({col_names}) VALUES ({curr_data})")
            sql_connect.execute(sql_query)
        except sd.exc.IntegrityError:
            continue  #record already exists
        except Exception as e:
            print ("Unexpected error:", sys.exc_info()[0])
            print(col_names)
            print(curr_data)
            display_error_line(e)
            print(e)
            add_count = -1
            break
        #finally:
            #conn.connection.commit()
    return  add_count, result_msg


def update_tables(sql_connect, primary_keys, update_table, sql_table, result_msg):
    update_count = len(update_table)
    print("There are " + str(update_count) + " records that need to be updated")
    result_msg.append("There are " + str(update_count) + " records that need to be updated")
    if len(update_table) == 0:
        return update_count, result_msg

    key_str = ['`' + str(s) + '`' + " like '%s'" for s in primary_keys]
    key_str = " and ".join(key_str)
    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            primary_value = update_table.loc[index, primary_keys].values.tolist()
            update_str = ["`" + i + '` = "' + str(j) + '"' for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)

            update_str = update_str.replace('-1000000000', "N/A")
            update_str = update_str.replace("N/A", str(np.nan))
            update_str = update_str.replace("nan", "NULL")
            update_str = update_str.replace('"nan"', "NULL")

            update_str = update_str.replace('"NULL"',"NULL")

            sql_query = sd.text(f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            sql_connect.execute(sql_query)
        except Exception as e:
            print(e)
            update_count = -1
            print(update_table)
            display_error_line(e)
        #finally:
            #conn.connection.commit()
    return update_count, result_msg

def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))

def write_to_slack(message_slack, slack_chanel):
    http = urllib3.PoolManager()
    data={"text": message_slack}
    r=http.request("POST", slack_chanel, body=json.dumps(data), headers={"Content-Type":"application/json"})
