"""
INSERTING PHONEPE PULSE DATA INTO MySQL
"""

#-----------MODULES-USED-------------
import os
import git
import json
import pandas as pd
import mysql.connector as mc


#--------------SQL-CONNECTION---------------
def get_mysql_connection():
    return mc.connect(
        host="localhost",
        port="3306",
        user="root",
        passwd="saipavan55",
        database="phonepe",
        auth_plugin='mysql_native_password'
    )


#-------------CLONING-GITHUB-REPOSITORY------------
def clone_repository():
    repo_url = "https://github.com/PhonePe/pulse.git"
    clone_dir = "saipavanclone"
    git.Repo.clone_from(repo_url, clone_dir)


#--------------AGGREGATED_INSURANCE---------------
def agg_ins():
    path_agg_ins = "C:/Users/saipa/Downloads/saipavanclone/data/aggregated/insurance/country/india/state/"
    agg_insur_list= os.listdir(path_agg_ins)
    columns_agg_ins = {"States":[], "Years":[], "Quarter":[], "Insurance_type":[], "Insurance_count":[],"Insurance_amount":[]}
    for state in agg_insur_list:
        cur_states =path_agg_ins+state+"/"
        agg_year_list = os.listdir(cur_states)
        for year in agg_year_list:
            cur_years = cur_states+year+"/"
            agg_file_list = os.listdir(cur_years)
            for file in agg_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                for i in dat["data"]["transactionData"]:
                    name = i["name"]
                    count = i["paymentInstruments"][0]["count"]
                    amount = i["paymentInstruments"][0]["amount"]
                    columns_agg_ins["Insurance_type"].append(name)
                    columns_agg_ins["Insurance_count"].append(count)
                    columns_agg_ins["Insurance_amount"].append(amount)
                    columns_agg_ins["States"].append(state)
                    columns_agg_ins["Years"].append(year)
                    columns_agg_ins["Quarter"].append(int(file.strip(".json")))
    aggre_insurance = pd.DataFrame(columns_agg_ins)
    aggre_insurance["States"] = aggre_insurance["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    aggre_insurance["States"] = aggre_insurance["States"].str.replace("-"," ")
    aggre_insurance["States"] = aggre_insurance["States"].str.title()
    aggre_insurance['States'] = aggre_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return aggre_insurance


#-------------AGGREGATED-TRANSACTION--------------
def agg_tran():
    path_agg_tran = "C:/Users/saipa/Downloads/saipavanclone/data/aggregated/transaction/country/india/state/"
    agg_tran_list = os.listdir(path_agg_tran)
    columns_agg_tran ={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[],"Transaction_amount":[] }
    for state in agg_tran_list:
        cur_states =path_agg_tran+state+"/"
        agg_year_list = os.listdir(cur_states)
        for year in agg_year_list:
            cur_years = cur_states+year+"/"
            agg_file_list = os.listdir(cur_years)
            for file in agg_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                for i in dat["data"]["transactionData"]:
                    name = i["name"]
                    count = i["paymentInstruments"][0]["count"]
                    amount = i["paymentInstruments"][0]["amount"]
                    columns_agg_tran["Transaction_type"].append(name)
                    columns_agg_tran["Transaction_count"].append(count)
                    columns_agg_tran["Transaction_amount"].append(amount)
                    columns_agg_tran["States"].append(state)
                    columns_agg_tran["Years"].append(year)
                    columns_agg_tran["Quarter"].append(int(file.strip(".json")))
    aggre_transaction = pd.DataFrame(columns_agg_tran)
    aggre_transaction["States"] = aggre_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    aggre_transaction["States"] = aggre_transaction["States"].str.replace("-"," ")
    aggre_transaction["States"] = aggre_transaction["States"].str.title()
    aggre_transaction['States'] = aggre_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return aggre_transaction


#------------------AGGREGATED-USER---------------------
def agg_user():
    path_agg_user = "C:/Users/saipa/Downloads/saipavanclone/data/aggregated/user/country/india/state/"
    agg_user_list = os.listdir(path_agg_user)
    columns_agg_user = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}
    for state in agg_user_list:
        cur_states = path_agg_user+state+"/"
        agg_year_list = os.listdir(cur_states)
        for year in agg_year_list:
            cur_years = cur_states+year+"/"
            agg_file_list = os.listdir(cur_years)
            for file in agg_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                try:
                    for i in dat["data"]["usersByDevice"]:
                        brand = i["brand"]
                        count = i["count"]
                        percentage = i["percentage"]
                        columns_agg_user["Brands"].append(brand)
                        columns_agg_user["Transaction_count"].append(count)
                        columns_agg_user["Percentage"].append(percentage)
                        columns_agg_user["States"].append(state)
                        columns_agg_user["Years"].append(year)
                        columns_agg_user["Quarter"].append(int(file.strip(".json")))
                except:
                    pass
    aggre_user = pd.DataFrame(columns_agg_user)
    aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    aggre_user["States"] = aggre_user["States"].str.replace("-"," ")
    aggre_user["States"] = aggre_user["States"].str.title()
    aggre_user['States'] = aggre_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return aggre_user


#-----------------MAP-INSURANCE-----------------------
def map_insur():
    path_map_ins = "C:/Users/saipa/Downloads/saipavanclone/data/map/insurance/hover/country/india/state/"
    map_insur_list = os.listdir(path_map_ins)
    columns_map_insur= {"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[],"Transaction_amount":[] }
    for state in map_insur_list:
        cur_states =path_map_ins+state+"/"
        agg_year_list = os.listdir(cur_states)
        for year in agg_year_list:
            cur_years = cur_states+year+"/"
            agg_file_list = os.listdir(cur_years)
            for file in agg_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                for i in dat["data"]["hoverDataList"]:
                    name = i["name"]
                    count = i["metric"][0]["count"]
                    amount = i["metric"][0]["amount"]
                    columns_map_insur["Districts"].append(name)
                    columns_map_insur["Transaction_count"].append(count)
                    columns_map_insur["Transaction_amount"].append(amount)
                    columns_map_insur["States"].append(state)
                    columns_map_insur["Years"].append(year)
                    columns_map_insur["Quarter"].append(int(file.strip(".json")))
    map_insurance = pd.DataFrame(columns_map_insur)
    map_insurance["States"] = map_insurance["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    map_insurance["States"] = map_insurance["States"].str.replace("-"," ")
    map_insurance["States"] = map_insurance["States"].str.title()
    map_insurance['States'] = map_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return map_insurance


#-----------------MAP-TRANSACTION-------------------
def map_tran():
    path_map_tran = "C:/Users/saipa/Downloads/saipavanclone/data/map/transaction/hover/country/india/state/"
    map_tran_list = os.listdir(path_map_tran)
    columns_map_tran = {"States":[], "Years":[], "Quarter":[],"District":[], "Transaction_count":[],"Transaction_amount":[]}
    for state in map_tran_list:
        cur_states = path_map_tran+state+"/"
        map_year_list = os.listdir(cur_states)
        for year in map_year_list:
            cur_years = cur_states+year+"/"
            map_file_list = os.listdir(cur_years)
            for file in map_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                for i in dat['data']["hoverDataList"]:
                    name = i["name"]
                    count = i["metric"][0]["count"]
                    amount = i["metric"][0]["amount"]
                    columns_map_tran["District"].append(name)
                    columns_map_tran["Transaction_count"].append(count)
                    columns_map_tran["Transaction_amount"].append(amount)
                    columns_map_tran["States"].append(state)
                    columns_map_tran["Years"].append(year)
                    columns_map_tran["Quarter"].append(int(file.strip(".json")))
    map_transaction = pd.DataFrame(columns_map_tran)
    map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
    map_transaction["States"] = map_transaction["States"].str.title()
    map_transaction['States'] = map_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return map_transaction


#------------------MAP-USER-----------------------
def map_user():
    path_map_user = "C:/Users/saipa/Downloads/saipavanclone/data/map/user/hover/country/india/state/"
    map_user_list = os.listdir(path_map_user)
    columns_map_user = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}
    for state in map_user_list:
        cur_states = path_map_user+state+"/"
        map_year_list = os.listdir(cur_states)
        for year in map_year_list:
            cur_years = cur_states+year+"/"
            map_file_list = os.listdir(cur_years)
            for file in map_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                for i in dat["data"]["hoverData"].items():
                    district = i[0]
                    registereduser = i[1]["registeredUsers"]
                    appopens = i[1]["appOpens"]
                    columns_map_user["Districts"].append(district)
                    columns_map_user["RegisteredUser"].append(registereduser)
                    columns_map_user["AppOpens"].append(appopens)
                    columns_map_user["States"].append(state)
                    columns_map_user["Years"].append(year)
                    columns_map_user["Quarter"].append(int(file.strip(".json")))
    map_user = pd.DataFrame(columns_map_user)
    map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    map_user["States"] = map_user["States"].str.replace("-"," ")
    map_user["States"] = map_user["States"].str.title()
    map_user['States'] = map_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return map_user


#-----------------TOP-INSURANCE------------------
def top_insura():
    path_top_insur = "C:/Users/saipa/Downloads/saipavanclone/data/top/insurance/country/india/state/"
    top_insur_list = os.listdir(path_top_insur)
    columns_top_insur = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
    for state in top_insur_list:
        cur_states = path_top_insur+state+"/"
        top_year_list = os.listdir(cur_states)
        for year in top_year_list:
            cur_years = cur_states+year+"/"
            top_file_list = os.listdir(cur_years)
            for file in top_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                for i in dat["data"]["pincodes"]:
                    entityName = i["entityName"]
                    count = i["metric"]["count"]
                    amount = i["metric"]["amount"]
                    columns_top_insur["Pincodes"].append(entityName)
                    columns_top_insur["Transaction_count"].append(count)
                    columns_top_insur["Transaction_amount"].append(amount)
                    columns_top_insur["States"].append(state)
                    columns_top_insur["Years"].append(year)
                    columns_top_insur["Quarter"].append(int(file.strip(".json")))
    top_insur = pd.DataFrame(columns_top_insur)
    top_insur["States"] = top_insur["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    top_insur["States"] = top_insur["States"].str.replace("-"," ")
    top_insur["States"] = top_insur["States"].str.title()
    top_insur['States'] = top_insur['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return top_insur


#-----------------TOP-TRANSACTION-------------------
def top_tran():
    path_top_tran = "C:/Users/saipa/Downloads/saipavanclone/data/top/transaction/country/india/state/"
    top_tran_list = os.listdir(path_top_tran)
    columns_top_tran = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
    for state in top_tran_list:
        cur_states = path_top_tran+state+"/"
        top_year_list = os.listdir(cur_states)
        for year in top_year_list:
            cur_years = cur_states+year+"/"
            top_file_list = os.listdir(cur_years)
            for file in top_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                for i in dat["data"]["pincodes"]:
                    entityName = i["entityName"]
                    count = i["metric"]["count"]
                    amount = i["metric"]["amount"]
                    columns_top_tran["Pincodes"].append(entityName)
                    columns_top_tran["Transaction_count"].append(count)
                    columns_top_tran["Transaction_amount"].append(amount)
                    columns_top_tran["States"].append(state)
                    columns_top_tran["Years"].append(year)
                    columns_top_tran["Quarter"].append(int(file.strip(".json")))
    top_transaction = pd.DataFrame(columns_top_tran)
    top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
    top_transaction["States"] = top_transaction["States"].str.title()
    top_transaction['States'] = top_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return top_transaction


#---------------TOP-USER-----------------
def top_usera():
    path_top_user = "C:/Users/saipa/Downloads/saipavanclone/data/top/user/country/india/state/"
    top_user_list = os.listdir(path_top_user)
    columns_top_user = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}
    for state in top_user_list:
        cur_states = path_top_user+state+"/"
        top_year_list = os.listdir(cur_states)
        for year in top_year_list:
            cur_years = cur_states+year+"/"
            top_file_list = os.listdir(cur_years)
            for file in top_file_list:
                cur_files = cur_years+file
                data = open(cur_files,"r")
                dat = json.load(data)
                for i in dat["data"]["pincodes"]:
                    name = i["name"]
                    registeredusers = i["registeredUsers"]
                    columns_top_user["Pincodes"].append(name)
                    columns_top_user["RegisteredUser"].append(registeredusers)
                    columns_top_user["States"].append(state)
                    columns_top_user["Years"].append(year)
                    columns_top_user["Quarter"].append(int(file.strip(".json")))
    top_user = pd.DataFrame(columns_top_user)
    top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
    top_user["States"] = top_user["States"].str.replace("-"," ")
    top_user["States"] = top_user["States"].str.title()
    top_user['States'] = top_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    return top_user


#----------------INSERTING-DATA-AGG-INSUR--------------------
def insert_agg_insur(cursor, aggre_insurance):
    for index,row in aggre_insurance.iterrows():
        query = '''INSERT INTO aggregated_insurance (States, Years, Quarter, Insurance_type, Insurance_count, Insurance_amount)
                                                            values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Insurance_type"],
                row["Insurance_count"],
                row["Insurance_amount"]
                )
        cursor.execute(query,values)


#---------------INSERTING-DATA-AGG-TRAN--------------------
def insert_agg_tran(cursor, aggre_transaction):
    for index,row in aggre_transaction.iterrows():
        query = '''INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                            values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Transaction_type"],
                row["Transaction_count"],
                row["Transaction_amount"]
                )
        cursor.execute(query,values)


#------------------INSERTING-DATA-AGG-USER--------------------
def insert_agg_user(cursor, aggre_user):
    for index,row in aggre_user.iterrows():
        query = '''INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
                                                        values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Brands"],
                row["Transaction_count"],
                row["Percentage"])
        cursor.execute(query,values)


#----------------INSERTING-DATA-MAP-INSUR--------------------
def insert_map_insur(cursor, map_insurance):
    for index,row in map_insurance.iterrows():
                query = '''
                    INSERT INTO map_insurance (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                    VALUES (%s, %s, %s, %s, %s, %s)

                '''
                values = (
                    row['States'],
                    row['Years'],
                    row['Quarter'],
                    row['Districts'],
                    row['Transaction_count'],
                    row['Transaction_amount']
                )
                cursor.execute(query,values)


#------------------INSERTING-DATA-MAP-TRAN--------------------
def insert_map_tran(cursor, map_transaction):
    for index,row in map_transaction.iterrows():
                query = '''
                    INSERT INTO map_Transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                    VALUES (%s, %s, %s, %s, %s, %s)

                '''
                values = (
                    row['States'],
                    row['Years'],
                    row['Quarter'],
                    row['District'],
                    row['Transaction_count'],
                    row['Transaction_amount']
                )
                cursor.execute(query,values)


#------------------INSERTING-DATA-MAP-USER--------------------
def insert_map_user(cursor, map_user):
    for index,row in map_user.iterrows():
        query = '''INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                            values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Districts"],
                row["RegisteredUser"],
                row["AppOpens"])
        cursor.execute(query,values)


#------------------INSERTING-DATA-TOP-INSUR--------------------
def insert_top_insur(cursor, top_insur):
    for index,row in top_insur.iterrows():
        query = '''INSERT INTO top_insurance (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                                                        values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Pincodes"],
                row["Transaction_count"],
                row["Transaction_amount"])
        cursor.execute(query,values)


#------------------INSERTING-DATA-TOP-TRAN--------------------
def insert_top_tran(cursor, top_transaction):
    for index,row in top_transaction.iterrows():
        query = '''INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                                                        values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Pincodes"],
                row["Transaction_count"],
                row["Transaction_amount"])
        cursor.execute(query,values)


#------------------INSERTING-DATA-TOP-USER--------------------
def insert_top_user(cursor, top_user):
    for index,row in top_user.iterrows():
        query = '''INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                                                values(%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Pincodes"],
                row["RegisteredUser"])
        cursor.execute(query,values)


#---------------MAIN-FUNCTION---------------
def main():
    mycon = get_mysql_connection()
    cursor = mycon.cursor()
    aggre_insurance = agg_ins()
    aggre_transaction = agg_tran()
    aggre_user = agg_user()
    map_insurance = map_insur()
    map_transaction = map_tran()
    map_use = map_user()
    top_insur = top_insura()
    top_transaction = top_tran()
    top_user = top_usera()
    insert_agg_insur(cursor, aggre_insurance)
    insert_agg_tran(cursor, aggre_transaction)
    insert_agg_user(cursor, aggre_user)
    insert_map_insur(cursor, map_insurance)
    insert_map_tran(cursor, map_transaction)
    insert_map_user(cursor, map_use)
    insert_top_insur(cursor, top_insur)
    insert_top_tran(cursor, top_transaction)
    insert_top_user(cursor, top_user)
    mycon.commit()
    mycon.close()


#---------------CALL-MAIN-----------------   
main()
