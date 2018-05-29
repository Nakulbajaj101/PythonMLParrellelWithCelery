import pandas as pd
from sklearn.ensemble import RandomForestClassifier as RFC
from sklearn.cross_validation import train_test_split
import numpy as np
from utilities import executing_reading_data as ERD
from utilities import executing_writing_data as EWD
from settings import location_type, import_settings
from utilities import read_data_from_bq




def data_format(data, chunksize = 10, columnname = "churner", agg_func = sum):
    #function to create data partitions and get aggregated values for the partition
    df = pd.DataFrame()
    index = 0
    data_chunk = int(data.shape[0]/chunksize)
    df1 = []
    for i in range (1, chunksize + 1):
        datapartition = data.copy()
        datapartition = datapartition[index*data_chunk:data_chunk*i]
        datapartition["number"] = i
        datapartition = datapartition.groupby(["number"]).agg({columnname:agg_func}).reset_index()
        df1.append(datapartition)
        index += 1
    df = pd.concat(df1, axis=0, ignore_index=True)
    df = df.drop(["number"],1)
    return df



def analysis_data(data, codecolumnprefix = "rfmcode_", churncolumn = "churner", probabilitycolumn = "probability_model", predictcolumn = "predicted_churner"):
    dataextract = data[[i for i in data if ((codecolumnprefix in i) or (churncolumn in i or probabilitycolumn in i)) and (predictcolumn not in i)]]
    columns = [i for i in dataextract if churncolumn not in i] + [churncolumn]
    dataextract = dataextract[columns]
    del data

    df = pd.DataFrame()
    df1 = []
    for i in range (0, dataextract.shape[1]-1):
        data = dataextract[[dataextract.columns[i],churncolumn]]
        if probabilitycolumn in data:
            data = data.sort_values(dataextract.columns[i], ascending=False).reset_index().drop(["index"],1)
        else:
            data = data.sort_values(dataextract.columns[i], ascending=True).reset_index().drop(["index"],1)
        data = data_format(data)
        data = data.rename(columns = {churncolumn :dataextract.columns[i]})
        df1.append(data)
    df = pd.concat(df1, axis = 1)
    del df1
    df1 = df.copy()

    #creating data into plot format
    index = 0
    for i in range (0,df.shape[1]):
        for j in range (0, 10):
            df1[df.columns[i]][j] = ((np.float(df[df.columns[i]][index:j+1].sum()))/np.float((df[df.columns[i]].sum())))*100
    df1["general"] = [(i+1)*10 for i, j in enumerate  (df1[df1.columns[0]])]
    return df1

def top_companies(from_location = 'bq', location = 'location', schema = 'Automate_IIQ_Dev', maximum = 3000):
    query = """select a.cmpnyid from (select distinct cmpnyid from `{}.cc_churn_company_customer_churncodes_result`) a inner join
                (
                select cmpnyid from
                (select cmpnyid, rank() over (order by txnval_total desc) as ranking, txnval_total from `{}.cc_feature_company` )
                where ranking <= {}) b
                on a.cmpnyid = b.cmpnyid""".format(schema, schema, maximum)
    settings_location = location_type(from_location)
    settings = import_settings(settings_location)
    data = read_data_from_bq(query = query, querylocation=location, settings=settings)
    data = [i for i in data[data.columns[0]]]
    return data


def preprocess_data(X, churncolumn = "churner"):
    data = X.copy()
    traindata = data[[i for i in data if "cmpnyid" not in i and "custid" not in i and "custpstcd" not in i]]
    traindata = pd.get_dummies(data = traindata, prefix_sep="_", columns = ["gender", "custstate", "custage_bin"])

    traindata, testdata = train_test_split(traindata, test_size = 0.3)
    traindatax = traindata[[i for i in traindata if churncolumn not in i]]
    testdatax = testdata[[i for i in traindata if churncolumn not in i]]
    traindatay = traindata[churncolumn]
    testdatay = testdata[churncolumn]

    clf = RFC(n_jobs = -1)
    clf.fit(traindatax, traindatay)
    prediction = clf.predict(testdatax)
    probability = clf.predict_proba(testdatax)
    testdata["predicted_churner"] = prediction
    testdata["probability_model"] = probability[:,1]
    return testdata



def run_data_script(companyid, read_location = 'bq', write_location = 'gs', schema ="Automate_IIQ_Dev", columnname = 'cmpnyid', table = "cc_churn_company_customer_churncodes_result", chunksize = 5000,gcs_location = 'churn_new'):
    data = ERD(location = read_location, querylocation = None, table = table, columnname = columnname, schema = schema, condition = companyid)
    data = preprocess_data(data)
    final_data = analysis_data(data)
    final_data["cmpnyid"] = companyid
    columns = ["cmpnyid"] + [i for i in final_data if "cmpnyid" not in i]
    final_data = final_data[columns]
    data["cmpnyid"] = companyid
    data = data[["cmpnyid"] + [i for i in data if "cmpnyid" not in i]]
    EWD(data = data, location = write_location, fileid=companyid,analysis = "churn_profile", gcs_location=gcs_location)
    EWD(data = final_data, location = write_location, fileid=companyid,analysis = "churn_models", gcs_location=gcs_location)
    return print("done task for {}" .format(companyid))

if __name__ == "__main__":
    run_data_script(154, schema = "Automate_IIQ_Dev")
    #run_data_script(154, location = 'pg', schema = "credit_card_feature")
