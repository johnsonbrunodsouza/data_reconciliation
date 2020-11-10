import re
import os
from flask import Flask, render_template, request
import pandas as pd
import numpy as np
import itertools
import matplotlib.pyplot as plt
import csv
from IPython.core.interactiveshell import InteractiveShell
from sas7bdat import SAS7BDAT

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False


def is_subkey(newkey,keys):

    for key in keys:
        if set(key).issubset(newkey):
            return True
    return False

def primarykey_recognition(file,max_num=4):

    doc = file
    num = 1
    sm=9999999999
    result = []
    table_length = len(doc.values)
    while num <= max_num:
        keys = list(itertools.combinations(doc.columns,num))
        for key in keys:
            if is_subkey(key,result):
                continue
            bools = np.array(doc.duplicated(subset=list(key)))
            if np.sum(bools) < sm:
                sm=np.sum(bools)
                result=list(key)
        num += 1
    return result
def unique(list1):
        unique_list = []
        for x in list1:
            if x not in unique_list:
                unique_list.append(x)
        return unique_list


def count_delimiters(uniq_delimiters, line):
        val = []
        for i in uniq_delimiters:
            val.append(line.count(i))
        return val


def delimiters(f):
        patterns = [r"\W+"]
        possible_delimiters = []
        for p in patterns:
            possible_delimiters = re.findall(p, f)
        possible_unique_delimiters = unique(possible_delimiters)
        possible_unique_delimiters.remove("\n")
        count = count_delimiters(possible_unique_delimiters, f)
        return possible_unique_delimiters, count


def get_delimiter(filename):
            declared_delimiter = []
            declared_delimiter_cnt = []
            with open(filename) as f:
                for line in f:
                    if len(declared_delimiter) == 1:
                        break
                    else:
                        delm, cnt = delimiters(line)
                        if len(declared_delimiter) != 0:
                            i = 0
                            while ((i + 1) <= len(declared_delimiter)):
                                flag = 0
                                for j in range(0, len(delm)):
                                    if declared_delimiter[i] == delm[j] and declared_delimiter_cnt[i] == cnt[j]:
                                        flag = 1
                                        i = i + 1
                                if flag == 0:
                                    del declared_delimiter[i]
                                    del declared_delimiter_cnt[i]

                        else:
                            declared_delimiter = delm
                            declared_delimiter_cnt = cnt


            return declared_delimiter

def decode_fixed_length(l_vec,filename):
    DELIMITER = l_vec
    ind_v = []
    col_v = []
    data_v = []
    v12 = 0
    v13 = 0
    y = 0

    line1 = []

    f22 = open(filename, "r")
    c2 = len(list(f22))
    f22.seek(0, os.SEEK_SET)

    if (c2 == 1):
        for i in f22:
            u = len(i)

        if len(list(DELIMITER)) == 0:
            z = 10
            y = 10
            DELIMITER.append(10)
        else:
            for i in (DELIMITER):
                y = y + i
            z = y
        f22.seek(0, os.SEEK_SET)
        line = f22.readline()
        i = 0
        while i < u:
            line1.append(line[i:z])
            i = i + y
            z = z + y
    else:
        if len(list(DELIMITER)) == 0:
            line = f22.readline()
            u = 0
            for i in line:
                u = u + 1
            u = u - 1

            DELIMITER.append(u)
        f22.seek(0, os.SEEK_SET)
        line1 = f22

    idx = np.cumsum([0] + list(DELIMITER))
    slices = [slice(i, j) for (i, j) in zip(idx[:-1], idx[1:])]

    for i in (DELIMITER):
        v12 = v12 + 1
        col_v.append('col' + str(v12))

    for k in line1:
        data = []
        v13 = v13 + 1
        for s in slices:
            data.append(k[s])
        data_v.append(data)
        ind_v.append(v13)
    return  c2, data_v,col_v,ind_v,

@app.route('/')
def index():
    return render_template('gg.html')


@app.route('/', methods=['POST'])
def getvalue():
        payload=request.get_json()
        Operation = payload['Operation']
        sourcefile = payload['SourceFile']
        baseline = payload['BaselineData']
        key = payload['ComparisonKey']
        Ikey = payload['IgnoreKey']

        count1 = ""
        count2 = ""
        count3 = ""
        count4 = ""
        count5 = ""
        comment = ""
        v_b=[]
        counter=0
        v_cll=[]
        b = []
        bb=[]
        aa=[]
        row1=[]
        update_count=0
        update_count=1
        header1 = ""
        header2 = ""

        sourcefile1 = sourcefile

        if Operation == 'Comparison':
            if sourcefile.split(".")[1]=="xlsx" or sourcefile.split(".")[1]=="xls" :
                fletype1 =  'xlsx'

            elif sourcefile.split(".")[1] == "sas7bdat":
                fletype1 = 'xlsx'
                with SAS7BDAT(sourcefile) as file:
                    file.to_data_frame().to_excel('result_'+ sourcefile.split(".")[0] + ".xlsx", index=False)
                    sourcefile = 'result_' + sourcefile1.split(".")[0] + ".xlsx"

            elif sourcefile.split(".")[1] == "dat" or sourcefile.split(".")[1]=="txt" :
                fletype1 = 'text'
            elif sourcefile.split(".")[1] == "csv" :
                fletype1 = 'csv'
            else :
                fletype1 = 'na'

            if baseline.split(".")[1] == "xlsx" or baseline.split(".")[1] == "xls":
                fletype2 = 'xlsx'

            elif baseline.split(".")[1] == "sas7bdat":
                fletype2 = 'xlsx'
                with SAS7BDAT(baseline) as file:
                    file.to_data_frame().to_excel('converted_'+ baseline.split(".")[0] + ".xlsx", index=False)
                    baseline = 'result_' + baseline.split(".")[0] + ".xlsx"

            elif baseline.split(".")[1] == "dat" or baseline.split(".")[1] == "txt":
                fletype2 = 'text'
            elif baseline.split(".")[1] == "csv":
                fletype2 = 'csv'
            else:
                fletype2 = 'na'

        if Operation == 'Comparison' :
           cc = key.split(",")
           if (fletype1 == 'xlsx' or fletype1 == 'csv' or fletype1 == 'text') and (fletype2 == 'xlsx' or fletype2 == 'csv' or fletype2 == 'text' ):
              if fletype1  == 'text':
                 delm = get_delimiter(sourcefile)
                 delm1 = delm[0]

              if fletype2 == 'text' :
                  delm = get_delimiter(baseline)
                  delm2 = delm[0]


              if fletype1 == 'xlsx':
                 f1 = pd.read_excel(sourcefile)

              elif fletype1 == 'text':
                 f1 = pd.read_csv(sourcefile, delimiter=delm1)

              elif fletype1 == 'csv':
                 f1 = pd.read_csv(sourcefile)

              if fletype2 == 'xlsx':
                 f2 = pd.read_excel(baseline)


              elif fletype2 == 'text':
                  f2 = pd.read_csv(baseline, delimiter=delm2)

              elif fletype2 == 'csv':
                  f2 = pd.read_csv(baseline)


              if header1 == 'No' :
                 f1h = list(f1)
                 for i in (list(f1)):
                    counter = counter + 1
                    v_b.append('col' + str(counter))
                    f1.columns = v_b
                    f1.loc[len(f1)] = f1h

              counter=0
              v_b =[]

              if header2 == 'No':
                 f2h = list(f2)
                 for i in (list(f2)):
                    counter = counter + 1
                    v_b.append('col' + str(counter))
                    f2.columns = v_b
                    f2.loc[len(f1)] = f2h

              if key == "":
                    aa = primarykey_recognition(f1)

              else:
                 regex = re.compile('[0-9,]+$')
                 if (regex.match(key)):
                    for i in cc:
                       aa.append(f1.keys()[int(i) - 1])
                 else:
                    aa = cc

              if Ikey != "":
                 regex = re.compile('[0-9,]+$')

                 if (regex.match(Ikey)):
                    for i in Ikey.split(","):
                       bb.append(f1.keys()[int(i) - 1])
                 else:
                    bb = Ikey.split(",")
              else:
                 bb = []
              c1 = len(f1)
              c2 = len(f2)
              if c1 != 0 :
                  df = pd.DataFrame(columns=['field', 'count', 'duplicate', 'blank', 'junk', 'top', 'least'])
                  for fld in list(f1.columns):
                      mlbcount = 0
                      nullcount2 = 0
                      nullcount1 = 0
                      nullcount = 0
                      for v in f1[fld]:
                          if str(v).strip() == "":
                              nullcount1 = nullcount1 + 1
                      nullcount2 = f1[fld].isnull().sum()
                      nullcount = nullcount1 + nullcount2
                      count = len(f1)
                      for i in list(f1[fld].dropna()):
                          if not str(i).isascii():
                              mlbcount = mlbcount + 1

                      mstcmn = f1[fld].value_counts().nlargest(3)
                      least = f1[fld].value_counts().nsmallest(3)
                      if nullcount > 1:
                          duplicate = ((len(f1[fld]) - f1[fld].nunique()) - nullcount2)
                      else:
                          duplicate = len(f1[fld]) - f1[fld].nunique()
                      df = df.append({'field': fld, 'count': count, 'duplicate': duplicate, 'blank': nullcount, 'junk': mlbcount,
                                      'top': mstcmn, 'least': least}, ignore_index=True)

                      df.to_csv(sourcefile1.split('.')[0]+'_dataquality.csv', index=False)

              plt.style.use('dark_background')
              fig, ax = plt.subplots(figsize=(21, 6.5))
              width = 0.25
              ind = np.arange(len(df['field']))
              ax.bar(ind, df['blank'], width, color="green", label='blank')
              ax.bar(ind + width, df['duplicate'], width, color="red", label='duplicate')
              ax.bar(ind + width + width, df['junk'], width, color="blue", label='junk')
              ax.plot(ind, df['blank'], color="green", ls="dotted")
              ax.plot(ind + width, df['duplicate'], color="red", ls="dotted")
              ax.plot(ind + width + width, df['junk'], color="blue", ls="dotted")
              ax.plot(df['field'], df['count'], color="yellow", ls="dotted", lw=2,
                      label='Total Volume')
              plt.xticks(ind + width / 2 + width / 2, df['field'])
              ax.set_xlabel("Columns")
              ax.set_ylabel("Count")
              ax.set_title("Data Trends")
              ax.legend(loc=1)
              plt.setp(plt.gca().get_xticklabels(), rotation=20, horizontalalignment='right')
              plt.savefig("static/image/data_profile_sas.png")


              if c1 != 0 and c2 != 0:
                 for i in list(bb):
                    b.append(f1.columns.get_loc(i))

                 f8 = f1.drop_duplicates(aa, keep='last')
                 f9 = f2.drop_duplicates(aa, keep='last')
                 f11 = f1.drop_duplicates(aa, keep=False)

                 index1 = pd.MultiIndex.from_arrays([f8[col] for col in aa])
                 index2 = pd.MultiIndex.from_arrays([f9[col] for col in aa])
                 index3 = pd.MultiIndex.from_arrays([f11[col] for col in aa])

                 f3 = f8.loc[~index1.isin(index2)]
                 f4 = f9.loc[~index2.isin(index1)]

                 f5 = f8.loc[index1.isin(index2)]
                 f6 = f9.loc[index2.isin(index1)]

                 f10 = f8.loc[~index1.isin(index3)]
                 f10['flag'] = 'DUP'

                 f5 = f5.sort_values(aa)
                 f6 = f6.sort_values(aa)

                 #f3['flag'] = 'I'
                 #f4['flag'] = 'D'
                 #f5['flag'] = 'U'
                 #f6['flag'] = 'U'
                 f3.insert(0, 'flag', 'I')
                 f4.insert(0, 'flag', 'D')
                 f5.insert(0, 'flag', 'U')
                 f6.insert(0, 'flag', 'U')


                 df_row_reindex = pd.concat([f10, f3, f4 ], ignore_index=True)

                 f5.equals(f6)
                 comparison_values = f5.values == f6.values

                 rows, cols = np.where(comparison_values == False)
                 ind1 = 0

                 for item in zip(rows, cols):
                    if item[1] not in b and ((f6.iloc[item[0], item[1]])== (f6.iloc[item[0], item[1]]) or (f5.iloc[item[0], item[1]])== (f5.iloc[item[0], item[1]])) :
                       print('{} --> {}'.format(f6.iloc[item[0], item[1]], f5.iloc[item[0], item[1]]))
                       f5.iloc[item[0], item[1]] = '{} --> {}'.format(f6.iloc[item[0], item[1]], f5.iloc[item[0], item[1]])


                 for i in zip(rows, cols):
                    if i[0] not in row1 and i[1] not in b and ((f6.iloc[i[0], i[1]])== (f6.iloc[i[0], i[1]]) or (f5.iloc[i[0], i[1]])== (f5.iloc[i[0], i[1]])):
                       row1.append(i[0])
                       df_row_reindex = pd.concat([df_row_reindex, f5.iloc[i[0]:i[0] + 1]], ignore_index=True)
                       ind1 = ind1 + 1

                 count1 = c1
                 count2 = len(f10)
                 count3 = len(f3)
                 count4 = ind1
                 count5 = len(f4)
                 if count2 != 0 or count3 != 0 or count4 != 0 or count5 != 0:
                       df_row_reindex.to_csv("recon_" + sourcefile1.split('.')[0] + '.csv', index=False)



                 if fletype1 == 'text':
                    comment = "Field delimiter for source data is : '" + delm1 + "'" + '. '

                 if fletype2 == 'text':
                    comment = "Field delimiter for baseline data is : '" + delm2 + "'" + '.'

                 if header1 == 'No':
                       comment = comment +  "Header is added for source data : " + str(v_b) + '. '

                 if header2 == 'No':
                       comment = comment + "Header is added for baseline data : " + str(v_b) + '. '

                 if key == "":
                       comment = comment + "Recon Keys are : " + str(aa) + '. '

                 if count2 != 0 or count3 != 0 or count4 != 0 or count5 != 0:
                       comment = comment + "Recon results are stored in file : recon_" + sourcefile.split('.')[0] + '.csv' + '.  '

                 if count3 == 0 and count4 == 0 and count5 == 0:
                    status = "Files are in Sync"
                 else:
                    status = "Files are not in Sync"
              else:
                 if c1 == 0:
                    comment = "Can't perform recon." + sourcefile + " is empty"
                    status = ""

                 else:
                    comment = "Can't perform recon." + baseline + " is empty"
                    status = ""

           else:

              comment = 'Error : File type is not supported'
              status = ""

           return ({"File count": count1, "Duplicate": count2, "Insert": count3, "Update": count4, "Delete": count5,"Recon Status": status, "Summary": comment})


if __name__ == '__main__':
    app.run(debug=True)
