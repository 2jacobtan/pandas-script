""" 
__Documentation__
Requires these files:
    masters.txt #one on each line: full path of a folder containing master list .xls(x) files
    raws.txt #one on each line: full path of a folder containing raw data .xls(x) files
 """

### to import packages
import pandas as pd
import os
from itertools import chain
from string import punctuation
from datetime import datetime

TRUNCATE_LEN = 12 # length limit for vlookup_2()

def folderList_from_txt(fileName):
    """ Look inside fileName for list of folder names ; return pyList. """
    with open(fileName, "r") as ins:
        pyList = [line.strip() for line in ins]
    return pyList

### to read data from files
def isExcel(file_path):
    """ Look at the file (file_path !must be full path) ; check if it is .xls(x) ; return type:bool """
    check = ( file_path.endswith((".xlsx",".xls")) and os.path.isfile(file_path) )
    return check
def xlsList_from_folder(dirPath):
    """ Look in dirPath for .xls files ; return a list of them. """
    xlsList = [fPath for fPath in (os.path.join(dirPath,f) for f in os.listdir(dirPath)) if isExcel(fPath)]
    return xlsList
def xlsList_from_folderTree(dirPath):
    """ Look in dirPath (!and all sub directories) for .xls files ; return a list of them. """
    xlsList = []
    for (root,_,files) in os.walk(dirPath):
        for f in files:
            fPath = os.path.join(root,f)
            if isExcel(fPath):
                xlsList.append(fPath)
    return xlsList
def df_from_xlsList(xlsGenerator):
    """ Make one DataFrame object from a list of xls file paths """
    print("Reading excel files:")
    xlsList = list(xlsGenerator)
    for i,x in enumerate(xlsList):
        print("to read ({}):".format(i),x)
    df = pd.concat((pd.read_excel(fPath) for fPath in xlsList),ignore_index=True,sort=False)
    #df.applymap(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)
        # maybe useful to escape weird characters, especially for Pakistan raw data (tried and failed)
    print("Reading excel files: done.")
    return df
### to write data to file
def safeToExcel(filePath,dataFrame):
    with pd.ExcelWriter(filePath,engine="xlsxwriter") as writer: #must use pd.ExcelWriter with engine="xlsxwriter" to overcome illegal character error
        dataFrame.to_excel(writer,index=False)
def safeToExcel_indexTrue(filePath,dataFrame):
    with pd.ExcelWriter(filePath,engine="xlsxwriter") as writer: #must use pd.ExcelWriter with engine="xlsxwriter" to overcome illegal character error
        dataFrame.to_excel(writer,index=True)
def safeToCSV(filePath,dataFrame): #currently unused
        dataFrame.to_csv(filePath,index=False)

### string cleaning
def alpha_numeric_only(cell):
    cell = str(cell)
    cell = ''.join(e for e in cell if e.isalnum())
    return cell
def del_messr_prefix(cell):
    cell = str(cell)
    if cell[0:3].upper() == "M/S":
        cell = cell[3:]
    return cell
def strNormalize_series(pd_series):
    print("pd_series.head() before normalize:\n",pd_series.head(5))
    #return pd_series.map((lambda s: str(s).translate(str.maketrans('', '', punctuation))),na_action="ignore")
        #removes punctuation only
    pd_series = pd_series.map((lambda s: alpha_numeric_only(del_messr_prefix(s))),na_action="ignore")
    pd_series = pd_series.str.upper()
        #convert to uppercase
    print("@pd_series.shape",pd_series.shape)
    print("@pd_series.head() after normalize:\n",pd_series.head(5))
    return pd_series
def rm_dupRows(dataFrame,stopAt_index):
    """ remove deplicate rows by checking first two columns """
    column_names = ["Importer Company","Standard Manufacturing CO"]
    check_columns = dataFrame[column_names[0:stopAt_index]]
    print(check_columns.shape)
    print(check_columns.head(5))
    dups_bool = check_columns.duplicated()
        #default is duplicated(keep=True)
    df_cleaned = dataFrame.loc[~dups_bool]
        # ~ inverts the dups_bool so that e.g. True becomes False, and vice versa
    return df_cleaned
def clean_masterDF(master_df):
    """ modifies in-place """
    #df_m = master_df.iloc[:,0:8] #probably obsolete
    df_m = master_df
    print("@df_m:", df_m.shape)
    print(df_m.head(4))
    #safeToExcel_indexTrue("Masterlist_beforeClean.xlsx",df_m)
    df_m["Importer Company"] = strNormalize_series(df_m["Importer Company"])
    df_m_cleaned = rm_dupRows(df_m,2) # remove duplicates
    print('@df_m:',df_m.shape)
    print('@df_m_cleaned:',df_m_cleaned.shape)
    print(df_m_cleaned.head(5))
    df_m_cleaned_duplicates = df_m_cleaned.loc[df_m_cleaned.iloc[:,0].duplicated(keep=False)].sort_values(by=['Importer Company','Standard Manufacturing CO'])
    print(df_m_cleaned_duplicates)
    """ use when necessary """
    #safeToExcel_indexTrue("Masterlist duplicates.xlsx",df_m_cleaned_duplicates)
    return df_m_cleaned
def truncate_masterDF(df_m_original,length):
    df_m = df_m_original.copy()
    df_m["Importer Company"] = df_m["Importer Company"].str.slice(0,length)
    df_m = rm_dupRows(df_m,2)
    return df_m

#manual input
IMPORTER_COLUMN_LABELS = "Importer,IMPORTER NAME,FOREIGN IMPORTER NAME".split(sep=',')
""" ** Importer column is labelled differently depending on data source country. **"""
def getImporterColumnLabel(df_r):
    importer_column_label_list = df_r.columns.intersection(IMPORTER_COLUMN_LABELS).format()
    print("importer_column_label:", importer_column_label_list)
    assert len(importer_column_label_list) == 1, 'len(importer_column_label) should be 1. It was {}'.format(importer_column_label_list)
    return importer_column_label_list[0]

### vlookup passes
def vlookup(raw_df, df_m_cleaned):
    """ vlookup pass 1 """
    df1 = raw_df
    importer_column_label = getImporterColumnLabel(df1)
    print('@df1.shape:',df1.shape)
    df1["vlookup"] = strNormalize_series(df1[importer_column_label])
    print('@df1 with vlookup column:',df1.shape)
    print("@df_master_cleaned.loc[:,'Importer Company'].duplicated().value_counts()")
    print(df_m_cleaned.loc[:,'Importer Company'].duplicated().value_counts())
    df = pd.merge(df1,df_m_cleaned,left_on="vlookup",right_on="Importer Company",how="left",suffixes=('_x',''))
    print('@df.shape:',df.shape)
    return df
def vlookup_2(df_unmatched,df_m_cleaned_truncated):
    """ Use master list that has gone through clean_masterDF() and truncate_masterDF().
        Use subset of rows of raw data unmatched after the first vlookup() pass.
    """
    original_len = len(df_unmatched.columns)
    print("@df_unmatched['vlookup'] before slice():")
    print(df_unmatched['vlookup'].head(4))
    # truncate 'vlookup' column to match truncated master list
    df_unmatched['vlookup'] = df_unmatched['vlookup'].str.slice(0,TRUNCATE_LEN)
    print("@df_unmatched['vlookup'] after slice():", df_unmatched.shape)
    print(df_unmatched['vlookup'].head(4))
    #safeToExcel_indexTrue("debug/"+get_datetime()+" df_unmatched.xlsx",df_unmatched)
    # drop() columns to prevent overlap between left and right DataFrames when merge()
    df_unmatched.drop(df_m_cleaned_truncated.columns, axis=1, inplace=True) # drop() in-place
    print("@df_unmatched in vlookup_2() before merge():")
    print(df_unmatched.head(4))
    df = pd.merge(df_unmatched,df_m_cleaned_truncated,left_on="vlookup",right_on="Importer Company",how="left",suffixes=('_x',''))
    print("@df in vlookup_2() after merge:", df.shape)
    print(df.head(4))
    assert (len(df.columns) == original_len)
    return df

def get_datetime():
    return datetime.now().isoformat(timespec='minutes').translate(str.maketrans("T:","_,"))

def main():
#manual input
    MASTER_FOLDERS = folderList_from_txt("masters.txt")
    RAW_FOLDERS = folderList_from_txt("raws.txt")

    ### to load master lists
    print("Loading master list files …")
    master_files = chain.from_iterable((xlsList_from_folder(folder) for folder in MASTER_FOLDERS))
        #accumulated list of .xls files (full path) in all the folders listed in MASTER_FOLDERS (!without traversing subfolders)
    df_master = df_from_xlsList(master_files)
        #read each masterlist file as a DataFrame and concatenate into one DataFrame
    df_master_cleaned = clean_masterDF(df_master) #to use with vlookup()
    #print("@df_master_cleaned.loc[:,'Importer Company'].duplicated().value_counts()")
    #print(df_master_cleaned.loc[:,'Importer Company'].duplicated().value_counts())
    df_master_cleaned_truncated = truncate_masterDF(df_master_cleaned,TRUNCATE_LEN)
    print("*** Begin __Output merged countries__ ***")
# manual input
    INDICES = range(0,4) # output only a subset of the countries
    """ *** check indices *** """
# manual input
    COUNTRIES="America,Britain,Canada,Denmark".split(sep=",")
        # manually type in a comma-separated list of the country names
    timeNow = get_datetime()
    directory = "Output/"+timeNow+"/"
    subfolder_1 = "precise match/"
    subfolder_2 = "precise match + first 12 letters match/"
    os.makedirs(directory+subfolder_1)
    os.makedirs(directory+subfolder_2)
    for i,countryFolder in enumerate(RAW_FOLDERS):
        if i in INDICES:
            print("Country:", COUNTRIES[i])
            country_fileList = xlsList_from_folderTree(countryFolder)
            country_df = df_from_xlsList(country_fileList)
            
            # first pass
            current_df = vlookup(country_df,df_master_cleaned) # first pass
            print("@current_df after vlookup():",current_df.shape)
            print(current_df[['Importer Company','Standard Manufacturing CO']].head(8))
            filePath_vlooked = directory+subfolder_1+COUNTRIES[i]+' '+timeNow+" vlooked"+".xlsx"
            safeToExcel(filePath_vlooked,current_df)
            print(filePath_vlooked, "completed vlookup output.")

            # second pass
            # pick out unmatched rows for processing
            lengthA0 = len(current_df.index); print("@lengthA0:",lengthA0) # before dropna()
            df_2 = current_df.loc[current_df['Importer Company'].isna()]
            lengthB = len(df_2.index); print("@lengthB:",lengthB)
            current_df.dropna(subset=["Importer Company"],inplace=True)
            lengthA1 = len(current_df.index); print("@lengthA1:",lengthA1)# after dropna()
            assert (lengthA1 + lengthB == lengthA0)
            output_vlookup_2 = vlookup_2(df_2,df_master_cleaned_truncated)
                # second pass: extract subset of the remaining unmatched rows in current_df, then match with truncated master list, and return result
                # to merge the output back to current_df
            print("@output_vlookup2:", output_vlookup_2.shape)
            print(output_vlookup_2.head(4))
            # ********** not tested: DataFrame.update() *****************
            current_df = current_df.append(output_vlookup_2, ignore_index=True) # add output of vlookup_2 back to main DataFrame
            print("@current_df after vlookup2():",current_df.shape)
            #print(current_df[['Importer Company','Standard Manufacturing CO']].head(8))
            print(current_df.head(8))
            filePath_vlooked_2 = directory+subfolder_2+COUNTRIES[i]+' '+timeNow+" vlooked_2"+".xlsx"
            safeToExcel(filePath_vlooked_2,current_df)
            print(filePath_vlooked, "completed vlookup_2 output.")

    print("*"*3,"End of __Output merged countries__","*"*3)

if __name__== "__main__":
    main()

### end of module
print("*** yha_loadData.py fully loaded ***")
