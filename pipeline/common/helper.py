import os
import datetime
from datetime import timedelta
from pyspark.sql.functions import when, count, col
class Helper:

    def remove_file(self,file_directory_path,file_name):
        if os.path.isfile(file_directory_path + file_name):
            print("[{}]:[INFO] : deleting {} ...".format(datetime.datetime.utcnow(),file_directory_path + file_name))
            os.remove(file_directory_path + file_name) 
            print("[{}]:[INFO] : delete finished {} ".format(datetime.datetime.utcnow(),file_directory_path + file_name))   
        return None
    
    def get_concate_filename_date_pattern(self,file_directory_path, previous_days, date_format):
        today = datetime.datetime.now()
        day = timedelta(days = previous_days)
        temp_date = (today - day).strftime(date_format)
        pattern = file_directory_path + temp_date
        return pattern

    def check_file_count(self,blob_list,filelist):
        self.testdata=""
        expected_file_count= len(filelist)
        latest_file_count = len(blob_list)
        new_files=[]
        missing_files=[]

        for blob_name in blob_list :       
            #getting file name from direcotry example : MVP2_prod_05272021/HUM_RETAIL_INSIGHT_MBR_FLU_VACN_20210725.dat
            b_file_name = blob_name.split('/')[-1]  
            isNew=True         
            for f_name in filelist:
                if (b_file_name.find(f_name) != -1):
                    filelist[f_name]=1
                    isNew=False
                    break
            if isNew is True:
                new_files.append(b_file_name)
               
        for file, value in filelist.items():
            if value==0:
                missing_files.append(file)

        self.testdata = (self.testdata + ':curly_loop: *FILE COUNT SUMMARY :*\n')  
        if expected_file_count!=latest_file_count or len(missing_files)>0:
             self.testdata = (self.testdata + '*Status :* Count Not Match :x: \n')
        else :
             self.testdata = (self.testdata + '*Status :* Count Match :white_check_mark: \n')

        self.testdata = (self.testdata + '*Expected File Count :* '+ str(expected_file_count) + '\n')
        self.testdata = (self.testdata + '*Latest File Count :* '+ str(latest_file_count) + '\n')
        if len(missing_files)>0 :
            self.testdata = (self.testdata + '*Missing File :* '+ str(missing_files) + '\n')
        if len(new_files)>0 :
            self.testdata = (self.testdata + '*New File :* '+ str(new_files) + '\n')
        return self.testdata


    def check_gap_count(self,gap_list,measure_blob):
        self.testdata=""
        distict_measure=measure_blob.select('MEAS_ID','SUB_MEAS_ID').distinct().collect()
        new_gaps=[]
        missing_gaps=[]
        for gap in distict_measure:
            isNew=True 
            SUB_MEAS=''
            if str(gap['SUB_MEAS_ID'])=='NULL' or str(gap['SUB_MEAS_ID'])=='None' :
                SUB_MEAS=''
            else :
                SUB_MEAS=str(gap['SUB_MEAS_ID'])
            for obj in gap_list:
                if obj['code']==gap['MEAS_ID'] and obj['subcode']==SUB_MEAS:                  
                        isNew=False
                        obj['status']=1
                        break
            if isNew is True:
                new_gaps.append(str(gap['MEAS_ID'])+' '+str(gap['SUB_MEAS_ID'])+' , ')
        self.testdata = (self.testdata + ':curly_loop: *MEASURE GAP SUMMARY :*\n')

        temp_gap_summary=" ".join(str(x) for x in distict_measure)
        temp_gap_summary = temp_gap_summary.replace("Row(MEAS_ID='", '')
        temp_gap_summary = temp_gap_summary.replace("SUB_MEAS_ID=", '')
        temp_gap_summary = temp_gap_summary.replace("',", '')
        temp_gap_summary = temp_gap_summary.replace(')', ',')
        self.testdata = (self.testdata + '*Latest Gap List :* '+str(temp_gap_summary)+'\n')

        if len(new_gaps)>0 :
            self.testdata = (self.testdata + '*New gap :* '+ str(new_gaps) + '\n')

        for obj in gap_list:
             if obj['status']==0:
                missing_gaps.append(str(obj['code'])+' '+str(obj['subcode'])+' , ')
        if len(missing_gaps)>0 :
            self.testdata = (self.testdata + '*Missing gap :* '+ str(missing_gaps) + '\n')
        
        if len(new_gaps)>0 :
             self.testdata = (self.testdata + '*Status : :x: \n')
        
        return self.testdata

            


    def get_null_count_summary(self,data_df):     
        df_null_counts=data_df.select([count(when(col(c).isNull(), c)).alias(c) for c in data_df.columns]).collect()
        return df_null_counts



  
    