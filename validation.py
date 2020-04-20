import logging
import argparse
import fileinput
from decimal import Decimal
from get_cfg import get_cfg
from filepolling import file_polling
import shutil
import watchtower
import boto3
import zipfile
import os
from nch_arch_db_io import arch_db_updt

def write_to_s3(nch_file,file_nm_only,bucket_name):
# Create an S3 client
    S3 = boto3.client('s3',use_ssl=False, verify=False)
#Change the source file name here
    SOURCE_FILENAME = nch_file
    #BUCKET_NAME = 'nch-arch'
    BUCKET_NAME = bucket_name

    if file_nm_only[:1] != "P":
        file_nm_only = "P" + file_nm_only[1:]

# Uploads the given file using a managed uploader, which will split up large
# files automatically and upload parts in parallel.
    try:
        S3.upload_file(SOURCE_FILENAME, BUCKET_NAME, file_nm_only)
        logger.info("File Successfully copied to S3 - {0}".format(file_nm_only))
        return True
    except Exception as e:
        logger.error("File upload to S3 failed - {0}".format(file_nm_only))
        logger.error(str(e))
        return False

   msg1=' has failed Validation'
   msg2=' has failed Upload to S3'
   def send_notif(filename,msg):
       sns = boto3.client('sns')
            # Publish a simple message to the specified SNS topic
       response = sns.publish(
            TopicArn='arn:aws:sns:us-west-2:832658686751:CD_status',
                Message='File '+filename+msg,
                                            )
    
    
def escape(c):
    c = ord(c)
    if c <= 0xff:
        return r'\x{0:02x}'.format(c)
    elif c <= '\uffff':
        return r'\u{0:04x}'.format(c)
    else:
        return r'\U{0:08x}'.format(c)
    
# Decompressing the zipped file landed in data folder
def decompress_file(path_name,file_path):
    with zipfile.ZipFile(path_name, 'r') as zip:
        # printing all the contents of the zip file
        # zip.printdir()

        # extracting all the files
        print('Extracting all the files now...')
        zip.extractall(file_path)
        zi=zip.infolist()
        file_name=""
        file_size=""
        for elem in zi:
            file_name=elem.filename
            file_size=elem.file_size

        #zip.filename
        #print('Done!')
        #return (zip.namelist()[0])
        return file_name, file_size
def get_file_name(path_name):
    parts=path_name.split("/")
    file_name_only = (parts[(len(parts)-1)])
    arch_file_name_only=file_name_only[:-1] +"A"
    nch_file_name_only = file_name_only[:-2] +""
    return nch_file_name_only, arch_file_name_only


#defining the Validation logic here
def valid(clm_type_aggr,trail_rec):
    calc_clm_type_aggrs={}
    aggrs=trail_rec.split(";")
    i=0
    for aggr in aggrs:
        if len(aggr)== 2:
            i_amt= Decimal(aggrs[i+1])
            i_count=int(aggrs[i+2])
            calc_clm_type_aggrs[aggr]=(i_amt,i_count)
        i=i+1
    logger.info("Calc values by claim type: {0}".format(calc_clm_type_aggrs))
    logger.info(" Check-sum values on file: {0}".format(clm_type_aggr))
    if str(clm_type_aggr)==str(calc_clm_type_aggrs):
        file_nm = nch_file_name_only
        act_type = "Validation Completed"
        act_status = "Successful"
        arch_db_updt(act_type, file_nm, act_status, calc_aggrs=calc_clm_type_aggrs, file_aggrs=clm_type_aggr)
        return True
    else:
        file_nm = nch_file_name_only
        act_type = "Validation Completed"
        act_status = "Unsuccessful"
        arch_db_updt(act_type, file_nm, act_status, calc_aggrs=calc_clm_type_aggrs, file_aggrs=clm_type_aggr)
        send_notif(file_nm,msg1)
        return False
    #print("file_aggrs: {0}".format(str(calc_clm_type_aggrs)))
    #print("calc_aggrs: {0}".format(str(clm_type_aggr)))

def aggr_data(clm_type_aggr,clm_type_cd,clm_pd_amt):
   if clm_type_cd in clm_type_aggr:
       lst=list(clm_type_aggr[clm_type_cd])
       t_clm_amt = lst[0]
       clm_count = lst[1]
       clm_count = clm_count + 1
       t_clm_amt = t_clm_amt + clm_pd_amt
       upd_aggr=(t_clm_amt,clm_count)
       clm_type_aggr[clm_type_cd]=upd_aggr
   else:
       #print("adding claim type {0}".format(clm_type_cd))
       clm_type_aggr[clm_type_cd]=(clm_pd_amt,1)
   return clm_type_aggr
if __name__ == "__main__":
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(10)
    logger.addHandler(watchtower.CloudWatchLogHandler('NCH_Archive_logs'))
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_name",type=str,help="name of the NCH file to be validated",required=True)
    parser.add_argument("--cfg_file_path",type=str,help="name and path of the ini file",required=True)
    args=parser.parse_args()
    cfg = get_cfg(args.cfg_file_path)
    smry_file_name = args.file_name
    nch_file_name_only, arch_file_name_only =get_file_name(smry_file_name)
    arch_file_name = cfg.data_file_path + arch_file_name_only

    file_nm = nch_file_name_only
    act_type = "Transferred Started"
    act_status = "Successful"
    arch_db_updt(act_type, file_nm, act_status)

    logger.info("Begin NCH Validation for file {0}".format(arch_file_name_only))
    file_polling(smry_file_name)

    file_nm = nch_file_name_only
    act_type = "Transferred Completed"
    act_status = "Successful"
    arch_db_updt(act_type, file_nm, act_status)

    uzip_file_name=""
    file_nm = nch_file_name_only
    act_type = "Decompression Started"
    act_status = "In progress"
    arch_db_updt(act_type, file_nm, act_status)

    logger.info("unzipping file {0}".format(arch_file_name_only))
    uzip_file_name, file_size=decompress_file(arch_file_name,cfg.stg_file_path)
    file_nm = nch_file_name_only
    act_type = "Decompression Completed"
    act_status = "Successful"
    arch_db_updt(act_type, file_nm, act_status,file_size=file_size)

    input_file = cfg.stg_file_path + uzip_file_name

    clm_type_aggr={}

    #out_file=open(cfg.stg_file_path+file_nm_only,"w+")
    #in_file=open(input_file, "rb", 32768)
    file_nm = nch_file_name_only
    act_type = "Validation Started"
    act_status = "In progress"
    arch_db_updt(act_type, file_nm, act_status)
    rc=0
    i = 0
    try:
       for line in fileinput.input(input_file,openhook=fileinput.hook_encoded('cp437')):
       #for line in fileinput.input(input_file,False,'',0,'r',fileinput.hook_encoded("iso-8859-1")):
       #for line in fileinput.input(input_file):
        #for ln in in_file:
            i = i + 1
            #line=str(ln)
            prev_rec = str(line)
            if (line[7:8]) == "S" :
                print("Trailer record")
                #out_file.write(line)
            elif (line[7:8])  == "D":
                print("Header record")
                #out_file.write(line)
            elif((line[9:11]) == "72" or (line[9:11]) == "71" or (line[9:11]) == "81" or (line[9:11])=="82") and line[:1] =="+":
                if int(line[0:6]) != len(line) - 1:
                    raise Exception ("Invalid record length, Rec lenght value on file is: {0} - Actual length of rec is: {1} for file: {2}".format(int(line[0:6]), len(line),nch_file_name_only))
                paid_amt = line[241:254]
               #print("is 70 or 80")
               #print("clm_type_cd {0}".format(line[9:11]))
               #print(paid_amt)
                clm_type_aggr = aggr_data(clm_type_aggr, line[9:11], Decimal(paid_amt))
                #out_file.write(line)
            elif (line[9:11]) >= "00" and (line[9:11]) <="99" and line[:1] =="+":
               #print('not 70 or 80')
                if int(line[0:6]) != len(line) - 1:
                    raise Exception("Invalid record length, Rec length value on file is: {0} - Actual length of rec is: {1} for file: {2}".format(int(line[0:6]), len(line),nch_file_name_only))
                paid_amt = line[244:257]
                #print(paid_amt)
                #print("clm_type_cd {0}".format(line[9:11]))
                clm_type_aggr= aggr_data(clm_type_aggr,line[9:11],Decimal(paid_amt))
                #out_file.write(line)
            else:
                print("none of the above")
                # #print(line)
                if len(line) == 1:
                    if (escape(str(line))) == r"\x1a":
                        pass
                    else:
                        raise Exception ("invalid record type found at record number: {0} - record: {1} ".format(i,line[0:50]))
                else:
                    raise Exception("invalid record type found at record number: {0} - record: {1} ".format(i, line[0:50]))

    except Exception as e:
        logger.error ("error occured at record count: {0} for file: {1}".format(i,nch_file_name_only))
        logger.error((str(e)))
        quit(1)

    #out_file.close()
    fileinput.close()
    try:
        if os.path.isfile(smry_file_name):
            smry_rec= fileinput.input(smry_file_name).readline()
            if smry_rec!="":
                if valid(clm_type_aggr, smry_rec):
                    logger.info("validation successful for file {0}".format(nch_file_name_only))
                    if cfg.write_to_s3_flag == "True":
                        file_nm = nch_file_name_only
                        act_type = "S3 Upload Started"
                        act_status = "In progress"
                        arch_db_updt(act_type, file_nm, act_status)
                        if write_to_s3(input_file, nch_file_name_only, cfg.s3_bucket_name) == True:
                            os.remove(input_file)
                            os.remove(arch_file_name)
                            file_nm = nch_file_name_only
                            act_type = "S3 Upload Completed"
                            act_status = "Successful"
                            arch_db_updt(act_type, file_nm, act_status)
                          
            #Reuploading the file if S3 upload fails
                            
                        elseif write_to_s3(input_file, nch_file_name_only, cfg.s3_bucket_name) == True:
                            os.remove(input_file)
                            os.remove(arch_file_name)
                            file_nm = nch_file_name_only
                            act_type = "S3 upload Completed"
                            act_status = "Successful"
                            arch_db_updt(act_type, file_nm, act_status)
                           

                        else:
                            file_nm = nch_file_name_only
                            act_type = "S3 Upload Failed"
                            act_status = "Unsuccessful"
                            arch_db_updt(act_type, file_nm, act_status)
                            send_notif(file_nm,msg2)
                    else:
                        shutil.move(input_file, cfg.pass_valdtn_file_path + nch_file_name_only)
                        logger.info("Moved file {0} to {1}".format(nch_file_name_only,cfg.pass_valdtn_file_path))
                        os.remove(arch_file_name)
                        logger.info("Deleted arch_file_name")

                else:
                    shutil.move(arch_file_name, cfg.fail_valdtn_file_path + arch_file_name_only)
                    shutil.move(input_file, cfg.fail_valdtn_file_path + nch_file_name_only)
                    raise Exception ("Validation failed for file: {0}".format(nch_file_name_only))
            else:
                raise Exception ("NO summary record for file {0}".format(smry_file_name))
        else:
            raise Exception ("Summry file does not exist for file {0}".format(smry_file_name))

    except Exception as e:
        print(e)
        rc=1

    quit(rc)


