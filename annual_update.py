import argparse
from utilities import annual_helpers


#iterate over tsv files, like tsv folder files, and swaths of 00N - 80N 10S-50S... for extent
parser = argparse.ArgumentParser()
parser.add_argument('--analysis-type', '-a', required=True, choices=['extent', 'loss'])
args = parser.parse_args()
analysis_type = args.analysis_type

# download jar file
utilities.download_jar()

# list all the tsv'd boundary files in tsv-boundary-int-gadm28 folder
tsv_list = [key for key in bucket.list(prefix='alerts-tsv/tsv-boundaries-gadm28/') if ".tsv" in key.name.encode('utf-8')]

for tsv in tsv_list:
    tsv_name = tsv.split("/")[-1]
    csv_name = tsv_name.replace(".tsv", ".csv") 
    
    # if there is not a csv output already in the file system
    if not utilities.check_output_exists(analysis_type, csv_name):
        
        if analysis_type == 'extent':

            ns_list = utilities.gen_ns_list()
            
            for ns_tile in ns_list:
                extent_points_path = 's3a://gfw2-data/alerts-tsv/extent_2000/{}*'.format(ns_tile)
                utilities.write_props(analysis_type, extent_points_path, tsv_name)
                
                utilities.call_pip()
        
                utilities.upload_to_s3(analysis_type, tsv_name)

        else:
            loss_points_path = 's3a://gfw2-data/alerts-tsv/loss_2016/'
            utilities.write_props(analysis_type, loss_points_path, tsv_name)
    
            utilities.call_pip()
            
            utilities.upload_to_s3(analysis_type, tsv_name)