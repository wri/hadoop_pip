import argparse
from utilities import annual_helpers
from boto.s3.connection import S3Connection


conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')

#iterate over tsv files, like tsv folder files, and swaths of 00N - 80N 10S-50S... for extent
parser = argparse.ArgumentParser()
parser.add_argument('--analysis-type', '-a', required=True, choices=['extent', 'loss', 'gain'])
args = parser.parse_args()
analysis_type = args.analysis_type

# download jar file
annual_helpers.download_jar()

# list all the tsv'd boundary files in tsv-boundary-int-gadm28 folder
tsv_list = [key.name.encode('utf-8') for key in bucket.list(prefix='alerts-tsv/tsv-boundaries-gadm28/') if ".tsv" in key.name.encode('utf-8')]

for tsv in tsv_list:
    tsv_name = tsv.split("/")[-1]
    csv_name = tsv_name.replace(".tsv", ".csv")

    if analysis_type == 'extent':

        ns_list = annual_helpers.gen_ns_list(tsv_name)

        for ns_tile in ns_list:

            # if there is not a csv output already in the file system
            if not annual_helpers.check_output_exists(analysis_type, csv_name, ns_tile):
                extent_points_path = 's3a://gfw2-data/alerts-tsv/extent_2000/{}*'.format(ns_tile)
                annual_helpers.write_props(analysis_type, extent_points_path, tsv_name)

                annual_helpers.call_pip()

                annual_helpers.upload_to_s3(analysis_type, tsv_name, ns_tile)

    else:

        # if there is not a csv output already in the file system
        if not annual_helpers.check_output_exists(analysis_type, csv_name):
        
            if analysis_type == 'loss':
                points_path = r's3a://gfw2-data/alerts-tsv/loss_2016/'
            else:
                points_path = r's3a://gfw2-data/alerts-tsv/gain_tiles/'
                
            annual_helpers.write_props(analysis_type, points_path, tsv_name)

            annual_helpers.call_pip()

            annual_helpers.upload_to_s3(analysis_type, tsv_name)
