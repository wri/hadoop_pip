import shutil
import os
import subprocess
from boto.s3.connection import S3Connection

import calc_tsv_extent


conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')


def download_jar():

    # check first to see if the target folder is already there:
    if not os.path.exists('target'):

        jar_file = 's3://gfw2-data/alerts-tsv/batch-processing/target_0.3.3.zip'

        cmd = ['aws', 's3', 'cp', jar_file, '.']
        subprocess.check_call(cmd)

        jar_name = os.path.basename(jar_file)
        cmd = ['unzip', jar_name]
        subprocess.check_call(cmd)


def write_props(analysis_type, points_path, poly_name):
    four_poly_fields = ['bra_biomes_int_gadm28.tsv', 'fao_ecozones_bor_tem_tro_sub_int_diss_gadm28_large.tsv', 'wdpa_final_int_diss_wdpaid_gadm28_large.tsv']
    
    # for our purposes, extent is the same as gain
    # four input fields (x, y, value and area)
    # and this is easier than editing the scala code to include a gain type
    if analysis_type == 'gain':
        analysis_type = 'extent'

    poly_fields = '1,2,3'

    if poly_name in four_poly_fields:
        poly_fields += ',4'

    points_dict = {'loss':'0,1,2,3,4,5', 'extent':'0,1,2,3'}
    points_fields = points_dict[analysis_type]

    application_props = """
spark.app.name=YARN Points in World
output.path=hdfs:///user/hadoop/output
output.sep=,
points.y=1
points.path={0}
points.fields={1}
points.x=0
reduce.size=0.5
polygons.path=s3a://gfw2-data/alerts-tsv/tsv-boundaries-gadm28/{2}
polygons.wkt=0
polygons.fields={3}
analysis.type={4}
    """.format(points_path, points_fields, poly_name, poly_fields, analysis_type)

    # if the geometry of interest isn't flagged as "large",
    # get the extent of data (using ogrinfo) and tack it
    # on to application_properties
    if '_large.tsv' not in poly_name:
        application_props += calc_tsv_extent.extent_props_str(poly_name)

    with open("application.properties", 'w') as app_props:
        app_props.write(application_props)


def gen_ns_list(poly_name):

    # checking extent for all input polygons, given that we have
    # to go 10 by 10 degrees
    min_x, min_y, max_x, max_y = calc_tsv_extent.get_extent(poly_name)

    dissolved_lat_geojson = r'/vsicurl/http://gfw2-data.s3.amazonaws.com/alerts-tsv/gis_source/lossdata_lat_diss.geojson'

    # build ogrinfo, including spat to refine extent
    cmd = ['ogrinfo', dissolved_lat_geojson, '-al', '-spat']
    cmd += [str(min_x), str(min_y), str(max_x), str(max_y)]

    # run ogrinfo and grab all responses with lat_id in it, except
    # for the first line, which just says that lat_id is a field
    response_list = calc_tsv_extent.run_subprocess(cmd)
    lat_list = [x for x in response_list if 'lat_id' in x.lower()][1:]

    # split line responses from lat_id (String) = 10N to 10N, etc
    ns_list = [x.split(' = ')[1] for x in lat_list]

    return ns_list


def call_pip():
    subprocess.call(['hdfs', 'dfs', '-rm', '-r', 'output'])
    pip_cmd = ['spark-submit', '--master', 'yarn']
    pip_cmd += ['--executor-memory', '20g']
    pip_cmd += ['--jars', r'target/libs/jts-core-1.14.0.jar', 'target/spark-pip-0.3.jar']

    subprocess.check_call(pip_cmd)


def check_output_exists(analysis_type, geom_csv_name, ns_tile=None):

    if analysis_type == 'extent':
        folder_name = os.path.splitext(geom_csv_name)[0]
        prefix = r'extent/{}/'.format(folder_name)
        out_csv = ns_tile + '.csv'

    else:
        out_csv = geom_csv_name
        prefix = r'{}/'.format(analysis_type)

    full_path_list = [key.name for key in bucket.list(prefix='alerts-tsv/output2016/{}'.format(prefix))]
    filename_only_list = [x.split('/')[-1] for x in full_path_list]

    return out_csv in filename_only_list


def upload_to_s3(analysis_type, tsv_name, ns_tile_name=None):
    cmd = ['hdfs', 'dfs', '-ls', 'output/']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = p.communicate()

    if "_SUCCESS" not in out:
        raise ValueError("process failed, success file not found")

    basename = os.path.splitext(tsv_name)[0]

    # Extent outputs should be <geom name>/10N.csv, 20N.csv etc
    if analysis_type == 'extent':
        csv_name = ns_tile_name + '.csv'
        out_path = r'extent/{}/'.format(basename)
    else:
        csv_name = basename + '.csv'
        out_path = r'{}/'.format(analysis_type)

    cmd = ['hdfs', 'dfs', '-getmerge', 'output/', csv_name]
    subprocess.check_call(cmd)

    tsv_outputs = 's3://gfw2-data/alerts-tsv/output2016/{}'.format(out_path)
    cmd = ['aws', 's3', 'mv', csv_name, tsv_outputs]
    subprocess.check_call(cmd)
