from boto.s3.connection import S3Connection
import shutil
import os
import subprocess

conn = S3Connection(host="s3.amazonaws.com")
bucket = conn.get_bucket('gfw2-data')


def download_jar():
    
    jar_file = 's3://gfw2-data/alerts-tsv/batch-processing/target_0.3.3.zip'
             
    cmd = ['aws', 's3', 'cp', jar_file, '.']
    subprocess.check_call(cmd)
    
    jar_name = os.path.basename(jar_file)
    cmd = ['unzip', jar_name]
    subprocess.check_call(cmd)
        
    
def write_props(analysis_type, points_path, poly_name): 
    four_poly_fields = ['bra_biomes_int_gadm28.tsv', 'fao_ecozones_bor_tem_tro_sub_int_diss_gadm28.tsv', 'wdpa_keep_wdpaid_diss_int_gadm28.tsv']
    
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
    
    with open("application.properties", 'w') as app_props:
        app_props.write(application_props)
        
  
def gen_ns_list():
    # iterate over the 00N* bc extent can't run all the tiles at once..
    north_list = ['{:02d}N'.format(x) for x in range(0, 90, 10)]
    south_list = ['{:02d}S'.format(x) for x in range(10, 60, 10)]
    ns_list = north_list +south_list    
    
    return ns_list

    
def call_pip():
    subprocess.call(['hdfs', 'dfs', '-rm', '-r', 'output'])
    pip_cmd = ['spark-submit', '--master', 'yarn']
    pip_cmd += ['--executor-memory', '20g']
    pip_cmd += ['--jars', r'target/libs/jts-core-1.14.0.jar', 'target/spark-pip-0.3.jar']

    subprocess.check_call(pip_cmd)

    
def check_output_exists(analysis_type, out_csv):
    full_path_list = [key.name for key in bucket.list(prefix='alerts-tsv/output2016/{}/'.format(analysis_type))]
    filename_only_list = [x.split('/')[-1] for x in full_path_list]
    
    return out_csv in filename_only_list
           

def upload_to_s3(analysis_type, tsv_name):
    cmd = ['hdfs', 'dfs', '-ls', 'output/']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = p.communicate()

    if "_SUCCESS" not in out:
        raise ValueError("process failed, success file not found") 

    csv_name = tsv_name.replace(".tsv", ".csv")    
    
    cmd = ['hdfs', 'dfs', '-getmerge', 'output/', csv_name]
    subprocess.check_call(cmd)
    
    tsv_outputs = 's3://gfw2-data/alerts-tsv/output2016/{}/'.format(analysis_type)
    cmd = ['aws', 's3', 'mv', csv_name, tsv_outputs]
    subprocess.check_call(cmd)
    


            
        
        
        
        
  
