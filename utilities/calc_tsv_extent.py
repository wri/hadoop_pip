import os
import sys
import subprocess


def get_extent(s3_path):

    write_vrt(s3_path)
    
    # ogrinfo, vrt name, layer name inside vrt
    cmd = ['ogrinfo', 'data.vrt', 'data', '-so', '-al', '-ro']
    response_list = run_subprocess(cmd)
    
    extent_line = [x for x in response_list if 'extent' in x.lower()][0]

    return extent_str_to_ints(extent_line)
    
def extent_props_str(s3_path):

    min_x, min_y, max_x, max_y = get_extent(s3_path)

    prop_str = """
extent.xmin={}
extent.ymin={}
extent.xmax={}
extent.ymax={}
    """.format(min_x, min_y, max_x, max_y)
    
    return prop_str
    

def extent_str_to_ints(extent_str):

    extent_split = extent_str.split(' - ')
    
    # extract corner coords, adding 0.5 degree buffer
    min_x, min_y = unpack_extent_tuple(extent_split[0], -0.25)
    max_x, max_y = unpack_extent_tuple(extent_split[1], 0.25)
    
    # return float values with 0.5 degree buffer
    return min_x, min_y, max_x, max_y
    
    
def unpack_extent_tuple(extent_tuple, offset):

    x, y = extent_tuple.replace(')', '').split('(')[1].split(',')

    return float(x) + offset, float(y) + offset



def write_vrt(s3_path):

    vrt_text = '''<OGRVRTDataSource>
    <OGRVRTLayer name="data">
        <SrcDataSource>/vsicurl/http://gfw2-data.s3.amazonaws.com/alerts-tsv/tsv-boundaries-gadm28/{0}.tsv</SrcDataSource>
        <SrcLayer>{0}</SrcLayer>
        <GeometryType>wkbPolygon</GeometryType>
        <GeometryField encoding="WKT" field='field_1'/>
    </OGRVRTLayer>
</OGRVRTDataSource>'''.format(os.path.splitext(os.path.basename(s3_path))[0])

    with open('data.vrt', 'w') as thefile:
        thefile.write(vrt_text)



def run_subprocess(cmd):

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess_list = []

    # Read from STDOUT and raise an error if we parse one from the output
    for line in iter(p.stdout.readline, b''):
        subprocess_list.append(line.strip())

    print '\n'.join(['-'*30] + subprocess_list + ['-'*30])

    return subprocess_list        
        

if __name__ == '__main__':
	print extent_props_str(sys.argv[1])
