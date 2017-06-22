import argparse,tarfile,os,shutil
from impala.dbapi import connect

parser = argparse.ArgumentParser(description="Backup hive databse tables")
parser.add_argument("tablename", type=str, help="Tablename")
args = parser.parse_args()

host="localhost"
table=args.tablename

parent_path=os.getcwd()
os.mkdir('{}'.format(table), 0o755)
os.chdir('{}'.format(table))
conn = connect(host='{}'.format(host), port=21050)
cursor = conn.cursor()
cursor.execute('show create table {}'.format(table))
results = cursor.fetchall()
f = open('{}.schema'.format(table),'w')
for row in results:
        f.write('\n'.join(row))
f.close()
lines = open('{}.schema'.format(table)).readlines()
open('{}.schema'.format(table), 'w').writelines(lines[:-1])
cursor.execute('select * from {}'.format(table))
results = cursor.fetchall()
f = open('{}.data'.format(table),'w')
for row in results:
        f.write(str(row).replace("'", "").replace(", ", ",").replace("(", "").replace(")", "")+'\n')
f.close()
os.chdir('{}'.format(parent_path))
with tarfile.open('{}.tar.gz'.format(table), "w:gz") as tar:
        tar.add('{}'.format(table))
shutil.rmtree('{}'.format(table))
