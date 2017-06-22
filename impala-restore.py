import argparse,tarfile,os,shutil,re,itertools
from impala.dbapi import connect

parser = argparse.ArgumentParser(description="Restore hive databse tables")
parser.add_argument("tablename", type=str, help="Tablename")
args = parser.parse_args()

host="localhost"
table=args.tablename

t = tarfile.open('{}.tar.gz'.format(table), 'r')
t.extractall('')
os.remove('{}.tar.gz'.format(table))
os.chdir('{}'.format(table))
perm_table=table
temp_table=table+"_temp"
f = open('{}.schema'.format(table),'r')
q=list(f)
indeces = list(i for i,val in enumerate(q) if val.startswith('PARTITIONED'))
pos=int(str(indeces).replace('[','').replace(']',''))
del q[pos]
q[pos-1]=','
temp_query="\n".join(q).replace('\n',' ').replace('{}'.format(perm_table),'{}'.format(temp_table))

f = open('{}.schema'.format(table),'r')
q=list(f)
perm_query="\n".join(q).replace('\n',' ')

f = open('{}.schema'.format(table),'r')
p=list(f)
indeces1 = list(i for i,val in enumerate(p) if val.startswith('PARTITIONED'))
pos1=int(str(indeces1).replace('[','').replace(']',''))
count=pos1+1
part=[]
while count > 0:
	if re.search("\)","{}".format(p[count])):
		break
	else:
		part.append([((p[count].split('  ', 1)[1]).split(' ', 1)[0])])
		count=count+1
partition=(','.join(map(str,(list(itertools.chain.from_iterable(part))))))
os.popen("hdfs dfs -mkdir /user/{}".format(perm_table)).read()
os.popen("hdfs dfs -put {}.data /user/{}".format(perm_table,perm_table)).read()
os.popen("hdfs dfs -chmod -R 777 /user/{}/".format(perm_table)).read()

conn = connect(host='{}'.format(host), port=21050)
cursor = conn.cursor()
cursor.execute('{}'.format(temp_query))
cursor.execute('truncate table {}'.format(temp_table))
cursor.execute("load data inpath '/user/{}/{}.data' into table {}".format(perm_table,perm_table,temp_table))
cursor.execute('{}'.format(perm_query))
cursor.execute('truncate table {}'.format(perm_table))
cursor.execute('insert into table {} partition({}) select * from {}'.format(perm_table,partition,temp_table))
cursor.execute('drop table {}'.format(temp_table))
os.popen("hdfs dfs -rm -r -skipTrash /user/{}".format(perm_table)).read()
os.chdir('..')
shutil.rmtree('{}'.format(perm_table))
