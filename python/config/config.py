import configparser
import datetime
import logging
import os

config = configparser.RawConfigParser()
config.optionxform = str
config.read('config_test.cfg')

# input
start_date = config.get('input', 'start_date')
end_date = config.get('input', 'end_date')

# output
output_dir = config.get('output', 'output_dir')

query_prefix = '''
SET hive.merge.tezfiles=true;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.tez.java.opts=-Xmx12288m;
SET tez.am.resource.memory.mb=16384;
SET tez.am.launch.cmd-opts=-Xmx12288m;
SET tez.runtime.sort.threads=2;
SET tez.runtime.sorter.class=PIPELINED;

SET hcat.desired.partition.num.splits=100;
SET hive.exec.orc.split.strategy=hybrid;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.execution.engine=tez;
SET tez.am.resource.memory.mb=6144;
SET tez.am.launch.cmd-opts =-Xmx4096m;
SET hive.tez.container.size=6144;
SET hive.tez.java.opts=-Xmx4096m;
'''


def date_range(start_date, end_date):
    start_date = datetime.datetime.strptime(start_date, '%Y%m%d').date()
    end_date = datetime.datetime.strptime(end_date, '%Y%m%d').date()
    return [(start_date + datetime.timedelta(n)).strftime('%Y%m%d')
            for n in range((end_date - start_date).days + 1)]


# logging
log_file = 'result.log'
logging.basicConfig(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger_console_handler = logging.StreamHandler()
logger_console_handler.setFormatter(formatter)
logger_file_handler = logging.FileHandler(filename=log_file)
logger_file_handler.setFormatter(formatter)