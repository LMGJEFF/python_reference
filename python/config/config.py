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
SET hive.tez.java.opts=-xmx12288m;
SET tez.am.resource.memory.mb=16384;
SET tez.am.launch.cmd-opts=-xmx12288m;
SET tez.runtime.sort.threads=2;
SET tez.runtime.sorter.class=PIPELINED;
SET tez.runtime.io.sort.mb=512;

SET hcat.desired.partition.num.splits=100;
SET mapreduce.input.fileinputformat.split.maxsize=67108864;
SET mapreduce.input.fileinputformat.split.minsize=33554432;
SET mapreduce.input.fileinputformat.split.minsize.per.rack=33554432;
SET mapreduce.input.fileinputformat.split.minsize.per.node=33554432;
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