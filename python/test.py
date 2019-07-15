from config.config import *

if __name__ == "__main__":
    logger = logging.getLogger(os.path.basename(__file__))
    logger.addHandler(logger_console_handler)
    logger.addHandler(logger_file_handler)

    logger.info('-'*80)
    logger.info('start_date is {}'.format(start_date))
    logger.info('end_date is {}'.format(end_date))
    logger.info('output_dir is {}'.format(output_dir))

    print(query_prefix)