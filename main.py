from etl import extract,transform
from util import load_data

def main():
    extract()
    transform()
    load_data('data_engineer_jobs')


main()