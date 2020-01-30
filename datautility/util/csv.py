import csv


def is_valid(file_path):
    '''
    Check if the csv file has proper definition.
    '''
    try:
        with open(file_path, 'r') as ds_file:
            dialect = csv.Sniffer().sniff(ds_file.read())

            return True
    except:
        return False



def load(src):
    '''
    Converts the csv file into list.
    :param src Location of the csv file.
    :type string
    :param validate Boolean value that determines whether of not to validate file for consistency.
    :type bool
    '''
    with open(src, 'r') as csv_file:
        return list(csv.reader(csv_file))
