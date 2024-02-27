import configparser
import os
import sys
from datetime import date
import tableauserverclient as TSC
from google.cloud.storage import Client, transfer_manager

input_location = os.path.dirname(sys.argv[0])
output_location = '/Users/regina.kanya/Documents/workbooks/'
current_date = date.today().strftime("%Y-%m-%d")
local_path = os.path.join(output_location, f'{current_date}/')
remote_path = f'{current_date}/'
folder_name = 'acasus-tableau-backup'

config = configparser.ConfigParser()
config.read(input_location + '/tableau_in_cloud.ini')

token_name = config['tableau']['token_name']
personal_access_token = config['tableau']['personal_access_token']
server_url = config['tableau']['server_url']
api_version = config['tableau']['api_version']
site_name = config['tableau']['site_name']

tableau_server_config = {
    'tableau_prod': {
        'server': server_url,
        'api_version': api_version,
        'personal_access_token_name': token_name,
        'personal_access_token_secret': personal_access_token,
        'site_name': '',
        'site_url': '',
        'site_id': ''
    }
}


def download_item(server, page_options, output_location_date, items):
    cnt = 0
    for item in TSC.Pager(items, page_options):
        cnt = cnt + 1
        if not os.path.isdir(output_location_date + '/' + item.project_name):
            os.mkdir(output_location_date + '/' + item.project_name)
        file_name = output_location_date + '/' + item.project_name + '/' + item.name.replace("/", "_")
        file_path = items.download(item.id, file_name)
        print("{0} - Downloaded the file to {1}/{2}.".format(cnt, item.project_name, item.name))
    print("Done {0} {1}".format(cnt, items))


def upload_items(folder_name, filenames, source_directory="", workers=8, expect=None):
    storage_client = Client()
    bucket = storage_client.bucket(folder_name)

    results = transfer_manager.upload_many_from_filenames(
        bucket, filenames, source_directory=source_directory, max_workers=workers)

    for root, dirs, files in os.walk(local_path):
        for filename in files:
            local_file_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_file_path, local_path)
            remote_file_path = os.path.join(remote_path, relative_path)

            blob = bucket.blob(remote_file_path)
            blob.upload_from_filename(local_file_path)
            print("Uploaded {} to {}.".format(filename, bucket.name))


def main():
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, personal_access_token, site_id='')
    server = TSC.Server(server_url)
    with server.auth.sign_in(tableau_auth):
        if not os.path.isdir(output_location): os.mkdir(output_location)
        output_location_date = output_location + date.today().strftime("%Y-%m-%d")
        if not os.path.isdir(output_location_date): os.mkdir(output_location_date)
        page_options = TSC.RequestOptions(1, 5)
        items_to_download = [server.datasources, server.workbooks]
        for items in items_to_download:
            print("Downloading {0} to folder {1}".format(items, output_location_date))
            download_item(server, page_options, output_location_date, items)


if __name__ == '__main__':
    main()
    filenames = os.listdir(local_path)
    upload_items(folder_name, filenames, source_directory="", workers=8)
