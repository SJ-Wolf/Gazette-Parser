from pprint import pprint
import time
import csv
import math
import os
import pickle
import re
import subprocess
import zipfile

import psycopg2
import requests
from bs4 import BeautifulSoup
from lxml import html


def download_links_to_file():
    url = 'https://bulkdata.uspto.gov/'

    r = requests.get(url)

    s = BeautifulSoup(r.text, 'html.parser')

    all_links = s.find_all('a')
    gazette_links = []
    for link in all_links:
        try:
            if link.attrs['href'].startswith('https://bulkdata.uspto.gov/data2/patent/officialgazette/'):
                gazette_links.append(link.attrs['href'])
        except KeyError:
            pass

    download_links = []
    for gaz_link in gazette_links:
        r = requests.get(gaz_link)
        s = BeautifulSoup(r.text, 'html.parser')
        for link_elem in s.find_all('a'):
            try:
                if link_elem.attrs['href'].endswith('.zip'):
                    download_links.append(gaz_link + '/' + link_elem.attrs['href'])
            except KeyError:
                pass

    with open('gaz_links', 'w') as f:
        f.writelines([x + '\n' for x in download_links])


def download_gaz_files_from_file():
    with open('gaz_links', 'r') as f:
        download_links = [x[:-1] for x in f.readlines()]

    subprocess.call('split -l {} -a 1 gaz_links downloads/split_gaz_links_'.format(math.ceil(len(download_links) / 3)),
                    shell=True)
    os.chdir('downloads')
    subprocess.call('wget -i split_gaz_links_a -nc &', shell=True)
    subprocess.call('wget -i split_gaz_links_b -nc &', shell=True)
    subprocess.call('wget -i split_gaz_links_c -nc &', shell=True)
    os.chdir('..')


# fills out zip_info
def insert_file_info_postgres():
    os.chdir('downloads')
    zip_files = [x for x in os.listdir('.') if x.endswith('.zip')]

    conn = psycopg2.connect(dbname='zip_info', user='scott', host='localhost', password='313ctric')
    cur = conn.cursor()

    cur.execute("""DROP TABLE IF EXISTS zip_info;""")
    cur.execute("""CREATE TABLE zip_info (
    zip_file TEXT,
    directory TEXT,
    file_name TEXT,
    full_name TEXT
    );
    """)

    file_info_data = []
    for cur_zip_file in zip_files:
        print(cur_zip_file)
        with zipfile.ZipFile(cur_zip_file, 'r') as f:
            for zipped_file in f.namelist():
                if 'OG/' not in zipped_file:
                    continue
                file_info = (
                    cur_zip_file, zipped_file[zipped_file.find('OG/'):zipped_file.rfind('/')],
                    zipped_file[zipped_file.rfind('/') + 1:], zipped_file)
                file_info_data.append(file_info)
    fast_insert_many(data=file_info_data, table='zip_info', cur=cur)
    conn.commit()
    cur.close()
    conn.close()
    os.chdir('../')


# extracts files given in zip_info
def extract_table_files():
    conn = psycopg2.connect(dbname='zip_info', user='scott', host='localhost', password='313ctric')
    cur = conn.cursor()
    cur.execute("""
        SELECT zip_file, json_agg(full_name)
        FROM zip_info
        WHERE directory = 'OG' AND file_name NOT LIKE 'Cpc%' AND (file_name = 'patent.htm%' OR file_name LIKE '%Body.htm%')
        GROUP BY zip_file;
        """)

    files_data = cur.fetchall()
    cur.close()
    conn.close()

    os.chdir('zip_extracts')
    for row in files_data:
        file_name = '../downloads/' + row[0]
        files_to_extract = row[1]
        print(file_name)
        with zipfile.ZipFile(file_name, 'r') as z:
            z.extractall(path=row[0], members=files_to_extract)

    os.chdir('../')


# fills out html_table_cell and html_patent
def parse_table_files():
    conn = psycopg2.connect(dbname='zip_info', user='scott', host='localhost', password='313ctric')
    cur = conn.cursor()
    cur.execute("""
        SELECT file_name, json_agg(zip_file || '/' || full_name)
        FROM zip_info
        WHERE directory = 'OG' AND file_name NOT LIKE 'Cpc%' AND (file_name = 'patent.html' OR file_name LIKE '%Body.htm%')
        GROUP BY file_name;
        """)

    files_data = cur.fetchall()
    cur.execute('DROP TABLE IF EXISTS html_table_cell;')
    cur.execute("""
        CREATE TABLE html_table_cell
        (
            file_name TEXT,
            file_path TEXT,
            row_num INTEGER,
            header_title TEXT,
            table_title TEXT,
            column_name TEXT,
            cell_name TEXT,
            cell_value TEXT,
            cell_href TEXT
        );""")
    cur.execute('DROP TABLE IF EXISTS html_patent;')
    cur.execute("""
        CREATE TABLE html_patent
        (
            file_name TEXT,
            file_path TEXT,
            patent_id TEXT
        );""")

    os.chdir('zip_extracts')
    html_patent_file_name = 'html_patent.csv'
    html_table_cell_file_name = 'html_table_cell.csv'
    if os.path.exists(html_patent_file_name):
        os.remove(html_patent_file_name)
    if os.path.exists(html_table_cell_file_name):
        os.remove(html_table_cell_file_name)
    html_patent_file = open(html_patent_file_name, 'a', newline='')
    html_patent_writer = csv.writer(
        html_patent_file, delimiter=',', escapechar='\\', lineterminator='\n')
    html_table_cell_file = open(html_table_cell_file_name, 'a', newline='')
    html_table_cell_writer = csv.writer(
        html_table_cell_file, delimiter=',', escapechar='\\', lineterminator='\n')

    for file_name, file_paths in files_data:
        print(file_name)
        if file_name == 'patent.html':
            patent_list_string_pattern = re.compile('var patentListString = "([a-z]|[A-Z]|[0-9]|,)+";')
            initial_pattern_length = len('var patentListString = "')
            data = []
            for file_path in file_paths:
                print('\t{}'.format(file_path))
                with open(file_path, 'r') as f:
                    file_text = f.read()
                    if len(file_text) < 10:
                        continue
                    patent_list_string_match = re.search(patent_list_string_pattern, file_text)
                    if patent_list_string_match:
                        patent_list_string = patent_list_string_match[0][initial_pattern_length:]
                        patent_list = [x.strip() for x in patent_list_string.split(',')]
                        data += [(file_name, file_path, x) for x in patent_list]
                    else:
                        print(file_path)
                        raise Exception("Can't find patentListString var.")
            html_patent_writer.writerows(data)
        else:
            data = []
            for file_path in file_paths:
                print('\t{}'.format(file_path))
                with open(file_path, 'r') as f:
                    file_text = f.read()
                    if len(file_text) < 10:
                        continue
                    root = html.fromstring(file_text)
                    head_title = root.xpath('/html/head/title')[0].text.strip()

                    table_elements = root.xpath('//table')
                    assert len(table_elements) == 1
                    table_elem = table_elements[0]
                    headers = None
                    table_title = None
                    for i, row in enumerate(table_elem.xpath('./tr')):
                        row_data = []
                        is_header = row.attrib.get('style') == 'margin-bottom:1em'  # defines header
                        if is_header:
                            headers = []
                        for cell in row.xpath('./td'):
                            a_elements = cell.xpath('./a')
                            if len(a_elements) == 0:
                                cell_text = cell.text
                                cell_href = cell.attrib.get('href')
                                cell_name = cell.attrib.get('name')
                            elif len(a_elements) == 1:
                                cell_text = a_elements[0].text
                                cell_href = a_elements[0].attrib.get('href')
                                cell_name = a_elements[0].attrib.get('name')
                            elif len(a_elements) == 2:
                                first_text = a_elements[0].text
                                sec_text = a_elements[1].text
                                cell_text = None
                                if first_text is not None and first_text.strip() != '':
                                    cell_text = first_text.strip()
                                if sec_text is not None and sec_text.strip() != '':
                                    if cell_text is not None:
                                        print(file_path)
                                        raise Exception('Two cell texts: "{}" and "{}"'.format(first_text, sec_text))
                                    cell_text = sec_text
                                cell_name = a_elements[0].attrib['name']
                                cell_href = a_elements[1].attrib['href']
                            elif len(a_elements) > 2:
                                print(file_path)
                                raise Exception('Too many a elements in cell.')

                            if cell_text is not None:
                                cell_text = cell_text.strip()
                                if cell_text == '':
                                    cell_text = None

                            if is_header:
                                headers.append(cell_text)
                            elif cell.attrib.get('style') == 'text-decoration:underline;margin-bottom:1em':
                                table_title = cell.text.strip()
                                assert table_title is not None
                            elif cell.attrib.get('colspan') is not None and (
                                            cell_text is None or cell_text.strip() == ''):
                                pass
                            else:
                                row_data.append(dict(
                                    cell_text=cell_text,
                                    cell_href=cell_href,
                                    cell_name=cell_name
                                ))
                        if headers is None and (
                                    (i == 0 and table_title is None) or (i == 1 and table_title is not None)) and (
                                    row_data[0]['cell_text'] in ('Class', 'Subclass', 'Patent', 'Subgroup')):
                            headers = [x['cell_text'] for x in row_data]
                        else:
                            if headers == [] or headers is None:
                                headers = [''] * len(row_data)
                            if len(row_data) > 0:  # possibly blank row (for spacing) or a header row
                                if len(row_data) != len(headers):
                                    print(file_path)
                                    raise Exception(
                                        'Unexpected number of cells in row. Found {}, should be {}.'.format(
                                            len(row_data),
                                            len(headers)))
                                for header_index, head in enumerate(headers):
                                    data.append((file_name, file_path, i if table_title is None else i - 1, head_title,
                                                 table_title, head, row_data[header_index]['cell_name'],
                                                 row_data[header_index]['cell_text'],
                                                 row_data[header_index]['cell_href'],))
            # cur.executemany("""
            #        INSERT INTO html_table_cell VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data)
            html_table_cell_writer.writerows(data)
    html_patent_file.close()
    html_table_cell_file.close()
    with open(html_patent_file_name, 'r') as f:
        cur.copy_from(f, 'html_patent', sep=',', null='', size=32000)
    with open(html_table_cell_file_name, 'r') as f:
        cur.copy_from(f, 'html_table_cell', sep=',', null='', size=32000)
    conn.commit()
    cur.close()
    conn.close()
    # os.remove(html_patent_file_name)
    # os.remove(html_table_cell_file_name)
    os.chdir('../')


# extracts most files in html_table_cell
def extract_raw_files():
    conn = psycopg2.connect(dbname='zip_info', user='scott', host='localhost', password='313ctric')
    cur = conn.cursor()
    cur.execute("""
        SELECT
          split_part(file_path, '/', 1) AS zip_file_name,
          json_agg(substring(file_path FROM length(split_part(file_path, '/', 1)) + 2 FOR
                    length(file_path) - length(file_name) - length(split_part(file_path, '/', 1)) - 1) || cell_href) AS file_name
        FROM html_table_cell
        WHERE column_name = 'Patent' AND header_title NOT IN
                                         ('Electrical Body', 'General and Mechanical Body', 'Chemical Body', 'Designs Body', 'Plants Body')
        GROUP BY split_part(file_path, '/', 1);
      """)
    file_extract_data = cur.fetchall()
    cur.close()
    conn.close()

    os.chdir('zip_extracts')
    for row in file_extract_data:
        file_name = '../downloads/' + row[0]
        files_to_extract = row[1]
        print(file_name)
        with zipfile.ZipFile(file_name, 'r') as z:
            z.extractall(path=row[0], members=files_to_extract)

    os.chdir('../')


# fills out csv_raw_patent
def parse_raw_files(redownload=True):
    conn = psycopg2.connect(dbname='zip_info', user='scott', host='localhost', password='313ctric')
    cur = conn.cursor()
    if redownload:
        cur.execute("""
            SELECT
              header_title,
              json_agg(distinct substring(file_path FOR
                        length(file_path) - length(file_name)) || cell_href) AS file_names
            FROM html_table_cell
            WHERE column_name = 'Patent' AND header_title NOT IN
                                             ('Electrical Body', 'General and Mechanical Body', 'Chemical Body', 'Designs Body', 'Plants Body')
            GROUP BY header_title
            ;
            """)
        print('Done executing.')
        raw_file_location_rows = cur.fetchall()
        raw_file_location_data = dict()
        for header_title, file_names in raw_file_location_rows:
            raw_file_location_data[header_title] = file_names
        with open('raw_file_location_data.pickle', 'wb') as f:
            pickle.dump(raw_file_location_data, f)
    else:
        with open('raw_file_location_data.pickle', 'rb') as f:
            raw_file_location_data = pickle.load(f)

    os.chdir('zip_extracts')

    data = []
    for header_title in raw_file_location_data.keys():
        for file_location in raw_file_location_data[header_title]:
            try:
                #if file_location != 'e-OG20021008_1263-2.zip/1263-2/OG/html/US05296902-20021008.html':
                #    continue
                with open(file_location, 'rb') as f:
                    file_text = f.read()
                    if len(file_text) < 10: # empty file
                        continue
                root = html.fromstring(file_text)

                # url, patent_id = get_patent_number_and_url(root)

                table_elements = root.xpath('//table')
                for t_index, t_elem in enumerate(table_elements):
                    for row_index, row_elem in enumerate(t_elem):
                        for cell_index, cell_elem in enumerate(row_elem):
                            cell_tostring = html.tostring(cell_elem, pretty_print=True, method='text', encoding='unicode')
                            cell_tostring = ' '.join(x.strip() for x in cell_tostring.split('\n') if x.strip() != '')
                            if cell_tostring != '':
                                data.append((file_location, header_title, t_index + 1, row_index + 1, cell_index + 1, cell_tostring))
            except:
                print(file_location)
                raise

    with open('raw_files_data.csv', 'w', newline='') as f:
        raw_files_writer = csv.writer(f, delimiter='\x0e', escapechar='\\', lineterminator='\n')
        raw_files_writer.writerows(data)

    cur.execute('DROP TABLE IF EXISTS csv_raw_patent')
    cur.execute("""
        CREATE TABLE csv_raw_patent
        (
            file_location TEXT,
            header_title TEXT,
            table_number INT,
            row_number INT,
            cell_number INT,
            line_text TEXT
        );""")
    with open('raw_files_data.csv', 'r') as f:
        cur.copy_from(f, 'csv_raw_patent', sep='\x0e', null='', size=32000)

    conn.commit()
    cur.close()
    conn.close()

    os.chdir('..')


def fast_insert_many(data, table, cur):
    with open('tmp.csv', 'w', newline='') as f:
        raw_files_writer = csv.writer(f, delimiter='\x0e', escapechar='\\', lineterminator='\n')
        raw_files_writer.writerows(data)
    with open('tmp.csv', 'r') as f:
        cur.copy_from(f, table, sep='\x0e', null='', size=32000)


def get_patent_number_and_url(root):
    a_elements = root.xpath('//a')
    for a_elem in a_elements:
        try:
            alt_text = a_elem.xpath('./img')[0].attrib.get('alt')
            if alt_text is not None and alt_text.startswith('Full Text Button for patent number'):
                return a_elem.attrib['href'], alt_text.replace('Full Text Button for patent number ', '')
        except IndexError:
            pass
    return None, None


def get_reexamination_data():
    conn = psycopg2.connect(dbname='zip_info', user='scott', host='localhost', password='313ctric')
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM csv_raw_patent
        WHERE header_title = 'Ex Parte Body' and file_location = 'e-OG20170131_1434-5.zip/1434-5/OG/html/1434-5/US08455531-20170131.html'
        ORDER BY file_location, table_number, row_number, cell_number
        ;
      """)
    file_extract_data = cur.fetchall()
    cur.close()
    conn.close()

    line_number = 0
    last_line = None

    patent_data = dict()
    for file_location, header_title, table_number, row_number, cell_number, line_text in file_extract_data:
        if last_line is None or last_line != (table_number, row_number):
            line_number += 1
        last_line = (table_number, row_number)
        if line_number == 1:
            assert patent_data.get('patent_number') is None
            patent_data['patent_number'] = line_text
        elif line_number == 2:
            assert patent_data.get('title') is None
            patent_data['title'] = line_text
        elif line_number == 3:
            assert patent_data.get('authors') is None
            patent_data['authors'] = line_text
        elif line_number == 4:
            assert patent_data.get('assigned_to') is None
            patent_data['assigned_to'] = line_text
        elif line_number == 5:
            assert patent_data.get('request_no') is None
            patent_data['request_no'] = line_text
        elif line_number == 6:
            assert patent_data.get('certificate_data') is None
            patent_data['certificate_data'] = line_text

    pprint(patent_data)

t0 = time.time()
get_reexamination_data()

print('Took {} seconds.'.format(time.time() - t0))
