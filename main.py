from time import strftime, sleep
import socket
from urllib3.connection import HTTPSConnection
import requests
import xmltodict
import xml.etree.ElementTree as E
import pandas
from os import path
import requests as requests

if __name__ == '__main__':
    HTTPSConnection.default_socket_options = (
        HTTPSConnection.default_socket_options + [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.SOL_TCP, socket.TCP_KEEPIDLE, 45),
        (socket.SOL_TCP, socket.TCP_KEEPINTVL, 10),
        (socket.SOL_TCP, socket.TCP_KEEPCNT, 6)
    ]
    )
    url='https://192.168.231.160:8089'
    login='admin'
    password='qaz123456'
    earliest='-3d@'
    data = {
        'search': 'search index=_audit action=search "search"=* earliest='+earliest+' latest=now | fields search'
    }
    response = requests.post(url+'/services/search/jobs', data=data, verify=False,auth=(login,password))
    job_tree = E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot()
    sid = job_tree.find('./sid').text
    searches_from_month = []
    fields_from_month = {}
    while (True):
        response = requests.get(url+'/services/search/jobs/' + sid, data={'output_mode': 'json'}, verify=False, auth=(login,password))
        if (response.json()['entry'][0]['content']['dispatchState'] == 'DONE'):
            response = requests.get(url+'/services/search/jobs/' + sid + '/results?count=0',verify=False, auth=(login,password))
            break
    job_tree = E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot()
    for result in job_tree:
        if (result.tag == 'result'):
            searches_from_month.append(result.find("./field[@k='search']/value/text").text)
    for search in searches_from_month:
        data = {
            "search": search.strip('\'')
        }
        response = requests.post(url+'/services/search/jobs', data=data, verify=False,auth=(login,password))
        if (response.status_code >= 200 and response.status_code < 300):
            search_sid = E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot().find('./sid').text
            command = {
                'action': 'finalize'
            }
            sleep(2)
            search_results = response.content.decode('utf-8')
            requests.post(url+'/services/search/jobs/' + search_sid + "/control", data=command,verify=False, auth=(login, password))
            response = requests.get(url+'/services/search/jobs/' + search_sid + '/results_preview', verify=False,auth=(login,password))
            used_fields_months = {}
            if (response.content.decode('utf-8') != ''):
                job_tree = E.ElementTree(E.fromstring(response.content.decode('utf-8'))).getroot()
                for result in job_tree:
                    t_index = result.find("./field[@k='index']")
                    if t_index is not None:
                        t_index = t_index.find('./value/text')
                        print(t_index.text)
                    t_source = result.find("./field[@k='source']")
                    if t_source is not None:
                        t_source = t_source.find('./value/text')
                    t_sourcetype = result.find("./field[@k='sourcetype']")
                    if t_sourcetype is not None:
                        t_sourcetype = t_sourcetype.find('./value/text')
                    for field in result:
                        if (field.tag == 'field'):
                            if t_index is not None and t_sourcetype is not None and t_source is not None:
                                used_fields_months[t_index.text] = used_fields_months.get(t_index.text, {})
                                if (used_fields_months[t_index.text].get(field.attrib['k'], False) == False and
                                        field.attrib['k'] != 'source' and field.attrib['k'] != 'sourcetype' and
                                        field.attrib['k'] != 'index'):
                                    fields_from_month[t_index.text] = fields_from_month.get(t_index.text, {})
                                    fields_from_month[t_index.text][t_source.text] = fields_from_month.get(t_index.text,
                                                                                                           {}).get(
                                        t_source.text, {})
                                    fields_from_month[t_index.text][t_source.text][
                                        t_sourcetype.text] = fields_from_month.get(t_index.text, {}).get(t_source.text,
                                                                                                         {}).get(
                                        t_sourcetype.text, {})
                                    fields_from_month[t_index.text][t_source.text][t_sourcetype.text][
                                        field.attrib['k']] = fields_from_month.get(t_index.text, {}).get(t_source.text,
                                                                                                         {}).get(
                                        t_sourcetype.text, {}).get(field.attrib['k'], 0) + 1
                                    used_fields_months[t_index.text][field.attrib['k']] = used_fields_months.get(
                                        t_index.text).get(field.attrib['k'], True)

    if (fields_from_month != {}):
        data = []
        for index in fields_from_month:
            pdata = {'index': index}
            for source in fields_from_month[index]:
                pdata['source'] = source
                for sourcetype in fields_from_month[index][source]:
                    pdata['sourcetype'] = sourcetype
                    for pair in fields_from_month[index][source][sourcetype]:
                        pdata[pair] = fields_from_month[index][source][sourcetype][pair]
            data.append(pdata)
        dfm = pandas.DataFrame(data=data)
        dfm = dfm.fillna(0)
        dfm.to_csv('report_time.csv', index=False)
        print(dfm)

