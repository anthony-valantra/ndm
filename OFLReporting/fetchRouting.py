import json
import requests
import json
import pymongo
from pymongo import MongoClient
from xml.dom import minidom
from datetime import datetime


def fetch_from_sterling(traceLevel):

    url='http://oflperf2.staples.com/smcfs/interop/InteropHttpServlet'
    progid='YFSEnvironment.progId=yantra'
    interopapi='InteropApiName=getRoutingGuideDetailList'
    flowtype='IsFlow=N'
    invokeflow='InvokeFlow='
    service='ServiceName='
    locale='YFSEnvironment.locale='
    userid='YFSEnvironment.userId=admin'
    password='YFSEnvironment.password=password'
    interopApiData='InteropApiData=<RoutingGuideDetail MaximumRecords="35000"></RoutingGuideDetail>'
    template='TemplateData'

    request = url+'?'+progid+'&'+interopapi+'&'+flowtype+'&'+invokeflow+'&'+service+'&'+locale+'&'+userid+'&'+password+'&'+interopApiData+'&'+template
    if traceLevel <= 1:
        print url,interopapi,interopApiData

    r = requests.get(request)
    response = r.content

    if traceLevel==1:
        print request

    if traceLevel == 1:
        print response

    return response

def parse_element(element,traceLevel):
    dict_data = dict()
    if element.nodeType == element.TEXT_NODE:
        dict_data['data'] = element.data
    if element.nodeType not in [element.TEXT_NODE, element.DOCUMENT_NODE,
                                element.DOCUMENT_TYPE_NODE]:
        for item in element.attributes.items():
            dict_data[item[0]] = item[1]

    if element.nodeType not in [element.TEXT_NODE, element.DOCUMENT_TYPE_NODE]:
        for child in element.childNodes:
            child_name, child_dict = parse_element(child,traceLevel)

            if child_name in dict_data:
                try:
                    dict_data[child_name].append(child_dict)
                except AttributeError:
                    dict_data[child_name] = [dict_data[child_name], child_dict]
            else:
                dict_data[child_name] = child_dict
    return element.nodeName, dict_data


def write_to_mongodb(collection,jsondata,traceLevel):
    if (traceLevel <= 1):
        print str(datetime.now()), "Updating mongoDB collection " , collection
    try:
        client = MongoClient("mongodb://localhost:27017")
        db = client['RoutingGuideDetails']
    except pymongo.errors.ServerSelectionTimeoutError, f:
        if traceLevel == 1:
            print "Could not connect to server: %s" % f
    except pymongo.errors.ConnectionFailure, e:
        if traceLevel == 1:
            print "Could not connect to server: %s" % e
    for i in range(len(jsondata['RoutingGuideDetails']['RoutingGuideDetail'])):
        try:
            route = jsondata['RoutingGuideDetails']['RoutingGuideDetail'][i]
            db.get_collection(collection).insert(route)
            if traceLevel == 1:
                print "\ninserting ",route
        except pymongo.errors.DuplicateKeyError, e:
            locateExistingRouteQuery = {"ShipFrom.ShipFromNode" : route["ShipFrom"]["ShipFromNode"] , "ShipTo.ShipToRegion" :  route["ShipTo"]["ShipToRegion"]  , "RoutingGuide.SelectionCriteria.ItemClassification" :  route["RoutingGuide"]["SelectionCriteria"]["ItemClassification"] }
            cursor= db.get_collection(collection).find(locateExistingRouteQuery)
            for existing_doc in cursor:
                message = e.message.split(":", 3)
                values = e.message.split("{", 2)
                if traceLevel == 1:
                    print "\nError message : ", message[0], "\n\tValues : ", values[1] , "\n\tIndex : ", message[2]
                if traceLevel == 1:
                    print "located existing record...", existing_doc
                db.get_collection(collection).remove(locateExistingRouteQuery)
                if traceLevel == 1:
                    print "removed existing record..."
                db.get_collection(collection).insert(route)
                if traceLevel == 1:
                    print "inserting record...", route

        i += 1

    print "Total records in, " , collection , " : " , db.get_collection('RoutingGuideDetails').find().count()
    cursor = db.get_collection(collection).find()
   # for existing_doc in cursor:
   #     print existing_doc["RoutingGuide"]["OrganizationCode"],existing_doc["RoutingGuide"]["SelectionCriteria"]["ItemClassification"], existing_doc["ShipFrom"]["ShipFromNode"],existing_doc["ShipTo"]["ShipToRegion"], existing_doc["RoutingGuideDetailCarrierList"]["RoutingGuideDetailCarrier"][0]["ScacAndService"]["ScacAndService"]


if __name__ == '__main__':
    traceLevel=0

    if (traceLevel<=1):
        print str(datetime.now()),"fetching from sterling"
    sterling_resp=fetch_from_sterling(traceLevel)

    if (traceLevel <= 1):
        print str(datetime.now()),"parsing sterling response into xml"
    xml_response = minidom.parseString(sterling_resp)

    if (traceLevel <= 1):
        print str(datetime.now()),"parsing xml response into json"
    json_response = parse_element(xml_response,traceLevel)[1]

    if (traceLevel <= 1):
        print str(datetime.now()),"writing json response into file - C:/Analysis/data.json"
        f = open('C:\Analysis\data.json', 'w')
        f.write(json.dumps(json_response, sort_keys=True, indent=4))
        f.close()

    if (traceLevel <= 1):
        print str(datetime.now()),"writing json response into mongodb"
    write_to_mongodb("RoutingGuideDetails",json_response,traceLevel)
    if (traceLevel <= 1):
        print str(datetime.now()), "Finished writing json response into mongodb"