#!/usr/bin/python

# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
# https://pypi.org/project/kafka-python/
# Dan Weeks' Rita Script

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import base64
import re
import itertools


def walk(d, perm):
    global path
    global outjson

    try:
        # If d is a list it wont have .items() so we need to iterate the list and walk() each list element which *should* be a json dict
        if isinstance(d, list):
            for jsonItem in d:
                walk(jsonItem, perm)
        else:
            for k,v in d.items():
                if isinstance(v, str) or isinstance(v, int) or isinstance(v, float):
                    path.append(k)
#                    print perm+"{}={}".format("".join(k), v)
#                    print perm + "{}".format("".join(k)) + " " + str(v)
                    outjson[perm+"{}".format("".join(k))] = str(v)
#                    print("a")
                    path.pop()
                elif isinstance(v, dict):
                    path.append(k)
#                    print(k)
#                    print(v)
#                    print("c")
                    walk(v, perm+k+"_")
                    path.pop()
                elif isinstance(v, unicode):
                    # DEBUG: prints all keys
#                    print k
                    # If the key is message then we recurse and walk the "top" part of the structure
                    if k == "message":
                        try:
                            if isinstance(v, dict):
                                walk(v, "message_")
                            else:
                                walk(json.loads(v), "message_")
# TODO: Catch specific exception for this case, don't assume they are all the same!
                        except Exception as e:
                            # If the object is not walkable its probably a unicode flat JSON so we have to allow it to throw the exception because we came here expected a deep unicode so we can walk it. Because this is not deep we have to throw exception, catch it and add it to the outjson
                            if isinstance(v, unicode):
                                outjson[perm+"{}".format("".join(k))] = v
                            else:
                                print("There is still something wrong")
#                            print("------------------derp 1")
#                            print(e)
#                            print(perm)
#                            print(type(v))
#                            print(v)
#                            print(d)
#                            print(outjson)
                        # This continue stops the rest of the for loop adding the same as above to the outjson but without the prefix!
                        continue
                    # If the key is b64 then we will need to decode it and recurse
                    if k == "b64":
                        # Make sure the b64 is valid (catch the Exception)
                        try:
                            # If somehow the b64 is valid but the len is too small we don't care
                            if (len(v) < 10): 
                                # TODO: perhaps its not worth printing here unless debug ?
                                # This happens becauses there is no data in the call, i.e. a ping
#                                print("B64 too short")
#                                print(v)
#                                print(perm)
#                                print(d)
#                                print(outjson)
                                # Skip this key (stops the for loop code adding this to the outjson)
                                continue
                            # Else its large enough and valid so we will decode it and recurse
                            else:
                                # DEBUG Print to the screen the decoded b64 string
#                                print(base64.b64decode(v))
                                # Decode b64 data and call ourself with perm + b64_ prefer (should be "message_b64_")
                                try:
                                    if isinstance(base64.b64decode(v), dict):
                                        walk(base64.b64decode(v), perm + "b64_")
                                    else:
                                        walk(json.loads(base64.b64decode(v)), perm + "b64_")
                                except Exception as e:
                                    # Any errors will remain base64 encoded but maintain the original data path
                                    outjson[perm+"{}".format("".join(k))] = v
#                                    print("---------------------derp 2")
#                                    print(e)
#                                    print(perm)
#                                    print(type(base64.b64decode(v)))
#                                    print(base64.b64decode(v))
#                                    print(d)
#                                    print(outjson)
                        # This continue stops the rest of the for loop adding the same as above to the outjson but without the prefix!
    
                                # Skip further processing of this key. This continue is here to stop the rest of the code in the for loop from adding b64 to outjson
                                continue
                        # Catches exceptions related to invalid json
                        except ValueError as e:
                             # TODO: perhaps not worth printing here unless debug ?
#                            print("Not valid base64")
                            # Skip this key. This is here to stpo the rest of the code in the for loop from adding invalid json to outjson
                            continue

                    # Add the key to the path structure ("message_b64_" + k) so for each recursion call, path gets "deeper" because path is passed to walk so "message_b64_customer_" + k etc
                    path.append(k)
#                    print "{}={}".format("_".join(path), v)
#                    print(k + " " + v)
#                    print(perm + "" + k + "=" + v)
                    # Add path (as explained above) + k (current key) to outjson with the value
                    outjson[perm+"{}".format("".join(k))] = v
#                    print("d")
                    path.pop()
                elif isinstance(v, list):
#                    walk(v, perm)
                     continue
                else:
                    print "###Type {} not recognized: {}.{}={}".format(type(v), ".".join(path),k, v)
                    print(k)
                    print(v)
    except Exception as e:
# TODO: perhaps not worth printing here unless debug ?
        print("<<<<<<<<<<<<<<<<<<<<<<< herp")
        print(e)
        print(perm)
        print(type(d))
        print(d)
        print(outjson)
        print("<<<<<<<<<<<<<<<<<<<<<<< derp")

#i=1
consumer = KafkaConsumer('rita-stingray-kafka', bootstrap_servers='0.0.0.0:9092', client_id='kafkamite', group_id='ritamite', auto_offset_reset='earliest')
producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092')

for msg in consumer:
#try:
    txn=''
#    i=i+1
    txnj=msg.value
#    print ">>> ====== RAW ============\n"
#    print(txnj)
    outjson = {}

    try:
        txn = json.loads(txnj, strict=False)
    except Exception as e:
        print("<<<<<<<<<<<<<<<<<<<<<<< serp")
        print(e)


#    print "=========  JSON TXN ===========\n"
#    print(txn)
#    print "=========  JSON MESSAGE ===========\n"
#    print(mess)

    try:
        path=[]
        walk(txn, "")
#        print(json.dumps(outjson))

        matchObj = re.match("^\/acceptor\/rest\/transactions\/ping$",outjson['message_path'])   
        if ( matchObj ):
            outjson['type']="ping"

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/payment$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="payment"
            outjson['instid']= matchObj.group(1)

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/(\d)$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="gettxn"
            outjson['instid']= matchObj.group(1)
            outjson['apid']= matchObj.group(2)

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/verify$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="verify"
            outjson['instid']= matchObj.group(1)

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/byRef$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="txnbyref"
            outjson['instid']= matchObj.group(1)

        matchObj = re.match("^\/acceptor\/rest\/customers\/(\d+)\/byRef$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="custbyref"
            outjson['instid']= matchObj.group(1)

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/(\d+)\/capture$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="capture"
            outjson['instid']= matchObj.group(1)
            outjson['apid']= matchObj.group(2)

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/(\d+)\/resume$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="resume"
            outjson['instid']= matchObj.group(1)
            outjson['apid']= matchObj.group(2)

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/(\d+)\/repeat$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="repeat"
            outjson['instid']= matchObj.group(1)
            outjson['apid']= matchObj.group(2)

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/(\d+)\/refund$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="refund"
            outjson['instid']= matchObj.group(1)
            outjson['apid']= matchObj.group(2)

        matchObj = re.match("^\/acceptor\/rest\/transactions\/(\d+)\/(\d+)\/cancel$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="cancel"
            outjson['instid']= matchObj.group(1)
            outjson['apid']= matchObj.group(2)

        matchObj = re.match("^\/acceptor\/rest\/cardinfo\/(\d+)\/(.+)$",outjson['message_path'])   
        if ( matchObj and matchObj.group(1)):
            outjson['type']="cardinfo"
            outjson['instid']= matchObj.group(1)
            outjson['cardinfoData']= matchObj.group(2)


#        print "========= Data to produce =========\n"
#        print(json.dumps(outjson))
        producer.send('rita-kafmgmt101-esmgmt',json.dumps(outjson))
#        print "=============   END   =============\n"
    except Exception as e:
        print("<<<<<<<<<<<<<<<<<<<<<<< herp")
        print(e)

#except:
#    print("derp")
