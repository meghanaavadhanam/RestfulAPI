from flask import Flask, jsonify, request
import redis
import uuid
import time, pymysql
from flask_restful import reqparse
import json

# initiating flask, redis, sql instances

app = Flask(__name__)
cache = redis.Redis(host='localhost', port=6379)
cache.flushdb()
conn = pymysql.connect(
        host='localhost',
        user='root',
        password = "Teradata900..",
        db='guid',
        )
cur = conn.cursor()

# The POST /guids endpoint creates a new GUID based on input json fields , which is stored in the MySQL database and Redis cache.
# The POST /guids/<guid_id> creates a new GUID based on given GUID in the URL and stores it in the MYSQL database and Redis Cache.

@app.route('/guids',  methods=['POST'])
@app.route('/guids/<guid_id>',  methods=['POST'])
def create_guid(guid_id='None'):

    #guid_id = request.json.get('guid')
    if guid_id == 'None':
        guid_id = str(uuid.uuid4().hex).upper() # 32 hexadecimal characters, all uppercase

    if len(guid_id) != 32 :
        return jsonify({"Error":"Invalid GUID"})
    
    creation_time = str(int(time.time()))
    expiration_time = request.json.get('expiry')
    if expiration_time != None:
        if not (int(expiration_time) >= (int(time.time()) + (30 * 24 * 60 * 60))):
            return jsonify({"error":"invalid expiration time"})
    elif expiration_time == None:
        expiration_time = str(int(time.time()) + (30 * 24 * 60 * 60)) # 30 days expiration time if validity is not provided 
    user = request.json.get('user')
    sql = "insert into guid_details values ('" + guid_id + "','" + user + "','" + creation_time + "','" + expiration_time + "')"
    cur.execute(sql)
    conn.commit()
    
    # sending the recently created guid into cache
    
    cache.set(guid_id, user, 100000) # ~1.15 day
   
    return jsonify(
        {'GUID': guid_id, 'creation_time': creation_time, 'expiration_time': expiration_time, 'user': user})


# GET /guids/<guid_id>
# The GET endpoint returns the GUID details from Redis cache if it exists, otherwise, 
# it fetches the GUID details from the MySQL database and returns it.
# The function also checks if the GUID has expired, upon fetching the GUID details from SQL Database.
@app.route('/guids/<guid_id>', methods=['GET'])
def get_guid(guid_id):
    
    # checking if GUID is present in cache
    if cache.get(guid_id):
        
        read_from_cache = cache.get(guid_id).decode("utf-8")
       
        print(cache.ttl(read_from_cache))
        return jsonify({"cache data": {"guid": guid_id, "user" : read_from_cache}})


    # reading from database, if not present in cache

    else:
        sql = "select user, expiry_date, guid from guid_details where guid ='"+guid_id+"'"
        currenttime = int(time.time())
        flag = cur.execute(sql)
        if flag == 1 :
            result = cur.fetchall()
            for iterator in result:
                if currenttime < int(iterator[1]):
                    return jsonify({'guid':guid_id,'Expiry':iterator[1],'user':iterator[0]}) # returning the guid, user and expiry date data
                else:
                    return jsonify({'guid': guid_id, 'Expiry': 'Expired', 'user': iterator[0]}) # returning expired if expired
    
    return jsonify({'error':'GUID not found'})

# PUT /guids/<guid_id>
# This endpoint updates the expiration time of the specified GUID in the MySQL database and updates the Redis cache accordingly.
# If the GUID does not exist in the database, it returns an error message.
@app.route('/guids/<guid_id>', methods=['PUT'])
def update_guid(guid_id):
    if len(guid_id) != 32 :
        return jsonify({"Error":"Invalid GUID"})
    
    expiration_time = request.json.get('expiry')
    if not (int(expiration_time) >= (int(time.time()) + (30 * 24 * 60 * 60))):
        return jsonify({"error":"invalid expiration time"})
    
    check_sql = "select user from guid_details where guid ='"+guid_id+"'"
    count = cur.execute(check_sql)
    print(count)
    if count:
        for iterator in cur.fetchall():
            user = iterator[0]
        update_sql = "update guid_details set expiry_date = '"+expiration_time+"' where guid='"+guid_id+"'"
        result = cur.execute(update_sql)
        if result:

            mydict = {"guid_id" : guid_id, "user" : user, "expiry_date": expiration_time}
            cache.hset("key", mapping = mydict)
            conn.commit()

            return jsonify(
                {'GUID': guid_id, 'expiration_time': expiration_time, 'user':user})
    else:
        return jsonify({"Error":"No GUID found"})


# DELETE /guids/<guid_id>
# Deletes the records where GUID in the URL endpoint meets condition.
@app.route('/guids/<guid_id>', methods=['DELETE'])
def delete_guid(guid_id):
    sql = "delete from guid_details where guid = '"+guid_id+"'"
    result = cur.execute(sql)
    if result ==1:
        conn.commit()
        return jsonify({'Message':'Deleted guid:'+guid_id+''})
    else:
        return jsonify({'error':'Could not delete'})


# DELETE /guids
# Deletes all the records which are expired, upon running, and displays the records which are going to be deleted.
@app.route('/guids', methods=['DELETE'])
def delete_guid_expired():
    timestamp = str(int(time.time()))
    sql2 = "select * from guid_details where expiry_date < '" + timestamp + "'"
    result2 = cur.execute(sql2)
    arr = []

    if result2 != 0:
        for iterator in cur.fetchall():
            mydict = {}
            guid = iterator[0]
            user = iterator[1]
            start_date = iterator[2]
            expiry_date = iterator[3]

            mydict = { "guid" : guid, "user" : user, "start_date": start_date, "expiry_date": expiry_date}
            arr.append(mydict)
        
        sql = "delete from guid_details where expiry_date < '" + timestamp + "'"
        result = cur.execute(sql)

        conn.commit()

        if result != 0:
            return jsonify({"deleted_items" : arr})
        else:
            return jsonify({"Message":"no items found"})

if __name__ == '__main__':
    app.run(debug=True)