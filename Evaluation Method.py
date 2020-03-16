from haversine import haversine
import mongoClient as mongo
from datasketch import MinHash, MinHashLSH

LHDB=mongo.mongoConnect("StreamAPI-GEO")

m1 = MinHash(num_perm=128)

#Create lsh index and load database 
data_dict = {} 
lsh1 = MinHashLSH(threshold=0.55, num_perm=128) 
insertedKeys = list()
geolocated_list = list()
counter = 0;
upper_limit = (LHDB.db_collection.count()/2)
for jsonFile in LHDB.db_collection.find():
    #Split the text bases on spaces
    splitted_word = jsonFile["text"].split(' ')
    
    if(jsonFile["id_str"] not in insertedKeys): 
        #Create an item combined by the words of the splitted text
        for tokenized in splitted_word:
            m1.update(tokenized.encode('utf8'))
        lsh1.insert(str(jsonFile["id_str"]),m1)
        #Update the keys
        insertedKeys.append(jsonFile["id_str"])   
    #Index fileds in dictionary    
    lot = jsonFile['place']['bounding_box']['coordinates'][0][0][0]
    lat = jsonFile['place']['bounding_box']['coordinates'][0][0][1]
    data = {jsonFile["id_str"] : [lot, lat] }
    data_dict.update(data)
    
    if counter <= upper_limit:
        counter = counter + 1
        geolocated_list.append(jsonFile["id_str"])
        
m1 = MinHash(num_perm=128)
assigned_lot=0;assigned_lat=0;
start=[];end=[];
haversine_distances = list()
#json file is being splitted from the collection
for jsonFile in LHDB.db_collection.find():
    splitted_word = jsonFile["text"].split(' ')
    for tokenized in splitted_word:
        m1.update(tokenized.encode('utf8'))
    result1 = lsh1.query(m1) 
#assigning long and lat values in the non geo items      
    for item in result1:
        if(item not in geolocated_list):
            assigned_lot = data_dict[jsonFile["id_str"]][0]
            assigned_lat = data_dict[jsonFile["id_str"]][1]
            lot = data_dict[item][0]
            lat = data_dict[item][1]
            start = (lat,lot)
            end = (assigned_lat,assigned_lot)
            distance = haversine(start, end)
            haversine_distances.append(distance)
        lsh1.remove(item)
        
print(haversine_distances)