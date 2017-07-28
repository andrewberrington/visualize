import ujson
the_dict=dict()
the_dict['level1']=dict()
the_dict['level1']['level2']=[1,2,3,4]
with open('test.json','w') as outjson:
    ujson.dump(the_dict,outjson,indent=4)
    
