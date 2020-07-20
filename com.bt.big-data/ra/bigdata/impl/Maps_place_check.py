# import googlemaps
# import redis
# from datetime import datetime
# from bs4 import BeautifulSoup
#
# redis_connection = redis.Redis(host='localhost', db=0)
# api_key = "AIzaSyAnHMpzguNcs0QMWLCP10cj3__KeNQNAp4"
#
# gmaps = googlemaps.Client(key=api_key)
# now = datetime.now()
# geocode_result = gmaps.geocode("bt eserv rmz ecoworld")
# geocode_result2 = gmaps.geocode("koramangala 4th block")
# # print geocode_result
# redis_connection.set("Office_Address", geocode_result[0]['formatted_address'])
# # print "koramangala",geocode_result2
# # print "BT",geocode_result
# now = datetime.now()
# directions_result = gmaps.directions(redis_connection.get("Office_Address"), "koramangala 4th block", mode="driving",
#                                      alternatives=True,
#                                      departure_time=now)
# # print directions_result
# directions_results = gmaps.directions(redis_connection.get("Office_Address"), "89,16th main road", mode="driving",
#                                       alternatives=True,
#                                       departure_time=now)
#
# for i in directions_results:
#     for k in i:
#         if 'legs' in k:
#             for j in i[k]:
#                 for leng in j:
#                     if 'steps' in leng:
#                         for l in j[leng]:
#                             for che in l:
#                                 if 'html_instructions' in che:
#                                     cleante = BeautifulSoup(l[che], "html.parser").text
#                                     print cleante
# cleantext = ""
# for i in directions_result:
#     for k in i:
#         if 'legs' in k:
#             for j in i[k]:
#                 for leng in j:
#                     if 'steps' in leng:
#                         for l in j[leng]:
#                             for che in l:
#                                 if 'html_instructions' in che:
#                                     cleante = BeautifulSoup(l[che], "html.parser").text
#                                     if cleantext is not "":
#                                         cleantext = cleantext + " | " + cleante
#                                     else:
#                                         cleantext = cleante
#
# redis_connection.sadd("Office2Home", cleantext)
# redis_connection.hset("BtCabRouteInfo", "Office2Home", cleantext)
# # place = gmaps.places("Restaurants", location={u'lat': 12.9245184, u'lng': 77.6883759})
# # place2 = gmaps.places("Restaurants", location={u'lat': 12.9314583, u'lng': 77.6299858})
# # print place
# # for i in place2:
# #     if 'results' in i:
# #         x = place2[i]
# #         for val in x:
# #             for k in val:
# #                 if 'name' in k:
# #                     redis_connection.sadd(k + "Koramangala", val[k])
# #
# # for i in place:
# #     if 'results' in i:
# #         x = place[i]
# #         for val in x:
# #             for k in val:
# #                 if 'name' in k:
# #                     redis_connection.sadd(k + "BT", val[k])
# #
# #                 testing_val = "Kairali Restaurant"
# #                 val1 = redis_connection.smembers("nameBT")
# #                 for y in val1:
# #                     if testing_val in y:
# #                         print k,val[k]
