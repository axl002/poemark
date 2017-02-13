import time
from tornado import websocket, web, ioloop
import rethinkdb as r
from tornado import ioloop, gen
from tornado.concurrent import Future, chain_future
import json

store = []
cl = []

class IndexHandler(web.RequestHandler):
    def get(self):
        self.render("sock2.html")

class SocketHandler(websocket.WebSocketHandler):
    def check_origin(self, origin):
        return True

    def open(self):
        if self not in cl:
            cl.append(self)

    def on_close(self):
        if self in cl:
            cl.remove(self)

class ApiHandler(web.RequestHandler):

#    @web.asynchronous
    @gen.coroutine
    def get(self, *args):
        self.finish()
        id = self.get_argument("id")
        value = self.get_argument("value")
        #value = 400
        #print(self.get_body_argument("balkanbox", default=None, strip=False))
        #subItem = "flask"
        print(value)
        #print("set loop")
        r.set_loop_type("tornado")
        print("connect")
        conn = r.connect('35.166.62.31',28015, db='poeapi')
        #print(conn)
        connection = yield conn
        print("getting cursor")
        cursor = yield r.table('itemList').filter(lambda doc:doc['itemName'].match("(?i)" + value)).filter((r.row["price"] < 999) & (r.row["price"] > 0)).changes().run(connection)
        #cursor = yield r.table('itemList').filter((r.row["price"] < 999) & (r.row["price"] > 0)).changes().run(connection)
        print("prepare to loop")
        while (yield cursor.fetch_next()):
                        item = yield cursor.next()
                        item= item["new_val"]
                        #print(item)
                        #time.sleep(2)
                        tempSTD = 0
                        avg = 0
                        try:
                                tempItem = yield r.table('lookUp').get(item['itemName']).run(connection)
                                tempSTD = tempItem["STD"]
                                avg = tempItem["avgPrice"]
                        except:
                                print('error getting price')
                        if (item["price"] <= avg - tempSTD/2):
                                 #print(item)
                                 data = {"id": id, "value" : json.dumps(item)}
                                 data = json.dumps(data)
                                 for c in cl:
                                     c.write_message(data)


    @web.asynchronous
    def post(self):
        pass

#class FormHandler(web.RequestHandler):

 #   def post():
#       store["itemName"] = self.get_body_argument("jugoslavia", default=None, strip=False)
#       print(store["itemName"])

app = web.Application([
    (r'/', IndexHandler),
    (r'/ws', SocketHandler),
    (r'/api', ApiHandler),
])

if __name__ == '__main__':
    app.listen(8888)
ioloop.IOLoop.instance().start()
