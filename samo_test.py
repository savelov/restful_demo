import sys
import os
import socket
from flask import Flask
from flask_restplus import Api, Resource, fields
from threading import Lock
from tenacity import *
import json
import pyodbc
import logging

# curl -X GET http://localhost:5000/api/KM59268
# curl -X POST "http://localhost:5000/api/" -H  "accept: application/json" -H  "Content-Type: application/json" -d '@IPM.json'


sys.path.append(os.path.dirname(__file__))
from savetomssql import save_reservation, save_payment, save_payments

application = Flask(__name__)

class MyConfig(object):
    RESTPLUS_JSON = {'cls': application.json_encoder, 'ensure_ascii' : False}

#use Flask JSON encoder instead of standard python to convert dates and decimals
application.config.from_object(MyConfig)

api = Api(application, version='1.0', title='Reservation API',
    description='A simple Reservation API',
)

ns = api.namespace('api', description='Reservation operations')

reservation = api.model('Reservation', {
'id' : fields.String(required=True),
'crmid' : fields.String(required=True),
'owner' : fields.Raw(required=True),
'status' : fields.Raw(required=True),
'manager' : fields.Raw(required=True),
'curstomer' : fields.Raw(required=True),
'agent' : fields.Raw(required=False),
'partner' : fields.Raw(required=False),
'tourists' : fields.Raw(required=True),
'cruises' : fields.Raw(required=True),
'services' : fields.Raw(required=False),
'payments' : fields.Raw(required=False),
'traffic' : fields.Raw(required=False),
'created' : fields.DateTime(required=True),
})

payment = api.model('Payment', {
'id' : fields.String(required=True),
'order' : fields.Raw(required=True),
'type' : fields.Raw(required=True),
'status' : fields.Raw(required=True),
'deposit' : fields.Raw(required=True),
'items' : fields.Raw(required=True),
'created' : fields.DateTime(required=True),
})


returnresult = api.model('Result', {
'crmid' : fields.String,
'error': fields.Raw(required=False),
})


paymentsresult = api.model('PaymentsResult', {
'guid' : fields.String,
'error': fields.Raw(required=False),
})

# Implement singleton to avoid global objects
class ConnectionManager(object):    
    __instance = None
    __connection = None
    __lock = Lock()

    def __new__(cls):
        if ConnectionManager.__instance is None:
            ConnectionManager.__instance = object.__new__(cls)        
        return ConnectionManager.__instance       
    
    def __getConnection(self):
        if (self.__connection == None):
            application_name = ";application={0}".format(socket.gethostname())  
            self.__connection = pyodbc.connect('DSN=UAT;DATABASE=samocopy;UID=samo;PWD=samo' + application_name)
        
        return self.__connection

    def __removeConnection(self):
        self.__connection = None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10), retry=retry_if_exception_type(pyodbc.OperationalError), after=after_log(application.logger, logging.DEBUG))
    def executeQueryJSON(self, verb, table, payload=None):
        result = {'crmid': '0', 'error': {'code':0, 'description' : None }}  
        try:
            conn = self.__getConnection()

            cursor = conn.cursor()
            
            if verb == "select" :
                cursor.execute(f"{verb} * from {table} where number=?", payload)
                row = cursor.fetchone()

                if row:
                    result = dict(zip([column[0] for column in cursor.description], row))
                else:
                    result = {}
            else:
                if table == "paymentupdate" :
                    result = save_payment(payload, cursor)
                elif table == "paymentsupdate" :
                    result = save_payments(payload, cursor)
                elif table == "reservationupdate":
                    result = save_reservation(payload, cursor)
                else:
                    result = {}

            cursor.commit()    
        except pyodbc.OperationalError as e:            
            application.logger.error(f"{e.args[1]}")
            if e.args[0] == "08S01":
                # If there is a "Communication Link Failure" error, 
                # then connection must be removed
                # as it will be in an invalid state
                self.__removeConnection() 
                raise                        
                         
        return result

class Queryable(Resource):
    def executeQueryJson(self, verb, payload=None):
        result = {}  
        entity = type(self).__name__.lower()
        result = ConnectionManager().executeQueryJSON(verb, entity, payload)
        return result



@ns.route('/reservation_uat/')
class ReservationUpdate(Queryable):
    '''Updates or creates new reservation'''

    @ns.doc('create_reservation')
    @ns.expect(reservation)
    @ns.marshal_with(returnresult, code=201)
    def post(self):
        '''Create a new reservation'''
        result = self.executeQueryJson("put", api.payload)
        return result, 201


@ns.route('/payment_uat/')
class PaymentUpdate(Queryable):
    '''Updates or creates new payment'''

    @ns.doc('create_payment')
    @ns.expect(payment)
    @ns.marshal_with(paymentsresult, code=201)
    def post(self):
        '''Create a new payment'''
        result = self.executeQueryJson("put", api.payload)
        return result, 201

@ns.route('/payments_uat/')
class PaymentsUpdate(Queryable):
    '''Updates or creates new payment'''

    @ns.doc('create_payments')
    @ns.expect(payment)
    @ns.marshal_with(paymentsresult, code=201)
    def post(self):
        '''Create a new payment'''
        result = self.executeQueryJson("put", api.payload)
        return result, 201

@ns.route('/reservation_uat/<string:id>')
@ns.response(404, 'Reservation not found')
@ns.param('id', 'The task identifier')
class Reservation(Queryable):
    '''Show a single reservation'''
    @ns.doc('get_reservation')
    def get(self, id):
        '''Fetch a reservation by internal id'''
        return self.executeQueryJson("select", id)


if __name__ == '__main__':
    application.run(debug=True)