# -*- coding: utf-8 -*-
"""
Редактор Spyder

Это временный скриптовый файл.
"""
import pyodbc
import uuid
import json
import sys
from datetime import datetime, timedelta
import traceback
import math

## added properties
## recipient- property+ cc_id ( =cruises.company.id)
## recipient - for managers - guid are taken from manager.id , handle errors
## TODO - complete countrymap, remove currencymap
## TODO - map legalgroupid /samo instance based on owner.id
## TODO - program - map from cruise.trace?
## TODO - reklama - map from traffic.source
## TOADD - agency INN as unique id to look in SAMO
## TOADD - tourist[0] = customer, need a flag
## TODO - Handle SQL exceptions to correctly close connections to prevent database hangs on update


# countrymap = {'RU':1, 'UA':169, 'BY':17, 'IL':57, 'AM':11, 'KZ':69, 'CZ':180, 'US':151, 'CY':75, 'IS':64 }
currencymap = {'EUR': 26, 'USD': 2, 'RUB': 21, 'RUR': 21}
# crucompanymap = {1:34475, 2:36167, 9:36168, 10:67141, 12:30041, 16:39161, 40:47663, 44:32334, 49:54475, 58:47748}
servicetypemap = {'VIS': 2, 'OTH': 5, 'IPM': 1, 'TRA': 3, 'EXC': 4, 'IPF': 1, 'DSC': 5, 'FLY': 5, 'HTL': 5, 'ERV': 1}
bank_eqv_map = {42: 0.0000, 95: 0.0125, 99: 0.0150, 192: 0.0000, 195: 0.0000, 200: 0.0000}

# constant for test/production version
# robotid = 90076
robotid = 92037
# properid_add_subclaim_KK = 85
properid_add_subclaim_KK=86
properid_vendor_KK = 84


class Loader:
    """
    This is a class from which one can load data from an SQL server.
    """

    def __init__(self, cursor):
        """
        This is the initialization file, and it requires the connection_string.

        :param connection_string:
        :type connection_string: str
        :return:
        """

        self.cursor = cursor
        self.number = None

    def GetRecipientID(self, order):
        """
        This function returns returns recipientid, either agent or human

        :param query: order
        :type query: str
  """
        cursor = self.cursor

        manager_guid = order["manager"]["id"]
        query = f'select id from recipient where guid=\'{manager_guid}\''
        cursor.execute(query)
        row = cursor.fetchone()
        if not row:
            return None
        managerid = row[0]

        if (not order["agent"]):
            lname = order["customer"]["lname"]
            fname = order["customer"]["fname"]
            mname = order["customer"]["mname"]
            if not mname: mname = ""
            phone = order["customer"]["phone"]
            email = order["customer"]["email"]
            query = '''
select id from dbo.[Recipient] where upper(rtrim([name]))=? and ltrim([phone])=? and [email]=?
'''
            values = ((lname + ' ' + fname + ' ' + mname).upper().rstrip(), phone, email)
            cursor.execute(query, values)
            row = cursor.fetchone()
            if (not row):
                selector = '''
insert into dbo.[Recipient]
([name], [recipienttypeid], [phone], [rtypeid], [legalgroupid], [metaphone], [managerid], [email])
values (?,?,?,?,?,?,?,?)
'''
                values = (
                    (lname + ' ' + fname + ' ' + mname).rstrip(), 8, phone, 1, 10,
                    (lname + fname + mname + phone).lower(),
                    managerid, email)
                cursor.execute(selector, values)

                cursor.execute("select IDENT_CURRENT('recipient')")
                row = cursor.fetchone()
            id = row[0]

        else:
            guid = order["agent"]["id"]
            query = f'select id from dbo.Recipient where guid=\'{guid}\''

            cursor.execute(query)
            row = cursor.fetchone()
            if (not row):
                name = order["agent"]["name"]
                query = 'insert into dbo.Recipient ([name], [rtypeid], [recipienttypeid], [metaphone],[managerid], [guid]) values (?,?,?,?,?,?)'
                values = (name, 2, 13, name.lower(), managerid, guid)
                cursor.execute(query, values)
                cursor.execute("select IDENT_CURRENT('recipient')")
                row = cursor.fetchone()
            id = row[0]

        return id

    def loadFromHumanSQL(self, lname, fname, bdate, passportno, sex, fake, nationality):
        """
        This function lreturns the tuple of ids.

        :param query: lname, fname, bdate, passportno
        :type query: str
        """
        ##  if reservation exists remove all tourist from reservation
        ##  delete from dbo.[people] where reservationid=reservationid
        ids = ()
        cursor = self.cursor

        if sex == 'M':
            sexid = 1
        elif sex == 'F':
            sexid = 2
        else:
            sexid = 0

        if (fake):
            query = 'select id from dbo.[human] where [p2name]=? and [born]=? and [sexid]=?'
            values = (lname + ' ' + fname, bdate, sexid)
            cursor.execute(query, values)
            rows = cursor.fetchall()

            for row in rows:
                ids = ids + (row[0],)

        else:
            query = '''
select id from dbo.[human] where [p2name]=? and [born]=? and [p2serie]=? 
and [p2number]=? and [sexid]=?
'''
            if nationality == 'RU':
                passport_sr = passportno[:2]
                passport_no = passportno[-7:]
            else:
                passport_sr = None
                passport_no = passportno
            values = (fname + ' ' + lname, bdate, passport_sr, passport_no, sexid)
            cursor.execute(query, values)
            rows = cursor.fetchall()

            for row in rows:
                ids = ids + (row[0],)

            values = (lname + ' ' + fname, bdate, passport_sr, passport_no, sexid)
            cursor.execute(query, values)
            rows = cursor.fetchall()

            for row in rows:
                ids = ids + (row[0],)

        return ids

    def SaveToHumanSQL(self, type, lname, fname, bdate, sex, nationality, passportno, passportexpiry, loyalty,
                       managerid=None):
        """
        This function uploads to server

        :param query:
        :type query: str
        """

        cursor = self.cursor

        selector = '''
insert into dbo.[Recipient]
([name], [rtypeid], [legalgroupid], [metaphone], [managerid])
values (?,?,?,?,?)
'''

        values = (lname + ' ' + fname, 1, 10, lname.lower() + ' ' + fname.lower(), managerid)
        cursor.execute(selector, values)

        cursor.execute("select IDENT_CURRENT('recipient')")
        row = cursor.fetchone()
        id = row[0]

        selector = '''
insert into dbo.[human]
([id], [sexid], [human], [born], [countryid], [p2name], [p2serie], [p2number], [p2date], [discontcard])
values (?,?,?,?,?,?,?,?,?,?)
'''

        if sex == 'M':
            sexid = 1
        elif sex == 'F':
            sexid = 2
        else:
            sexid = 0
        if type == 'A':
            human = 'ADL'
        elif type == 'C':
            human = 'CHD'
        else:
            human = ''

        if not passportno or passportexpiry.year < 1753:
            p2serie = None
            p2number = None
            p2date = None
        elif nationality == 'RU':
            p2serie = passportno[:2]
            p2number = passportno[-7:]
            p2date = passportexpiry
        else:
            p2serie = None
            p2number = passportno
            p2date = passportexpiry

        cursor.execute('select id from country where note like ?', nationality)
        row = cursor.fetchone()
        if (not row):
            countryid = None
        else:
            countryid = row[0]
        if bdate.year < 1753:
            bdate = None
        values = (id, sexid, human, bdate, countryid, lname + ' ' + fname, p2serie, p2number, p2date, loyalty)
        cursor.execute(selector, values)

        return id

    def SaveToReservationSQL(self, order, recipientid):
        """
        This function uploads to server

        :param query:
        :type query: str
        """

        # get counter, increase it, save counter, and use for reservation
        # managerid, recipientid,
        # insert reservation \'{reservation_guid}\
        # insert people
        # insert subclaim

        cursor = self.cursor

        reservation_guid = order["id"]
        km_number = order["crmid"]
        cursor.execute('select TOP 1 id from reservation where trash=0 and (guid=? or ndog=?) order by id desc',
                       (reservation_guid, km_number))

        row = cursor.fetchone()
        if (not row):
            reservation_new = 1
            reservationid = None
        else:
            reservation_new = 0
            reservationid = row[0]

        # check subclaims
        #           reservation_to_delete=row[0]
        #           query='select id from subclaim where claimid=?'
        #           cursor.execute(query,reservation_to_delete)
        #           rows=cursor.fetchall()
        #           if rows :
        #                query='select number from reservation where id=?'
        #                cursor.execute(query,reservation_to_delete)
        #                row = cursor.fetchone()
        #                self.number = row[0]

        # TODO - update existing reservation
        #                return 0

        #           query='update reservation set trash=1 where id=?'
        #           cursor.execute(query,reservation_to_delete)

        # create reservation if it is missing

        if reservation_new == 0:

            cursor.execute('select number from reservation where id=? and trash=0', reservationid)
            row = cursor.fetchone()
            number = row[0]
            self.number = number

        else:
            number = km_number
            self.number = number

        print('Dogovor number ', number, 'KM', km_number, 'reservationid ', reservationid)

        manager_guid = order["manager"]["id"]
        query = f'select id from recipient where guid=\'{manager_guid}\''
        cursor.execute(query)
        row = cursor.fetchone()
        humanid = row[0]

        guid = order["id"]
        currency = order["cruises"][0]["currency"]
        print(currency)

        date_created = datetime.fromisoformat(order["created"][:order["created"].find('.')])

        query = '''
insert into dbo.[reservation]
([number], [cdate], [recipientid], [humanid], [officeid], [legalid], [statusid],
 [pdate], [currencyid],[ndog],[guid])
values (?,?,?,?,?,?,?,?,?,?,?)
'''

        # TODO officeid by manager, legalid by owner, statusid?
        ##  if reservation is not exist create new, else update
        values = (
            km_number, date_created, recipientid, humanid, 29921, 136, 2, date_created, currencymap[currency],
            order["crmid"],
            guid)
        print(values)
        if (reservation_new == 1) and (km_number):
            cursor.execute(query, values)
            cursor.execute("select IDENT_CURRENT('reservation')")
            row = cursor.fetchone()
            id = row[0]
            cursor.execute('exec ChangesLog_AddNew ?,?,?,?,?,?,?,?,?,?,?,?,?', (
                'robot python', 1, 'reservation', id, km_number, 'reservation', id, str(id), None, None, '', None, ''))


        elif (reservation_new == 0) and (km_number):
            update_query = """ update dbo.[reservation] 
          set cdate = ?, recipientid=?, humanid = ?, officeid=?, legalid=?, statusid=?, pdate=?, currencyid=?, guid =?, ndog = ? where id=?"""
            cursor.execute(update_query, (
                date_created, recipientid, humanid, 29921, 136, 2, date_created, currencymap[currency], guid, km_number,
                reservationid))
            id = reservationid
        else:
            id = 0
        return id, reservation_new

    def SaveToPeopleSQL(self, reservationid, humanid):
        """
        This function uploads to server

        :param query:
        :type query: str
        """
        cursor = self.cursor

        selector = '''
          insert into dbo.[people] ([reservationid], [humanid])
          values (?,?)
          '''

        values = (reservationid, humanid)
        cursor.execute(selector, values)

    def SaveToSubClaimSQL(self, reservationid, order, number_of_turists, reservation_new):
        """
        This function uploads to server

        :param query:
        :type query: str
        """

        cursor = self.cursor

        date_begin = datetime.fromisoformat(order["cruises"][0]["date"])
        date_end = date_begin + timedelta(days=order["cruises"][0]["duration"])
        date_created = datetime.fromisoformat(order["created"][:order["created"].find('.')])

        # calc discont to use in subclaim cost
        cost_dsc = 0
        cost_dsc_ncf = 0
        for service in order["services"]:
            if (service["type"] == 'DSC'):
                for i in service["pricing"]:
                    cost_dsc = cost_dsc + i["p"]
                    cost_dsc_ncf = cost_dsc_ncf + i["ncf"]

        cost_cru = 0
        cost_cru_net = 0
        cost_ncf = 0
        cost_tip = 0
        cost_obc = 0
        cost_client = 0
        if order["agent"]:
            client_commission = order["cruises"][0]["commission"] / 100
        else:
            client_commission = 0

        # iterate by tourist, main order
        for i in order["cruises"][0]["pricing"]:
            cost_cru = cost_cru + i["p"]
            cost_ncf = cost_ncf + i["ncf"]
            cost_tip = cost_tip + i["tip"]
            cost_obc = cost_obc + i["obc"]
            # client cost
            # cost_client = (cost_cru_ctlg) * (1 - client_commission) + cost_ncf + cost_tip + cost_dsc_ncf
            cost_client = cost_client + math.ceil((i["p"] - i["ncf"] - i["tip"]) * (1 - client_commission))

        # catalogue cost

        cost_cru_ctlg = cost_cru - cost_ncf - cost_tip

        cursor.execute(
            'select userpropvalue.value_3 from town inner join userpropvalue on town.id=userpropvalue.documentid and town.countryid=193 and userpropvalue.userpropid=? where name=?',
            (properid_vendor_KK, order["cruises"][0]["company"]["name"],))
        row = cursor.fetchone()
        legalid = row[0]
        cursor.execute(
            'select userpropvalue.value_5 from town inner join userpropvalue on town.id=userpropvalue.documentid and town.countryid=193 and userpropvalue.userpropid=? where name=?',
            (properid_add_subclaim_KK, order["cruises"][0]["company"]["name"],))
        row = cursor.fetchone()
        if not row:
            do_subclaim_KK = 0
        else:
            do_subclaim_KK = row[0]

        cursor.execute(
            'select userpropvalue.value_4 from userpropvalue where userpropvalue.userpropid=65 and userpropvalue.documentid= ?',
            (legalid))
        row = cursor.fetchone()
        if not row:
            percent = 0
        else:
            percent = row[0] / 100

        comission = round(percent * (cost_cru_ctlg + cost_dsc), 2)
        if cost_cru_ctlg == 0:
            comm_in_perc = 0
        else:
            comm_in_perc = comission * 100 / cost_cru_ctlg
        # cost_cru_net = (cost_cru_ctlg + cost_dsc)*(1-percent)+cost_ncf+cost_tip
        cost_cru_net = cost_cru + cost_dsc - comission
        # client cost
        # cost_client = (cost_cru_ctlg )*(1-client_commission)+cost_ncf+cost_tip+cost_dsc_ncf
        cost_client = cost_client + cost_ncf + cost_tip + cost_dsc_ncf

        print(cost_cru, cost_cru_net, cost_cru_ctlg, cost_dsc, cost_ncf, cost_tip, percent, comission)

        subclaim_number = 1

        selector = '''
insert into dbo.[Subclaim]
  ([reservationid], [countryid], [legalid], [docdate], [statusid],[pdate],[datebeg],
  [dateend], [ccost], [cost], [price],[net],[commission],[commissionpercent],[currencyid], [extcode],
  [number],[autocalcnet])

values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

        # todo check add service in main KK subclaim, delete it
        values = (reservationid, 193, legalid, date_created, 2, date_created, date_begin, date_end,
                  cost_cru_ctlg, cost_client, cost_cru, cost_cru_net, comission, comm_in_perc,
                  currencymap[order["cruises"][0]["currency"]], order["cruises"][0]["booking"], subclaim_number, 0)

        query_update = '''
update dbo.[Subclaim]
  set reservationid=?, countryid=?, legalid=?, docdate=?, statusid=?, pdate=?, datebeg=?,
  dateend=?, ccost=?, cost=?, price=?, net=?, commission=?, commissionpercent=?, currencyid=?, extcode=?,
  number=?, autocalcnet=? where id=? 

'''
        # check KK subclaim number 1
        cursor.execute('select id from subclaim where reservationid=? and number=?', (reservationid, 1))
        row = cursor.fetchone()

        if (not row):  # reservation_new == 1
            cursor.execute(selector, values)
            cursor.execute("select IDENT_CURRENT('subclaim')")
            row = cursor.fetchone()
            subclaimid = row[0]
            subclaim_cost = 0
        else:
            subclaimid = row[0]
            cursor.execute('select subclaim.price from subclaim where subclaim.reservationid=? and subclaim.number=?',
                           (reservationid, 1))
            row = cursor.fetchone()
            subclaim_cost = row[0]
            if subclaim_cost != cost_cru:
                cursor.execute(query_update,
                               (reservationid, 193, legalid, date_created, 2, date_created, date_begin, date_end,
                                cost_cru_ctlg, cost_client, cost_cru, cost_cru_net, comission, comm_in_perc,
                                currencymap[order["cruises"][0]["currency"]], order["cruises"][0]["booking"],
                                subclaim_number, 0, subclaimid))
                cursor.execute('delete from surcharge where subclaimid=?', subclaimid)
                cursor.execute('delete from horder where subclaimid=?', subclaimid)

        # add surcharges
        if (reservation_new == 1 or subclaim_cost != cost_cru):
            if cost_ncf != 0:
                cursor.execute(
                    "insert into surcharge(subclaimid,cost,commission,perman,orderid,note) values(?,?,?,?,?,?)",
                    subclaimid, cost_ncf, 0, 0, 0, 'ncf')
            if cost_tip != 0:
                cursor.execute(
                    "insert into surcharge(subclaimid,cost,commission,perman,orderid,note) values(?,?,?,?,?,?)",
                    subclaimid, cost_tip, 0, 0, 0, 'tip')
            if cost_obc != 0:
                cursor.execute(
                    "insert into surcharge(subclaimid,cost,commission,perman,orderid,note) values(?,?,?,?,?,?)",
                    subclaimid, cost_obc, 0, 0, 0, 'obc')
            if cost_dsc_ncf != 0:
                cursor.execute(
                    "insert into surcharge(subclaimid,cost,commission,perman,orderid,note) values(?,?,?,?,?,?)",
                    subclaimid, cost_dsc, 0, 0, 0, 'dsc')

            #  try to find ship (hotel.id) by name
            cursor.execute(
                'select hotel.id from hotel inner join town on town.id=hotel.townid and town.countryid=193 where town.name=? and hotel.name=?',
                (order["cruises"][0]["company"]["name"], order["cruises"][0]["ship"]["name"]))
            row = cursor.fetchone()
            if (not row):
                cursor.execute('select id from town where name=?', order["cruises"][0]["company"]["name"])
                row = cursor.fetchone()
                townid = row[0]
                cursor.execute('insert into hotel(name, townid) values(?,?) ',
                               (order["cruises"][0]["ship"]["name"], townid))
                cursor.execute("select IDENT_CURRENT('hotel')")
                row = cursor.fetchone()
            hotelid = row[0]
            #  try to find cabin (room.id) by name
            cursor.execute('select id from room where name = ?', (order["cruises"][0]["room"]["category"]))
            row = cursor.fetchone()
            if (not row):
                cursor.execute('insert into room(name) values(?)', (order["cruises"][0]["room"]["category"]))
                cursor.execute("select IDENT_CURRENT('room')")
                row = cursor.fetchone()
            roomid = row[0]

            cursor.execute('select id from meal where name = ?', (order["cruises"][0]["room"]["dining"]))
            row = cursor.fetchone()
            if (not row):
                cursor.execute('insert into meal(name) values (?)', (order["cruises"][0]["room"]["dining"]))
                cursor.execute("select IDENT_CURRENT('meal')")
                row = cursor.fetchone()
            # mealid = row[0]
            mealid = 6  # FB for all cases
            cursor.execute(
                'insert into horder(subclaimid, hotelid, roomid, pcount, rcount, htplaceid, datebeg, dateend, mealid) values(?,?,?,?,?,?,?,?,?)',
                (subclaimid, hotelid, roomid, number_of_turists, 1, 906, date_begin, date_end, mealid))
        # else: continue

        if (order["partner"]):
            partner_guid = order["partner"]["id"]
            print(order["partner"]["id"])
            p_comm = order["cruises"][0]["commission"] / 100
            cursor.execute('select id from recipient where guid=?', (partner_guid))
            row = cursor.fetchone()
            partnerid = row[0]
            partner_commission_value = round(p_comm * cost_cru_ctlg)
            subclaim_number = subclaim_number + 1
            cursor.execute('select id from subclaim where reservationid=? and legalid=?', (reservationid, partnerid))
            row = cursor.fetchone()
            if (not row):
                cursor.execute(
                    'insert into subclaim(reservationid, countryid, legalid, docdate, statusid, pdate, datebeg, dateend, cost, price, net, commission, commissionpercent, currencyid, number, extcode) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (reservationid, 193, partnerid, date_created, 2, date_created, date_begin, date_end, 0, 0,
                     partner_commission_value, partner_commission_value, 0,
                     currencymap[order["cruises"][0]["currency"]], subclaim_number, ''))
            else:
                subclaimid = row[0]
                cursor.execute(
                    'update subclaim set reservationid=?,countryid=?, legalid=?, docdate=?, statusid=?, pdate=?, datebeg=?, dateend=?, cost=?, price=?, net=?, commission=?, commissionpercent=?, currencyid=?, number=?, extcode=? where id=?',
                    (reservationid, 193, partnerid, date_created, 2, date_created, date_begin, date_end, 0, 0,
                     partner_commission_value, partner_commission_value, 0,
                     currencymap[order["cruises"][0]["currency"]], subclaim_number, '', subclaimid)
                )

        if do_subclaim_KK:
            subclaim_number = subclaim_number + 1
            cursor.execute('select id from subclaim where reservationid=? and legalid=?', (reservationid, 36692))
            row = cursor.fetchone()
            if (not row):
                cursor.execute(
                    'insert into subclaim(reservationid, countryid, legalid, docdate, statusid, pdate, datebeg, dateend, ccost, cost, price, net, commission, commissionpercent, currencyid, number, extcode) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (reservationid, 193, 36692, date_created, 2, date_created, date_begin, date_end, cost_cru_net,
                     cost_cru_net, cost_cru_net, cost_cru_net, 0, 0,
                     currencymap[order["cruises"][0]["currency"]], subclaim_number, ''))
            elif (subclaim_cost != cost_cru):
                subclaimid = row[0]
                cursor.execute(
                    'update subclaim set reservationid=?, countryid=?, legalid=?, docdate=?, statusid=?, pdate=?, datebeg=?, dateend=?, ccost=?, cost=?, price=?, net=?, commission=?, commissionpercent=?, currencyid=?, number=?, extcode=? where id=?',
                    (reservationid, 193, 36692, date_created, 2, date_created, date_begin, date_end, cost_cru_net,
                     cost_cru_net, cost_cru_net, cost_cru_net, 0, 0,
                     currencymap[order["cruises"][0]["currency"]], subclaim_number, '', subclaimid))
            # else: continue

        for service in order["services"]:
            if (service["type"] == 'IPF'):
                service_type = 'ERV'
                service_name = 'Страховка от невыезда'
            elif (service["type"] == 'IPM'):
                service_type = 'ERV'
                service_name = 'страховка медицинская'
            else:
                service_type = service["type"]
                service_name = service["name"]

            print(service_type)

            if service_type == 'DSC':
                continue
            cursor.execute('select id from recipient where name = ?', (service_type))
            row = cursor.fetchone()
            legalid = row[0]
            subclaim_number = subclaim_number + 1
            if service["ldate"] != None:
                date_service_beg = datetime.fromisoformat(service["ldate"])
            else:
                date_service_beg = date_begin
            if service["hdate"] != None:
                date_service_end = datetime.fromisoformat(service["hdate"])
            else:
                date_service_end = date_end

            currency_local = currencymap[service["currency"]]
            currency_main = currencymap[order["cruises"][0]["currency"]]
            cursor.execute('select [dbo].udf_currate_get(?,?,?)', (currency_local, date_created, currency_main))
            row = cursor.fetchone()
            calc_rate = row[0]

            cost_srv = 0
            cost_srv_ncf = 0

            # additional services
            for i in service["pricing"]:
                cost_srv = cost_srv + i["p"]
                cost_srv_ncf = cost_srv_ncf + i["ncf"]
            # check if subclaim exist          s
            cursor.execute(
                'select subclaim.id from subclaim left join sorder on sorder.subclaimid =subclaim.id inner join service on sorder.serviceid=service.id where (service.name =?) and sorder.servicetypeid=? and subclaim.reservationid=?',
                (service_name, servicetypemap[service["type"]], reservationid))

            row = cursor.fetchone()
            if (not row):
                cursor.execute('select TOP 1 number from subclaim where reservationid=? order by number desc',
                               reservationid)
                row = cursor.fetchone()
                subclaim_number = row[0] + 1
                print('new subclaim_number', subclaim_number)
                cursor.execute(
                    'insert into subclaim(reservationid, countryid, legalid, docdate, statusid, pdate, datebeg, dateend, ccost,cost, price, net, commission, commissionpercent, currencyid, calcrate, number, extcode) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    reservationid, 193, legalid, date_created, 2, date_created, date_begin, date_end,
                    cost_srv - cost_srv_ncf, cost_srv, cost_srv, cost_srv, 0, 0,
                    currencymap[service["currency"]], calc_rate, subclaim_number, '')
                cursor.execute("select IDENT_CURRENT('subclaim')")
                row = cursor.fetchone()
                subclaimid = row[0]

                if cost_srv_ncf != 0:
                    cursor.execute(
                        "insert into surcharge(subclaimid,cost,commission,perman,orderid,note) values(?,?,?,?,?,?)",
                        subclaimid, cost_srv_ncf, 0, 0, 0, 'ncf')

                print(service)
                cursor.execute('select id from service where name= ?', (service["name"]))
                row = cursor.fetchone()
                if not row:
                    if service["type"] == 'IPM':
                        serviceid = 4844
                    elif service["type"] == 'IPF':
                        serviceid = 5599
                    else:
                        cursor.execute('insert into service(name,servicetypeid) values (?,?)', service["name"],
                                       servicetypemap[service["type"]])
                        cursor.execute("select IDENT_CURRENT('service')")
                        row = cursor.fetchone()
                        serviceid = row[0]
                else:
                    serviceid = row[0]
                print(serviceid)
                service_number_of_turists = len(service["pricing"])
                cursor.execute(
                    'insert into sorder(subclaimid,serviceid,servicetypeid,datebeg,dateend,pcount) values (?,?,?,?,?,?)',
                    subclaimid, serviceid, servicetypemap[service["type"]], date_service_beg, date_service_end,
                    service_number_of_turists)
            else:
                # TODO check if more than one subclaims exist
                subclaimid = row[0]
                print('subclaim exists', subclaimid)
                service_number_of_turists = len(service["pricing"])
                cursor.execute('update sorder set datebeg=?,dateend=?,pcount=? where subclaimid=?',
                               (date_service_beg, date_service_end, service_number_of_turists, subclaimid))
                cursor.execute(
                    'update subclaim set docdate=?, statusid=?, pdate=?, datebeg=?, dateend=?, ccost=?,cost=?, price=?, net=?, commission=?, commissionpercent=?, currencyid=?, calcrate=?, extcode=? where id=?',
                    (date_created, 2, date_created, date_begin, date_end, cost_srv - cost_srv_ncf, cost_srv, cost_srv,
                     cost_srv, 0, 0,
                     currencymap[service["currency"]], calc_rate, '', subclaimid))

                if cost_srv_ncf != 0:
                    cursor.execute(
                        'update surcharge set cost=?,commission=?,perman=?,orderid=?,note=? where subclaimid=?',
                        (cost_srv_ncf, 0, 0, 0, 'ncf', subclaimid))
                print(service)

        # calc total sum for reservation
        cursor.execute('exec RecalcReservation ?,1', reservationid)

    def SaveToPaymentSQL(self, reservationid, clientid, order):

        cursor = self.cursor

        # create payments
        ## id=95,  0.0125 tincoff, 0.015 vtb, id=42 0.000 psb
        
        cursor.execute('select currencyid from reservation where id=?', reservationid)
        row = cursor.fetchone()
        claim_currency = row[0]
        
        
        cursor.execute('select deb_acc, crd_acc, bank_pp, bank_name from _bank_acquire where id_type_cc=?', order["type"]["id"])
        row = cursor.fetchone()
        if (not row): 
            return 'нет описания типа платежа {0} в таблице _bank_aqcuire'.format(order["type"]["id"])
        else:
            deb_acc = row[0]
            crd_acc = row[1] 
            bank_pp = float(row[2])
            note = row[3]
            print(deb_acc,crd_acc,bank_pp,note)
        
        stop_no_sert = 0
        
        if (order["type"]["id"]==192 or order["type"]["id"]==193):
            cursor.execute('select id from presentcard where number=?', order["deposit"]["id"])
            row = cursor.fetchone()
            if (not row): 
                return 'no certificate with number {0}'.format(order["deposit"]["id"] ) 
        
        if (order["status"]["id"] == 200):
            cursor.execute('select id from [transaction] where guid=?', order["id"])
            row = cursor.fetchone()
            if (not row):
                sum_multi = 0
                sum_multi2 = 0
                sum_cur_eqv = 0
                if (order["type"]["id"] in (190,199)):
                        weight = -1
                        rcp2 = 136
                        rcp1 = clientid
                else:
                        weight = 1
                        rcp2 = clientid
                        rcp1 = 136
                acc_id_from = deb_acc
                acc_id_to = crd_acc
                pdate = order["created"]
                cursor.execute(
                            'insert into [transaction](guid,number,datetime,humanid,officeid,cash,note,account1id,recipient1id,value1,currency1id,account2id,recipient2id,value2,currency2id,purposeid,complete,tablename,documentid,documentvalue,weight) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            (order["id"], 11111, pdate, robotid, 29921, 4, note, acc_id_to, rcp1, sum_multi2, 21, acc_id_from,
                             rcp2, sum_multi, 21,
                             -100, 1, 'reservation', reservationid, 0, weight))
                cursor.execute("select IDENT_CURRENT('transaction')")
                row = cursor.fetchone()
                transactionid = row[0]
                              
                cursor.execute('select [transaction].id, subclaim.id from [transaction] inner join subclaim on subclaim.reservationid=[transaction].documentid and [transaction].tablename=? where [transaction].id=?', ('reservation', transactionid))
                val = cursor.fetchall()
                for i in val:
                   cursor.execute('insert into distribution (transactionid, subclaimid,value,rate,ratedirection,calcrate) values (?,?,?,?,?,?)', (i[0], i[1], 0, 1, 1, 1))
                
                for i in order["items"]:
                    curr1 = currencymap[i["currency"]]
                    if claim_currency != curr1 :
                        cursor.execute('select [dbo].udf_currate_get (?,?,?)', ( curr1, pdate, claim_currency))
                        row = cursor.fetchone()
                        rate1 = row[0]
                    else : rate1 = 1    
                    amount = abs(i["amount"])*rate1
                    sum_rub = abs(i["amount"] * i["rate"])
                    sum_cur_eqv = sum_cur_eqv + amount
                    sum_multi =sum_multi +sum_rub
                    print(sum_multi, sum_rub, ', ', amount, ',', rate1)
                sum_multi2 =sum_multi - round(sum_multi * bank_pp, 2)
                
                for i in order["items"]:
                    #curr1 = currencymap[i["currency"]]
                    cursor.execute('select TOP 1 distribution.id from distribution inner join subclaim on distribution.subclaimid=subclaim.id where transactionid=? and subclaim.currencyid=? and subclaim.legalid!=36692', (transactionid, currencymap[i["currency"]]))
                    row = cursor.fetchone()
                    if row:
                        distr_sub_id = row [0]
                        cursor.execute('update distribution set value=?, rate=?, calcrate=? where id=?',
                                       (abs(i["amount"])*i["rate"], i["rate"], 1/i["rate"], distr_sub_id))
                cursor.execute(
                            'update [transaction] set value1=?, value2=?, documentvalue=? where id=?',
                            (sum_multi2, sum_multi, sum_cur_eqv, transactionid))
                print('payment', transactionid , i)
            
               
                if (order["type"]["id"]==190):
                    print(order["type"]["id"], order["deposit"]["id"])
                    cursor.execute('select id from presentcard where number=?', order["deposit"]["id"])
                    row = cursor.fetchone()
                    if (not row):
                        cursor.execute('insert into presentcard(number,givendate,datebeg,cost,currencyid,recipientid,canceled,humanid,single,statusid) values (?,?,?,?,?,?,?,?,?,?)', (order["deposit"]["id"], order["created"], order["created"],sum_multi, 21, clientid, 0, robotid, 0, 1))
                        cursor.execute("select IDENT_CURRENT('presentcard')")
                        row = cursor.fetchone()
                        presentcardid = row[0]
                    else:
                        presentcardid = row[0]
                    print('presentcardid=', presentcardid)    
                    cursor.execute( 'insert into [transaction](guid,number,datetime,humanid,officeid,cash,note,account1id,recipient1id,value1,currency1id,account2id,recipient2id,value2,currency2id,purposeid,complete,tablename,documentid,documentvalue,weight) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            (order["id"], 11111, pdate, robotid, 29921, 4, 'создание депозита', 23, clientid, sum_multi2, 21, 8,
                             clientid, sum_multi, 21, None,
                             1, 'presentcard', presentcardid, sum_rub, weight))
                
                if (order["type"]["id"]==192 or order["type"]["id"]==193):
                    cursor.execute('select id from presentcard where number=?', order["deposit"]["id"])
                    row = cursor.fetchone()
                    if row: 
                        presentcardid=row[0]
                        cursor.execute('insert into presentcardtransaction(presentcardid,reservationid,transactionid) values (?,?,?)', (presentcardid,reservationid,transactionid))
        else:
            return 'статус платеж - не оплачен, не загружаем'            
                    
                    
            # cursor.execute('select id from subclaim where reservationid=? and number=?', reservationid,1)
                    # row=cursor.fetchone()
                    # if (not row): continue
                    # else:
                    #     subclaim_first=row[0]
                    #     print('subclaim_first',subclaim_first)
                    #     if (p["items"][0]["rate"]!=0) : calc_rate = 1/p["items"][0]["rate"]
                    #     else: calc_rate =0
                    #     #cursor.execute('exec Distribution_GetForSubEdit ?', transactionid)
                    #     #cursor.execute('insert into distribution(transactionid, subclaimid,value, rate, ratedirection,calcrate) values(?,?,?,?,?,?)', (transactionid, subclaim_first,sum_rub2,p["items"][0]["rate"],0,calc_rate))
                    #     #cursor.execute('update distribution set value=?, rate=?, ratedirection=?,calcrate=? where transactionid=? and subclaimid=?', (sum_rub2,p["items"][0]["rate"],0,calc_rate, transactionid, subclaim_first))

            
        return None

def save_payment(data, cursor):

    print('python version ' + sys.version)
    print('pyodbc version ' + pyodbc.version)

    tester = Loader(cursor)
    result={}

    try:
        guid=data["order"]["id"]
        cursor.execute('select id from reservation where guid=? or (ndog is not null and ndog=?)',
                       (guid, data["order"]["crmid"]))
        row = cursor.fetchone()
        if not row:
            result={'guid': data["id"], 
                      'error': 'заявка не найдена guid={0}, ndog={1}'.format(guid,data["order"]["crmid"])}

            #                cursor.rollback()
            print('no claim was found for guid=', guid,',ndog=',data["order"]["crmid"])
            print (result)
            #           return result
            return result
        else:
            reservationid = row[0]
            print ('loading reservation id=',reservationid,',guid=',guid)
            cursor.execute('select recipientid from reservation where id=?', reservationid)
            row = cursor.fetchone()
            recipientid = row[0]
            result={'guid': data["id"], 'error': tester.SaveToPaymentSQL(reservationid, recipientid, data)}

        # print(save_reservation(data, cursor))
        cursor.commit()

    except pyodbc.Error as err:
        cursor.rollback()
        print('handling exception ', traceback.format_exc())
        result = {'guid': data["id"], 'error': err.args[1]}

    except Exception as err:
        cursor.rollback()
        print ('handling exception ',traceback.format_exc())
        result = {'guid': data["id"], 'error': repr(err)}

    return result

def save_payments(data_all, cursor):

    print('python version ' + sys.version)
    print('pyodbc version ' + pyodbc.version)

    tester = Loader(cursor)
    result = []

    for data in data_all:
        guid=data["order"]["id"]
        cursor.execute('select id from reservation where guid=? or (ndog is not null and ndog=?)',
                       (guid, data["order"]["crmid"]))
        row = cursor.fetchone()
        if not row:
            result.append ( {'guid': data["id"], 
                      'error': 'заявка не найдена guid={0}, ndog={1}'.format(guid,data["order"]["crmid"])})
            print('no claim was found for guid=', guid,',ndog=',data["order"]["crmid"])
        else:
            reservationid = row[0]
            print ('loading reservation id=',reservationid,',guid=',guid)
            cursor.execute('select recipientid from reservation where id=?', reservationid)
            row = cursor.fetchone()
            recipientid = row[0]
            try : 
                result.append ({'guid': data["id"], 'error': tester.SaveToPaymentSQL(reservationid, recipientid, data)})
                cursor.commit()
            except Exception as err:
                cursor.rollback()
                print('handling exception ', traceback.format_exc())
                result.append ({'guid': data["id"], 'error': repr(err)})


    return result

def save_reservation(data, cursor):
    print('python version ' + sys.version)
    print('pyodbc version ' + pyodbc.version)

    try:

        # input validation
        if (data["agent"] and (
                data["cruises"][0]["commission"] == None or float(data["cruises"][0]["commission"]) <= 0)):
            error = {'crmid': 0,
                     'error': {'code': -1, 'description': 'не проставлена комиссия агента'}}
            return error

        if (data["partner"] and (
                data["cruises"][0]["commission"] == None or float(data["cruises"][0]["commission"]) <= 0)):
            error = {'crmid': 0,
                     'error': {'code': -2, 'description': 'не проставлена комиссия партнера'}}
            return error

        if (data["crmid"] == None):
            error = {'crmid': 0,
                     'error': {'code': -3, 'description': 'не присвоен номер договора КМххххх'}}
            return error

        for service in data["services"]:
            if (service and service["name"] and len(service["name"]) > 64):
                error = {'crmid': 0,
                         'error': {'code': -4, 'description': 'название услуги не может содержат более 64 символов'}}
                return error

        ncf_p = 0

        for i in data["cruises"][0]["pricing"]:
            if i["ncf"]:
                if i["ncf"] > 0: ncf_p = ncf_p + 1
        #    print(ncf_p)

        #    tip = tip + i["tip"]
        if (len(data["tourists"]) > ncf_p):
            error = {'crmid': 0,
                     'error': {'code': -5, 'description': 'не проставлены значения ncf'}}
            return error

        tester = Loader(cursor)
        # if agent guid is present, find out the recipient id
        recipientid = tester.GetRecipientID(data)
        if (recipientid == None):
            error = {'crmid': 0,
                     'error': {'code': -6, 'description': 'не найден manager'}}
            cursor.rollback()
            return error

        reservationid, reservation_new = tester.SaveToReservationSQL(data, recipientid)
        print(reservationid, '  ', reservation_new)

        if reservationid == 0:
            result = {'crmid': 0,
                      'error': {'code': -6, 'description': 'not created'}}
            cursor.rollback()
            return result

        if reservation_new == 0:
            cursor.execute('select number from reservation where id=?', reservationid)
            row = cursor.fetchone()
            number = row[0]
            print('dogovor', number, 'result : Reservation already present')
            # check if tourist link exists

            cursor.execute('delete from opeople where subclaimid in (select id from subclaim where reservationid=?)',
                           reservationid)
            cursor.execute('delete from people where reservationid=?', reservationid)
            # cursor.execute('delete from sorder where subclaimid in (select id from subclaim where reservationid=? and number!=1)',
            #              reservationid)

            # cursor.execute('delete from surcharge where subclaimid in (select id from subclaim where reservationid=? and number!=1)',
            #             reservationid)

        for i in data["tourists"]:
            ids = tester.loadFromHumanSQL(i["lname"], i["fname"], datetime.fromisoformat(i["bdate"]),
                                          i["passport"]["id"], i["sex"], i["passport"]["fake"], i["nationality"])
            print('tourist -', ids)
            if not ids:
                id = tester.SaveToHumanSQL(i["type"], i["lname"], i["fname"], datetime.fromisoformat(i["bdate"]),
                                           i["sex"], i["nationality"],
                                           i["passport"]["id"], datetime.fromisoformat(i["passport"]["edate"]),
                                           i["loyalty"])
                tester.SaveToPeopleSQL(reservationid, id)
            else:
                tester.SaveToPeopleSQL(reservationid, ids[0])

            print(i)

        tester.SaveToSubClaimSQL(reservationid, data, len(data["tourists"]), reservation_new)

        # tester.SaveToPaymentSQL(reservationid, recipientid, data)
        cursor.commit()
        result = {'crmid': tester.number}

    except pyodbc.Error as err:
        cursor.rollback()
        print('handling exception ', traceback.format_exc())
        result = {'crmid': tester.number,
                  'error': {'code': err.args[0], 'description': err.args[1]}}

    except Exception as err:
        cursor.rollback()
        print ('handling exception ',traceback.format_exc())
        result = {'crmid': tester.number,
                  'error': { 'code' : 0, 'description': repr(err) }}

    return result


if __name__ == '__main__':
    with open("invalidship.json", "r", encoding="utf8") as read_file:
        data = json.load(read_file)
        cursor = pyodbc.connect(
            'DSN=UAT;DATABASE=samocopy;UID=samo;PWD=samo').cursor()
        print(save_reservation(data, cursor))
        cursor.close()

    with open("pay_dep4.json", "r", encoding="utf8") as read_file:
        data = json.load(read_file)
        cursor = pyodbc.connect(
            'DSN=UAT;DATABASE=samocopy;UID=samo;PWD=samo').cursor()
        print(save_payments(data, cursor))
        cursor.close()


