from pyexcel_ods3 import get_data
import pyodbc

class Loader:
  """
  This is a class from which one can load data from an SQL server.
  """

  def __init__(self, connection_string):
      """
      This is the initialization file, and it requires the connection_string.

      :param connection_string:
      :type connection_string: str
      :return:
      """

      self.connection = pyodbc.connect(connection_string)

  def UpdateRecipientGUIDs(self, mapping):
      """
      This function uploads to server

      :param query:
      :type query: str
      """

      cursor=self.connection.cursor()

      for i in range (len(mapping)) :
           if not mapping[i] : continue
           if mapping[i][3]!='#N/A' and mapping[i][3]:
               cc_guid = mapping[i][1]
               samo_id = mapping[i][3]
               selector=f'update dbo.[Recipient] set guid=\'{cc_guid}\' where id={samo_id}'
               cursor.execute (selector)
               if (cursor.rowcount != 1) :
                   print('error updating - ',cc_guid,mapping[i][2],samo_id)
               else :
                   print (cc_guid,mapping[i][2],samo_id)
      cursor.commit()


tester = Loader('DSN=UAT;DATABASE=samocopy;UID=samo;PWD=samo')

data = get_data("claims_cru.ods")
mapping = data["agency_fromcc"]
tester.UpdateRecipientGUIDs(mapping)

