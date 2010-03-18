# encoding:utf8
""" Sample Cassandra Client

Created by Chris Goffinet on 2009-08-26. """ 

from thrift import Thrift 
from thrift.transport import TTransport 
from thrift.transport import TSocket 
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated 
from cassandra import Cassandra 
from cassandra.ttypes import * 
import time, pprint, binascii
import uuid

def main():
    socket = TSocket.TSocket("cass01", 9160) 
    transport = TTransport.TBufferedTransport(socket) 
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport) 
    client = Cassandra.Client(protocol)
    pp = pprint.PrettyPrinter(indent = 2) 
    keyspace = "Keyspace1" 
    x = uuid.uuid1()
    string = str(x)
    string_u =  string[0:8]+string[9:13]+string[14:18]+string[19:23]+string[24:36]
    hex_str = string_u
    binary=binascii.unhexlify(hex_str)
    column_path = ColumnPath(column_family="StandardByUUID1",column=binary) 
    key = "1" 
    value = " foobar@example.com " 
    timestamp = time.time() 
    try:
        transport.open()
        """ Insert the data into Keyspace 1 """
        client.insert(keyspace, key, column_path, value, timestamp, ConsistencyLevel.ZERO)
        """" Query for data """ 
        column_parent = ColumnParent(column_family="StandardByUUID1") 
        slice_range = SliceRange(start="", finish="") 
        predicate = SlicePredicate(slice_range=slice_range) 
        result = client.get_slice(keyspace, key, column_parent, predicate, ConsistencyLevel.ONE)
        pp.pprint(result)
    except Thrift.TException, tx:
        print 'Thrift: %s' % tx.message
	'''print 'Binary' % binary'''
    finally:
        transport.close()

if __name__ == '__main__':
  main()
