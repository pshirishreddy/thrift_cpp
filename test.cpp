

#include <string.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
 
#include "Cassandra.h"
 
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
 
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;
using namespace boost;
 
static string host("192.168.0.6");
static int port= 9160;
 
int main()
{
  shared_ptr<TTransport> socket(new TSocket(host, port));
  shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  CassandraClient client(protocol);
 
  try
  {
    transport->open();
 
    ColumnPath new_col;
       

    new_col.__isset.column = true; /* this is required! */
    new_col.column_family.assign("Data");
    new_col.super_column.assign("");
    new_col.column.assign("second");
 
    client.insert("drizzle",
                  "post1",
                  new_col,
                  "this is post1!!",
                  55,
                  ONE);
    client.insert("drizzle",
		  "post2",
		  new_col,
		  "this is post2!!",
		  56,
		  ONE);
    client.insert("drizzle",
                  "post3",
                  new_col,
                  "this is post3!!",
                  57,
                  ONE);
    client.insert("drizzle",
                  "post4",
                  new_col,
                  "this is post4!!",
                  57,
                  ONE);
   client.insert("drizzle",
                  "post5",
                  new_col,
                  "this is post5!!",
                  58,
                  ONE);
    client.insert("drizzle",
                  "post6",
                  new_col,
                  "this is post6!!",
                  56,
                  ONE);


 
    ColumnOrSuperColumn ret_val;
 
    client.get(ret_val,
               "drizzle",
               "post1",
               new_col,
               ONE);
 
    printf("Column name retrieved is: %s\n", ret_val.column.name.c_str());
    printf("Value in column retrieved is: %s\n", ret_val.column.value.c_str());
 
    transport->close();
  }
  catch (InvalidRequestException &re)
  {
    printf("ERROR: %s\n", re.why.c_str());
  }
  catch (TException &tx)
  {
    printf("ERROR: %s\n", tx.what());
  }
}


