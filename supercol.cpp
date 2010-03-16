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
  
  transport->open();
  
  ColumnPath new_col;
  new_col.__isset.super_column=true;
  new_col.__isset.column=true;
  new_col.column_family.assign("Posts");


  new_col.column.assign("thread_1");
  new_col.super_column.assign("thrd"); 
 
  client.insert("keyspace_forum",
	 	"post_1",
		new_col,
		"this is value in key post_1, with column name thread_1, super column Threads",
		123,
		ONE);
  transport->close();
  return 0;
}

