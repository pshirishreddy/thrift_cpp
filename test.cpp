#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <list>

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

static string host("cass01");
static int port = 9160;

int main()
{
	shared_ptr<TTransport> socket(new TSocket(host, port));
	shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	CassandraClient client(protocol);
	
	transport->open();
	
	SuperColum sc;
	
	ColumnPath cp;
	cp.__isset.column=true;
	cp.column_family.assign("Posts");
	cp.super_column.assign("");
	cp.column.assign("thread1");
	
	client.insert("keyspace_forum",
		      "post_1",
		       cp,
		       "this is first thread in post_1",
		       45,
		       ONE);
	cp.column.assign("thread2");
	client.insert("keyspace_forum",
			"post_1",
			cp,
			"this is second thread in post_1",
			46,
			ONE);
	cp.column.assign("thread3");
	client.insert("keyspace_forum",
			"post_1",
			cp,
			"this is third thread in post_1",
			47,
			ONE);
	
	/** Post_2 **/
	cp.column.assign("thread1");
	client.insert("keyspace_forum",
			"post_2",
			cp,
			"this is first thread in post_2",	
			48,
			ONE);
	cp.column.assign("thread2");
	client.insert("keyspace_forum",
			"post_2",
			cp,
			"this is second thread in post_2",
			49,
			ONE);
	cp.column.assign("thread3");
	client.insert("keyspace_forum",
			"post_2",
			cp,
			"this is third thread in post_2",
			50,
			ONE);
			
	/** end post_2 **/


	ColumnOrSuperColumn ret_val;

	    client.get(ret_val,
               "keyspace_forum",
               "post_1",
               cp,
               ONE);

	    printf("Column name retrieved is: %s\n", ret_val.column.name.c_str());
	    printf("Value in column retrieved is: %s\n", ret_val.column.value.c_str());

	transport->close();
	

	return 0;

}
