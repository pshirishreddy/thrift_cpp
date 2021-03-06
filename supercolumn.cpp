#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <list>
#include <vector>
#include "Cassandra.h"

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;

static string host("cass01");
static int port = 9160;

int main()
{
	shared_ptr<TTransport> socket(new TSocket(host,port));
	shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
	CassandraClient client(protocol);
	
	transport->open();
	
	ColumnPath cp;
	cp.__isset.super_column=true;
	cp.__isset.column=true;
	cp.column_family.assign("Super1");
	cp.super_column.assign("superkey");
	cp.column.assign("subcolumn1");

	client.insert("Keyspace1",
		      "post_1",  //key 
		      cp,
		      "this is Super1",
		      34,
		      ONE);

	

	ColumnOrSuperColumn val;
	
	client.get(val, "Keyspace1", "post_1", cp, ONE);
	
	vector<Column> :: iterator lc;
	
	lc = val.super_column.columns.begin();
	(*lc).name.c_str();
	
		/*ColumnOrSuperColumn retval;
	vector<ColumnOrSuperColumn> ls;
	SlicePredicate sp;
	ColumnParent cpa;
	SliceRange sr;
	sr.start=(char*)NULL;

	sr.end=(char*)NULL;
	sr.reverse = false;
	sr.count = 10;
	sp.column_names.push_back("superkey");
	cpa.column_family="Super1";
	client.get_slice(ls,"Keyspace1","post_1",cpa,sp,ONE);
	
//	vector<ColumnOrSuperColumn> :: iterator vit;
	//vit = ls.begin();

//	cout<<(*vit).column.name*/

	return 0;
}
