#include "Cassandra.h"
#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include <transport/TSocket.h>
#include <string>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <pthread.h>
#include <vector>
#include <map>

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;

static int port = 9160;
static string host("cass01");

void insert();
void get();
void get_slice();

string strCol;
string strColFamily;

int main()
{
  insert();
  get();
  get_slice();
  return 0;
}

void insert()
{
  shared_ptr<TTransport> socket_forum(new TSocket("cass01",port));
  shared_ptr<TTransport> transport_forum(new TBufferedTransport(socket_forum));
  shared_ptr<TProtocol> protocol_forum(new TBinaryProtocol(transport_forum));
  CassandraClient client(protocol_forum);
  transport_forum->open();
  
  ColumnPath col_path;
  
  col_path.__isset.column=true;
  col_path.column_family.assign("Forum");
  
  col_path.column.assign("catrelatedDiscussion");
  try
  {
	client.insert("Standard_Forum",
				  "num_posts",
				  col_path,
				  "1300",
				  time(NULL),
				  ONE);
	client.insert("Standard_Forum",
				  "last_post",
				  col_path,
				  "IIM call getters",
				  time(NULL),
				  ONE);
	client.insert("Standard_Forum",
				  "popular_tags",
				  col_path,
				  "cat,IIM,call",
				  time(NULL),
				  ONE);
  }
  catch(TException &ex)
  {
	cout<<"ERROR: "<<ex.what();
  }
  
  col_path.column.assign("PGprep");
  try
  {
	client.insert("Standard_Forum",
				  "num_posts",
				  col_path,
				  "300",
				  time(NULL),
				  ONE);
	client.insert("Standard_Forum",
				  "last_post",
				  col_path,
				  "GDPI",
				  time(NULL),
				  ONE);
	client.insert("Standard_Forum",
				  "popular_tags",
				  col_path,
				  "GD,english,aptitude",
				  time(NULL),
				  ONE);
  }
  catch(TException &ex)
  {
	cout<<"ERROR: "<<ex.what();
  }
  transport_forum->close();
}

void get()
{
  
  shared_ptr<TTransport> socket_forum(new TSocket("cass01",port));
  shared_ptr<TTransport> transport_forum(new TBufferedTransport(socket_forum));
  shared_ptr<TProtocol> protocol_forum(new TBinaryProtocol(transport_forum));
  CassandraClient client(protocol_forum);
  transport_forum->open();
  
  ColumnPath col_path;
  
  col_path.__isset.column=true;
  col_path.column_family.assign("Forum");
  
  col_path.column.assign("catrelatedDiscussion");

  ColumnOrSuperColumn ret_val;
  
  client.get(ret_val, "Standard_Forum", "num_posts", col_path, ONE);
  cout<<"In the column "<<ret_val.column.name.c_str()<<"\n";
  cout<<"Has the Value "<<ret_val.column.value.c_str()<<"\n";
}
  
void get_slice()
{
  shared_ptr<TTransport> socket_forum(new TSocket("cass01",port));
  shared_ptr<TTransport> transport_forum(new TBufferedTransport(socket_forum));
  shared_ptr<TProtocol> protocol_forum(new TBinaryProtocol(transport_forum));
  CassandraClient client(protocol_forum);
  transport_forum->open();
  
  ColumnPath col_path;
  
  vector <ColumnOrSuperColumn> ret_vals;
  ColumnParent col_parent;
  col_parent.column_family.assign("Forum");
  col_parent.__isset.super_column=false;
  
  SlicePredicate predicate;
  predicate.__isset.column_names=false;
  predicate.__isset.slice_range=true;
  predicate.slice_range.count=30;
	
  try
  {
	client.get_slice(ret_vals, "Standard_Forum", "last_post", col_parent, predicate, ONE);
  }
  catch(TException &ex)
  {
	cout<<"ERROR: "<<ex.what()<<endl;
  }
  
  vector <ColumnOrSuperColumn> :: iterator vit;
  for(vit = ret_vals.begin(); vit!=ret_vals.end(); vit++)
  cout<<"Column Name: "<<(*vit).column.name.c_str()<<"\t"<<"Column value in num_posts :"<<(*vit).column.value.c_str()<<endl;
  
  
}

  
  
