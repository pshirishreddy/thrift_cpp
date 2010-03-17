#include "Cassandra.h"
#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include <transport/TSocket.h>
#include <string>
//#include <stdio.h>
#include <iostream>
#include <fstream>
#include <pthread.h>
#include <vector>
#include <map>

using namespace std;

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

using boost::shared_ptr;

using namespace org::apache::cassandra;

//macros..
#define MAX_STRLEN  100000
#define ITEM_COUNT   100000
#define ChapterNumPerNovel 30

//global variables  & functions 

int port = 9160;	

void insert();
void get(void );
void get_slice();
void remove();

const string strKeyspace = "Keyspace1";
const string strColFamily_org = "Super1";
string strColFamily = "Super1";
string strColOrg = "chapter";
string strCol = "chapter";
string strSuperColOrg = "Novel";
string strSuperCol = "superkey";
string strKeyOrg = "";
string strKey = "post_1";
string strValue = "";
size_t thread_id = 0;

bool STOP = false;
size_t start_time = 0;
size_t average_time = 0;
int ret_num = 0;
unsigned long long total_get_count = 0;
unsigned long long ntmp = 10;
int error_num = 0;





int main(int argc, char **argv)
{
	//insert();
	//get(NULL);
	get_slice();
	//remove();
  
    
	return EXIT_SUCCESS;
}


void insert()
{
	
	shared_ptr<TTransport> socket_novel(new TSocket("cass01", port));		
	shared_ptr<TTransport> transport_novel(new TBufferedTransport(socket_novel) );
	shared_ptr<TProtocol> protocol_novel(new TBinaryProtocol(transport_novel) );
	CassandraClient client(protocol_novel);
	transport_novel->open();
	string strValueOrg = "1234567";

	
	ColumnPath col_path;
	int i = 0;
	char chTmp[MAX_STRLEN];
	string str_zeros;
	while(i<ITEM_COUNT )
	{
		memset(&chTmp, 0, MAX_STRLEN);
		sprintf(chTmp, "%d", i++);
		
		strValue = strValueOrg + string(chTmp);
		strKey = strKeyOrg + string(chTmp);
		strSuperCol = strSuperColOrg + string(chTmp);
		
		col_path.column_family.assign(strColFamily );
		col_path.column.assign(strCol);
		col_path.super_column.assign(strSuperCol);
		col_path.__isset.super_column = true;
		col_path.__isset.column = true;


		try
		{
			for(int j=0; j<ChapterNumPerNovel; j++)
			{
				memset(&chTmp, 0, MAX_STRLEN);
				sprintf(chTmp, "%d", j);
				int len = string(chTmp).length();
				str_zeros = "";
				for(int k=len; k<10; k++)
				{
					str_zeros += "0";
				}
				strCol= strSuperCol + "_" + strColOrg + str_zeros + string(chTmp);
				col_path.column.assign(strCol);
				client.insert(strKeyspace,strKey, col_path, strValue,time(NULL), ONE);
			}
		}
		catch(TException &tx)
		{
			printf("ERROR: %s\n", tx.what());
			error_num ++ ;
		}

		total_get_count ++ ;
		
	}

	STOP = true;
	transport_novel->close();
}

void remove()
{
	shared_ptr<TTransport> socket_novel(new TSocket("cass01", port));		
	shared_ptr<TTransport> transport_novel(new TBufferedTransport(socket_novel) );
	shared_ptr<TProtocol> protocol_novel(new TBinaryProtocol(transport_novel) );
	CassandraClient client(protocol_novel);
	transport_novel->open();

	int nRand = 0;
	srand((unsigned)time(NULL) );
	ColumnPath col_path;
	char chTmp[MAX_STRLEN];
	string str_zeros;
	while(true )
	{
		nRand = 0;
		usleep(1000*200);
		nRand = rand()%ITEM_COUNT;
		memset(&chTmp, 0, MAX_STRLEN);
		sprintf(chTmp, "%d", nRand);
		
		strKey = strKeyOrg + string(chTmp);
		strSuperCol = strSuperColOrg + string(chTmp);
		
		col_path.column_family.assign(strColFamily );
		col_path.column.assign(strCol);
		col_path.super_column.assign(strSuperCol);
		col_path.__isset.super_column = true;
		col_path.__isset.column = false;


		try
		{
			memset(&chTmp, 0, MAX_STRLEN);
			sprintf(chTmp, "%d", rand()%ChapterNumPerNovel);
			int len = string(chTmp).length();
			str_zeros = "";
			for(int k=len; k<10; k++)
			{
				str_zeros += "0";
			}
			strCol= strSuperCol + "_" + strColOrg + str_zeros + string(chTmp);

			client.remove(strKeyspace,strKey, col_path,time(NULL), ONE);
		}
		catch(TException &tx)
		{
			printf("ERROR: %s\n", tx.what());
			error_num ++ ;
		}
		
	}
	STOP = true;
	transport_novel->close();
}


void get_slice()
{
	shared_ptr<TTransport> socket_novel(new TSocket("cass01", port));		
	shared_ptr<TTransport> transport_novel(new TBufferedTransport(socket_novel) );
	shared_ptr<TProtocol> protocol_novel(new TBinaryProtocol(transport_novel) );
	CassandraClient client(protocol_novel);
	
	transport_novel->open();
	
	string strCol = "";
	string strKey = "";

	int nRand = 0;
	srand((unsigned)time(NULL)+thread_id);
	bool success = false;
	ColumnPath col_path;
	vector <ColumnOrSuperColumn> ret_vals;
	ColumnParent col_parent;
	col_parent.column_family.assign(strColFamily);
	col_parent.__isset.super_column = true;

	SlicePredicate predicate;
	predicate.__isset.column_names = false;
	predicate.__isset.slice_range = true;
	predicate.slice_range.count = 30;
	//vector<string> colnames_predicate;
	char chNum[100];
	int i = 0;
	while( i < 10000)
	{
		nRand = rand()%ITEM_COUNT;
		nRand = 8860;
		memset(&chNum, 0, 100);
		sprintf(chNum, "%d", nRand);
		strSuperCol = strSuperColOrg + string(chNum);
		strKey = strKeyOrg + string(chNum);

		col_parent.super_column.assign(strSuperCol);
		
		while(!success )
		{
			try
			{
				//client.get(ret_val, strKeyspace, strKey, col_path, ONE);
				client.get_slice(ret_vals, strKeyspace, strKey, col_parent, predicate, ONE);
			}
			catch (InvalidRequestException &re)
			{
				success = false;
				error_num ++ ;
				printf("ERROR: %s\n", re.why.c_str());
				cout << i << "\trand:" << nRand << '\t';
				//continue;
			}
			catch(TException &tx)
			{
				printf("ERROR: %s\n", tx.what());
				error_num ++ ;
				success = false;
				cout << i << "\trand:" << nRand << '\t';
				//continue;
			}
			success = true;
			
		}
		success = false;
		total_get_count ++ ;
		
		i++;
	
	}

	STOP = true;
	transport_novel->close();

}

void get()
{
	thread_id++;
	shared_ptr<TTransport> socket_novel(new TSocket("cass01", port));		
	shared_ptr<TTransport> transport_novel(new TBufferedTransport(socket_novel) );
	shared_ptr<TProtocol> protocol_novel(new TBinaryProtocol(transport_novel) );
	CassandraClient client(protocol_novel);
	
	transport_novel->open();
	start_time = time(NULL);
	
	string strCol = "";
	string strKey = "";

	int nRand = 0;
	srand((unsigned)time(NULL)+thread_id);
	bool success = false;
	ColumnPath col_path;
	ColumnOrSuperColumn ret_val;
	char chNum[100];
	int i = 0;
	while( i < 10000)
	{
		nRand = rand()%ITEM_COUNT;
		memset(&chNum, 0, 100);
		sprintf(chNum, "%d", nRand);
		strSuperCol = strSuperColOrg + string(chNum);
		strKey = strKeyOrg + string(chNum);

		col_path.column_family.assign(strColFamily);
		col_path.super_column.assign(strSuperCol);
		col_path.__isset.super_column = true;
		col_path.__isset.column = true;

		nRand = rand()%ChapterNumPerNovel;
		memset(&chNum, 0, 100);
		sprintf(chNum, "%d", nRand);
		strCol= strSuperCol + "_" + strColOrg + string(chNum);
		col_path.column.assign(strCol);
		while(!success )
		{
			try
			{
				client.get(ret_val, strKeyspace, strKey, col_path, ONE);
			}
			catch (InvalidRequestException &re)
			{
				success = false;
				error_num ++ ;
				printf("ERROR: %s\n", re.why.c_str());
				cout << i << "\trand:" << nRand << '\t';
				//continue;
			}
			catch(TException &tx)
			{
				printf("ERROR: %s\n", tx.what());
				error_num ++ ;
				success = false;
				cout << i << "\trand:" << nRand << '\t';
				//continue;
			}
			success = true;
			
		}
		success = false;
		total_get_count ++ ;
		
		i++;
	
	}

	average_time += (time(NULL) - start_time);
	ret_num++;
	transport_novel->close();

	//return NULL;
	
}


