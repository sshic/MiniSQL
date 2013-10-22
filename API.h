/************************些文件包括MiniSQL中API类的定义********************************************/
/************************作者:陈矫彦  时间：2009年10月*********************************************/

#ifndef API_H
#define API_H
#include "RecordManager.h"
#include <string>
#include <vector>
using namespace std;
class IndexManager;
class RecordManager;
class API{
public:
	RecordManager rm;
	API(){}
	~API(){}
	void dropTable(string tableName);
	void dropIndex(string indexName);

	void createIndex(string fileName,string tableName,string colName);
	void createTable(string tableName,vector<string> col,vector<string> type,vector<int> uniq,string primKey);

	void printRecord(string tableName);
	void printRecord(string tableName,string colName1,string cond1,string operater1);
	void printRecord(string tableName,string colName1,string cond1,string operater1,
		string colName2,string cond2,string operater2,int logic);

	void insertRecord(string tableName,vector<string> v);


	int insertIndexItem(string fileName,string colName,string value,int block,int index);
	int getIndexItem(string fileName,string colName,string value,int * block,int * index);

	void deleteValue(string tableName);
	void deleteValue(string tableName,string colName1,string cond1,string operater1);
	void deleteValue(string tableName,string colName1,string cond1,string operater1,
		string colName2,string cond2,string operater2,int logic);	

	int getRecordNum(string tableName);
	int calcuteLenth(string tableName);
	int calcuteLenth2(string type);
	vector<string> getCollName(string tableName);
	vector<string> getCollType(string tableName);

};

//这两个数据结构用于把int型，float型数据向字节转换，方便将其定长存储
struct int_t{
	int value;
};
struct float_t{
	float value;
};
#endif