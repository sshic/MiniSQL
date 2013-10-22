/************************这些文件包括MiniSQL中API类里面的函数实现************************************/
#include "API.h"
#include "CatalogManager.h"
#include <iostream>
#include <vector>
#include <string>

#define UNKNOWN_FILE 8                                 //三个宏定义，用于定义文件类型
#define TABLE_FILE 9
#define INDEX_FILE 10

using namespace std;
class CatalogManager;
class RecordManager;


extern CatalogManager cm;                             //对全局对象的外部声明

//打印一个表中所有的记录和打印的记录数
void API::printRecord(string tableName)
{
	//查找字典信息，如果文件不存，直接出错提示
	if(cm.findFile(tableName)==TABLE_FILE)
	{
		int m;
		m=rm.selectRecord(tableName);
		cout<<m<<" records selected"<<endl;
	}
	else
		cout<<"There is no table "<<tableName<<endl;
}

//根据一个where条件打印表中的记录和打印的记录数
void API::printRecord(string tableName,string colName1,string cond1,string operater1)
{
	//查找字典信息，如果文件不存，直接出错提示
	if(cm.findFile(tableName)==TABLE_FILE)
	{
		int m;
		m=rm.selectRecord(tableName,colName1,cond1,operater1);
		cout<<m<<" records selected"<<endl;
	}
	else
		cout<<"There is no table "<<tableName<<endl;
}

//根据两个where条件打印表中的记录和打印的记录数
void API::printRecord(string tableName,string colName1,string cond1,string operater1,
		string colName2,string cond2,string operater2,int logic)
{
	//查找字典信息，如果文件不存，直接出错提示
	if(cm.findFile(tableName)==TABLE_FILE)
	{
		int m;
		m=rm.selectRecord(tableName,colName1,cond1,operater1,colName2,cond2,operater2,logic);
		cout<<m<<" records selected"<<endl;
	}
	else
		cout<<"There is no table "<<tableName<<endl;
}

//向表中插入入记录
void API::insertRecord(string tableName,vector<string> v)
{
	//查找字典信息，如果文件不存，直接出错提示
	if(cm.findFile(tableName)!=TABLE_FILE)
	{
		cout<<"There is no table "<<tableName<<endl;
		return ;
	}
		vector<string> type;
		type=cm.getCollType(tableName);

		//s用来作为记录的暂时存放数组
		char s[2000];
		memset(s,0,2000);
		char *p;
		int pos=0;
		for(unsigned i=0;i<v.size();i++)
		{
			//如果类型超出临时数组的长度
			if(cm.calcuteLenth2(type.at(i))+pos>2000)
			{
				cout<<"Failed to insert. The record is too long"<<endl;
				break;
			}
			//如果是整型变量，则将整型转为char型,以char的字节形式存入数组
			if(type.at(i)=="int")
			{
				int_t t;
				t.value=atoi(v.at(i).c_str());
				p=(char *)&t;
				for(int j=0;j<sizeof(int);j++,pos++)
				{
					s[pos]=*p;
					p++;
				}
			}
			//如果是float形变量，则将float转为char型，以char的字节形式存入数组
			else if(type.at(i)=="float")
			{
				float_t tt;
				tt.value=(float)atof(v.at(i).c_str());
				p=(char *)&tt;
				for(int j=0;j<sizeof(float);j++,pos++)
				{
					s[pos]=*p;
					p++;
				}
			}
			//如果是字符型的变量，直接进行字节存储
			else
			{
				//如果实际输入的string长于定义的char长度，则报错。
				if(v.at(i).length()>(unsigned int)cm.calcuteLenth2(type.at(i)))
				{
					cout<<"Insert Failed. The string of "<<v.at(i)<<" is too long"<<endl;
					break;
				}
				const char * cp;
				cp=v.at(i).c_str();
				for(unsigned int j=0;j<v.at(i).length();j++,pos++)
				{
					s[pos]=*cp;
					cp++;
				}
				//由于是定长存储，所以列定义超出部分以0存储.
				for(unsigned int j=v.at(i).length();j<(unsigned int)cm.calcuteLenth2(type.at(i));j++,pos++)
					;

			}
		}

		//如果在数据中插入记录成功，则同样在字典信息中插入记录信息
		if(rm.insertRecord(tableName,s))
		{
			cm.insertRecord(tableName,1);
		}

}

//将表中的记录全部删除,同时输出删除的记录数目
void API::deleteValue(string tableName)
{
	//查找字典信息，如果文件不存，直接出错提示
	if(cm.findFile(tableName)!=TABLE_FILE)
	{
		cout<<"There is no table "<<tableName<<endl;
		return ;
	}
	//首先在RecordManager中删除记录，然后在数据字典中将记录数改为0
	int num=rm.deleteValue(tableName);
	if(cm.deleteValue(tableName))
		cout<<"Delete "<<num<<" records "<<"in "<<tableName<<endl;
	else
		cout<<"Fail to delete in table "<<tableName<<endl;
}

//根据一个where条件删除表中的记录，同时输出删除记录的数目
void API::deleteValue(string tableName,string colName1,string cond1,string operater1)
{
	//查找字典信息，如果文件不存，直接出错提示
	if(cm.findFile(tableName)!=TABLE_FILE)
	{
		cout<<"There is no table "<<tableName<<endl;
		return ;
	}
	int num=rm.deleteValue(tableName,colName1,cond1,operater1);
	if(cm.deleteValue(tableName,num))
		cout<<"Delete "<<num<<" records "<<"in "<<tableName<<endl;
	else
		cout<<"Fail to delete in table "<<tableName<<endl;
}

//根据两个where条件删除表中的记录,同时输出删除记录的数目
void API::deleteValue(string tableName,string colName1,string cond1,string operater1,
		string colName2,string cond2,string  operater2,int logic)
{
	//查找字典信息，如果文件不存，直接出错提示
	if(cm.findFile(tableName)!=TABLE_FILE)
	{
		cout<<"There is no table "<<tableName<<endl;
		return ;
	}
	int num=rm.deleteValue(tableName,colName1,cond1,operater1,colName2,cond2,operater2,logic);
	if(cm.deleteValue(tableName,num))
		cout<<"Delete "<<num<<" records "<<"in "<<tableName<<endl;
	else
		cout<<"Fail to delete in table "<<tableName<<endl;
}



//查找文件，返回在这个文件的记录数目
int API::getRecordNum(string tableName)
{
	return cm.getRecordNum(tableName);
}

//返回文件属性列表
vector<string> API::getCollName(string tableName)
{
	return cm.getCollName(tableName);
}

//返回文件属性类型列表
vector<string> API::getCollType(string tableName)
{
	return cm.getCollType(tableName);
}

//根据表名，返回文件中的记录长度
int API::calcuteLenth(string tableName)
{
	return cm.calcuteLenth(tableName);
}

//根据某个属性类型，返回这个属性类型的长度
int API::calcuteLenth2(string type)
{
	return cm.calcuteLenth2(type);
}

//删除一个表
void API::dropTable(string tableName)
{
	//查找字典信息，如果文件不存，直接出错提示
	if(cm.findFile(tableName)!=TABLE_FILE)
	{
		cout<<"There is no table "<<tableName<<endl;
		return ;
	}
	//直接在字典信息中将表删除
	if(cm.dropTable(tableName))
		cout<<"Drop table "<<tableName<<" successfully"<<endl;
}

//删除一个索引
void API::dropIndex(string indexName)
{
	//查找字典信息，如果索引不存，直接出错提示
	if(cm.findFile(indexName)!=INDEX_FILE)
	{
		cout<<"There is no index "<<indexName<<endl;
		return ;
	}
	if(cm.dropIndex(indexName))
		cout<<"Drop index "<<indexName<<" successfully"<<endl;
}

//根据给定的表、属性，创建索引
void API::createIndex(string fileName,string tableName,string colName)
{
	//查找字典信息，如果索引存在，直接出错提示
	if(cm.findFile(fileName)==INDEX_FILE)
	{
		cout<<"There is index "<<fileName<<" already"<<endl;
		return ;
	}



	//在IndexManager中增加索引




	//在字典信息中增加索引
	cm.addIndex(fileName,tableName,colName);
	cout<<"Create index "<<fileName<<" successfully"<<endl;
}

//根据列、主键等信息创建表
void API::createTable(string tableName,vector<string> col,vector<string> type,vector<int> uniq,string primKey)
{
	//查找字典信息，如果表已经存在，直接出错提示
	if(cm.findFile(tableName)==TABLE_FILE)
	{
		cout<<"There is table "<<tableName<<" already"<<endl;
		return ;
	}
	if(cm.addTable(tableName,col,type,uniq,primKey))
		cout<<"Create table "<<tableName<<" successfully"<<endl;
}


//给定表名，列名，这个例的一个值以及这条记录在数据文件中的储位置，将这个值插入到索引引文件中
//其中的位置是块偏移和记录在块中的位置
int API::insertIndexItem(string fileName,string colName,string value,int block,int index)
{

	return 1;
}

//给定表名、列名，以及这个列上的一个值，返回这条记录在数据文件中对应的位置
int API::getIndexItem(string fileName,string colName,string value,int * block,int * index)
{
	return 1;
}
