/************************包括MiniSQL中Main函数的实现****************************************/

#include "Interpreter.cpp"
#include "API.cpp"
#include "CatalogManager.cpp"
#include "BufferManager.cpp"
#include "RecordManager.cpp"

#include <iostream>
#include <string>
#include <fstream>


using namespace std;

//类声明
class CatalogManager;
class BufferManager;
class Interpreter;
class API;

#define CREATE 0  //SQL语句的第一个关键字
#define SELECT 1
#define DROP 2
#define DELETE 3
#define INSERT 4
#define QUIT 5
#define COMMIT 13
#define EXECFILE 14

#define TABLE 6   //SQL语句的第二个关键字
#define INDEX 7
#define UNKNOWN 8

//全局变量的定义，可以在整个工程内使用
CatalogManager cm;
BufferManager bm;
API ap;


int main()
{
	//输出揭示信息
	cout<<"*******************Welocme to use our MiniSQL**********************"<<endl;
	int flag=0;           //标记读取SQL语句的状态，如果flag=1,则为文件中读，如果flag=0，则为标准IO中读取
	ifstream file;        //用于输入脚本
	while(1)
	{
		Interpreter in;   //语法解析对象
		string s;
		//如果flag==0，不从文件中读入，从标准IO读入，则输出提示符
		if(!flag)
		{
			//输入提示信息
			cout<<">>";
			//以';'作为SQL语句结束的标志,输入一条SQL语句
			getline(cin,s,';');
		}
		//如果flag==1, 从文件中读入SQL语句
		else
		{
			cout<<endl;
			getline(file,s,';');

			//如果读到脚本末尾标记，则退出文件读取状态，设置flag=0;
			int sss=s.find("$end");
			if(sss>=0)
			{
				flag=0;
				file.close();
				in.~Interpreter();
				continue;
			}
		}

		//对SQL语句进行解析，如果解析失败，则退出可能进入的文件读取状态，重新读入SQL语句
		if(!in.interpreter(s))
		{
			flag=0;
			//判断文件是否打开，如果打开，则将其关闭
			if(file.is_open())
				file.close();
			//析构in对象
			in.~Interpreter();
			continue;
		}

		//对firstKey进行遍历，分类处理
		switch(in.firstKey)
		{
			//firstKey为create
			case CREATE:
				//创建表
				if(in.secondKey==TABLE)
					ap.createTable(in.fileName,in.col,in.type,in.uniq,in.primKey);
				//创建索引
				else if(in.secondKey==INDEX)
					ap.createIndex(in.fileName,in.tableName,in.colName);
				else
					cout<<"Error. Usage: create name"<<endl;
				break;
			//firstKey为select
			case SELECT:
				//无where条件查寻
				if(in.condNum==0)
					ap.printRecord(in.fileName);
				//一个where条件查寻
				else if(in.condNum==1)
					ap.printRecord(in.fileName,in.col1,in.condition1,in.operater1);
				//二个where条件查寻
				else
					ap.printRecord(in.fileName,in.col1,in.condition1,in.operater1,
						in.col2,in.condition2,in.operater2,in.logic);
				break;
			//firstKdy为drop
			case DROP:
				//删除表
				if(in.secondKey==TABLE)
					ap.dropTable(in.fileName);
				//删除索引
				else if(in.secondKey==INDEX)
					ap.dropIndex(in.fileName);
				else
					cout<<"Error. Usage: drop table name or index name"<<endl;
				break;
			//firstKey为delete
			case DELETE:
				//无条件删除所有记录
				if(in.condNum==0)
					ap.deleteValue(in.fileName);
				//根据一个where条件删除满足条件的记录
				else if(in.condNum==1)
					ap.deleteValue(in.fileName,in.col1,in.condition1,in.operater1);
				//根据两个where条件删除满足条件的记录
				else
					ap.deleteValue(in.fileName,in.col1,in.condition1,in.operater1,
									in.col2,in.condition2,in.operater2,in.logic);
				break;
			//firstKey为insert
			case INSERT:
				ap.insertRecord(in.fileName,in.insertValue);
				break;
			//firstKey为quit
			case QUIT:
				//将字典信息写回
				if(!cm.writeBack())
				{
					cout<<"Error! Fail to write back db.info"<<endl;
					return 0;
				}
				//将数据信息写回文件
				if(!bm.flushAll())
				{
					cout<<"Error! Fail to flush all the block into database"<<endl;
					return 0;
				}
				return 1;
			//firstKey为commit
			case COMMIT:
				//将字典信息写回
				if(!cm.writeBack())
					cout<<"Error! Fail to write back db.info"<<endl;
				//将数据写回文件
				if(!bm.flushAll())
					cout<<"Error! Fail to flush all the block into database"<<endl;
				break;

			//执行脚本
			case EXECFILE:

				//打开文件,如果失败，则输出错误信息
				file.open(in.fileName.c_str());
				if(!file.is_open())
				{
					cout<<"Fail to open "<<in.fileName<<endl;
					break;
				}

				//将状态设为从文件输入
				flag=1;
				break;
		}
		//析构in实例
		in.~Interpreter();
	}
	return 1;
}
