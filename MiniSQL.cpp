/************************����MiniSQL��Main������ʵ��****************************************/

#include "Interpreter.cpp"
#include "API.cpp"
#include "CatalogManager.cpp"
#include "BufferManager.cpp"
#include "RecordManager.cpp"

#include <iostream>
#include <string>
#include <fstream>


using namespace std;

//������
class CatalogManager;
class BufferManager;
class Interpreter;
class API;

#define CREATE 0  //SQL���ĵ�һ���ؼ���
#define SELECT 1
#define DROP 2
#define DELETE 3
#define INSERT 4
#define QUIT 5
#define COMMIT 13
#define EXECFILE 14

#define TABLE 6   //SQL���ĵڶ����ؼ���
#define INDEX 7
#define UNKNOWN 8

//ȫ�ֱ����Ķ��壬����������������ʹ��
CatalogManager cm;
BufferManager bm;
API ap;


int main()
{
	//�����ʾ��Ϣ
	cout<<"*******************Welocme to use our MiniSQL**********************"<<endl;
	int flag=0;           //��Ƕ�ȡSQL����״̬�����flag=1,��Ϊ�ļ��ж������flag=0����Ϊ��׼IO�ж�ȡ
	ifstream file;        //��������ű�
	while(1)
	{
		Interpreter in;   //�﷨��������
		string s;
		//���flag==0�������ļ��ж��룬�ӱ�׼IO���룬�������ʾ��
		if(!flag)
		{
			//������ʾ��Ϣ
			cout<<">>";
			//��';'��ΪSQL�������ı�־,����һ��SQL���
			getline(cin,s,';');
		}
		//���flag==1, ���ļ��ж���SQL���
		else
		{
			cout<<endl;
			getline(file,s,';');

			//��������ű�ĩβ��ǣ����˳��ļ���ȡ״̬������flag=0;
			int sss=s.find("$end");
			if(sss>=0)
			{
				flag=0;
				file.close();
				in.~Interpreter();
				continue;
			}
		}

		//��SQL�����н������������ʧ�ܣ����˳����ܽ�����ļ���ȡ״̬�����¶���SQL���
		if(!in.interpreter(s))
		{
			flag=0;
			//�ж��ļ��Ƿ�򿪣�����򿪣�����ر�
			if(file.is_open())
				file.close();
			//����in����
			in.~Interpreter();
			continue;
		}

		//��firstKey���б��������ദ��
		switch(in.firstKey)
		{
			//firstKeyΪcreate
			case CREATE:
				//������
				if(in.secondKey==TABLE)
					ap.createTable(in.fileName,in.col,in.type,in.uniq,in.primKey);
				//��������
				else if(in.secondKey==INDEX)
					ap.createIndex(in.fileName,in.tableName,in.colName);
				else
					cout<<"Error. Usage: create name"<<endl;
				break;
			//firstKeyΪselect
			case SELECT:
				//��where������Ѱ
				if(in.condNum==0)
					ap.printRecord(in.fileName);
				//һ��where������Ѱ
				else if(in.condNum==1)
					ap.printRecord(in.fileName,in.col1,in.condition1,in.operater1);
				//����where������Ѱ
				else
					ap.printRecord(in.fileName,in.col1,in.condition1,in.operater1,
						in.col2,in.condition2,in.operater2,in.logic);
				break;
			//firstKdyΪdrop
			case DROP:
				//ɾ����
				if(in.secondKey==TABLE)
					ap.dropTable(in.fileName);
				//ɾ������
				else if(in.secondKey==INDEX)
					ap.dropIndex(in.fileName);
				else
					cout<<"Error. Usage: drop table name or index name"<<endl;
				break;
			//firstKeyΪdelete
			case DELETE:
				//������ɾ�����м�¼
				if(in.condNum==0)
					ap.deleteValue(in.fileName);
				//����һ��where����ɾ�����������ļ�¼
				else if(in.condNum==1)
					ap.deleteValue(in.fileName,in.col1,in.condition1,in.operater1);
				//��������where����ɾ�����������ļ�¼
				else
					ap.deleteValue(in.fileName,in.col1,in.condition1,in.operater1,
									in.col2,in.condition2,in.operater2,in.logic);
				break;
			//firstKeyΪinsert
			case INSERT:
				ap.insertRecord(in.fileName,in.insertValue);
				break;
			//firstKeyΪquit
			case QUIT:
				//���ֵ���Ϣд��
				if(!cm.writeBack())
				{
					cout<<"Error! Fail to write back db.info"<<endl;
					return 0;
				}
				//��������Ϣд���ļ�
				if(!bm.flushAll())
				{
					cout<<"Error! Fail to flush all the block into database"<<endl;
					return 0;
				}
				return 1;
			//firstKeyΪcommit
			case COMMIT:
				//���ֵ���Ϣд��
				if(!cm.writeBack())
					cout<<"Error! Fail to write back db.info"<<endl;
				//������д���ļ�
				if(!bm.flushAll())
					cout<<"Error! Fail to flush all the block into database"<<endl;
				break;

			//ִ�нű�
			case EXECFILE:

				//���ļ�,���ʧ�ܣ������������Ϣ
				file.open(in.fileName.c_str());
				if(!file.is_open())
				{
					cout<<"Fail to open "<<in.fileName<<endl;
					break;
				}

				//��״̬��Ϊ���ļ�����
				flag=1;
				break;
		}
		//����inʵ��
		in.~Interpreter();
	}
	return 1;
}
