/************************��Щ�ļ�����MiniSQL��API������ĺ���ʵ��************************************/
#include "API.h"
#include "CatalogManager.h"
#include <iostream>
#include <vector>
#include <string>

#define UNKNOWN_FILE 8                                 //�����궨�壬���ڶ����ļ�����
#define TABLE_FILE 9
#define INDEX_FILE 10

using namespace std;
class CatalogManager;
class RecordManager;


extern CatalogManager cm;                             //��ȫ�ֶ�����ⲿ����

//��ӡһ���������еļ�¼�ʹ�ӡ�ļ�¼��
void API::printRecord(string tableName)
{
	//�����ֵ���Ϣ������ļ����棬ֱ�ӳ�����ʾ
	if(cm.findFile(tableName)==TABLE_FILE)
	{
		int m;
		m=rm.selectRecord(tableName);
		cout<<m<<" records selected"<<endl;
	}
	else
		cout<<"There is no table "<<tableName<<endl;
}

//����һ��where������ӡ���еļ�¼�ʹ�ӡ�ļ�¼��
void API::printRecord(string tableName,string colName1,string cond1,string operater1)
{
	//�����ֵ���Ϣ������ļ����棬ֱ�ӳ�����ʾ
	if(cm.findFile(tableName)==TABLE_FILE)
	{
		int m;
		m=rm.selectRecord(tableName,colName1,cond1,operater1);
		cout<<m<<" records selected"<<endl;
	}
	else
		cout<<"There is no table "<<tableName<<endl;
}

//��������where������ӡ���еļ�¼�ʹ�ӡ�ļ�¼��
void API::printRecord(string tableName,string colName1,string cond1,string operater1,
		string colName2,string cond2,string operater2,int logic)
{
	//�����ֵ���Ϣ������ļ����棬ֱ�ӳ�����ʾ
	if(cm.findFile(tableName)==TABLE_FILE)
	{
		int m;
		m=rm.selectRecord(tableName,colName1,cond1,operater1,colName2,cond2,operater2,logic);
		cout<<m<<" records selected"<<endl;
	}
	else
		cout<<"There is no table "<<tableName<<endl;
}

//����в������¼
void API::insertRecord(string tableName,vector<string> v)
{
	//�����ֵ���Ϣ������ļ����棬ֱ�ӳ�����ʾ
	if(cm.findFile(tableName)!=TABLE_FILE)
	{
		cout<<"There is no table "<<tableName<<endl;
		return ;
	}
		vector<string> type;
		type=cm.getCollType(tableName);

		//s������Ϊ��¼����ʱ�������
		char s[2000];
		memset(s,0,2000);
		char *p;
		int pos=0;
		for(unsigned i=0;i<v.size();i++)
		{
			//������ͳ�����ʱ����ĳ���
			if(cm.calcuteLenth2(type.at(i))+pos>2000)
			{
				cout<<"Failed to insert. The record is too long"<<endl;
				break;
			}
			//��������ͱ�����������תΪchar��,��char���ֽ���ʽ��������
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
			//�����float�α�������floatתΪchar�ͣ���char���ֽ���ʽ��������
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
			//������ַ��͵ı�����ֱ�ӽ����ֽڴ洢
			else
			{
				//���ʵ�������string���ڶ����char���ȣ��򱨴�
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
				//�����Ƕ����洢�������ж��峬��������0�洢.
				for(unsigned int j=v.at(i).length();j<(unsigned int)cm.calcuteLenth2(type.at(i));j++,pos++)
					;

			}
		}

		//����������в����¼�ɹ�����ͬ�����ֵ���Ϣ�в����¼��Ϣ
		if(rm.insertRecord(tableName,s))
		{
			cm.insertRecord(tableName,1);
		}

}

//�����еļ�¼ȫ��ɾ��,ͬʱ���ɾ���ļ�¼��Ŀ
void API::deleteValue(string tableName)
{
	//�����ֵ���Ϣ������ļ����棬ֱ�ӳ�����ʾ
	if(cm.findFile(tableName)!=TABLE_FILE)
	{
		cout<<"There is no table "<<tableName<<endl;
		return ;
	}
	//������RecordManager��ɾ����¼��Ȼ���������ֵ��н���¼����Ϊ0
	int num=rm.deleteValue(tableName);
	if(cm.deleteValue(tableName))
		cout<<"Delete "<<num<<" records "<<"in "<<tableName<<endl;
	else
		cout<<"Fail to delete in table "<<tableName<<endl;
}

//����һ��where����ɾ�����еļ�¼��ͬʱ���ɾ����¼����Ŀ
void API::deleteValue(string tableName,string colName1,string cond1,string operater1)
{
	//�����ֵ���Ϣ������ļ����棬ֱ�ӳ�����ʾ
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

//��������where����ɾ�����еļ�¼,ͬʱ���ɾ����¼����Ŀ
void API::deleteValue(string tableName,string colName1,string cond1,string operater1,
		string colName2,string cond2,string  operater2,int logic)
{
	//�����ֵ���Ϣ������ļ����棬ֱ�ӳ�����ʾ
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



//�����ļ�������������ļ��ļ�¼��Ŀ
int API::getRecordNum(string tableName)
{
	return cm.getRecordNum(tableName);
}

//�����ļ������б�
vector<string> API::getCollName(string tableName)
{
	return cm.getCollName(tableName);
}

//�����ļ����������б�
vector<string> API::getCollType(string tableName)
{
	return cm.getCollType(tableName);
}

//���ݱ����������ļ��еļ�¼����
int API::calcuteLenth(string tableName)
{
	return cm.calcuteLenth(tableName);
}

//����ĳ���������ͣ���������������͵ĳ���
int API::calcuteLenth2(string type)
{
	return cm.calcuteLenth2(type);
}

//ɾ��һ����
void API::dropTable(string tableName)
{
	//�����ֵ���Ϣ������ļ����棬ֱ�ӳ�����ʾ
	if(cm.findFile(tableName)!=TABLE_FILE)
	{
		cout<<"There is no table "<<tableName<<endl;
		return ;
	}
	//ֱ�����ֵ���Ϣ�н���ɾ��
	if(cm.dropTable(tableName))
		cout<<"Drop table "<<tableName<<" successfully"<<endl;
}

//ɾ��һ������
void API::dropIndex(string indexName)
{
	//�����ֵ���Ϣ������������棬ֱ�ӳ�����ʾ
	if(cm.findFile(indexName)!=INDEX_FILE)
	{
		cout<<"There is no index "<<indexName<<endl;
		return ;
	}
	if(cm.dropIndex(indexName))
		cout<<"Drop index "<<indexName<<" successfully"<<endl;
}

//���ݸ����ı����ԣ���������
void API::createIndex(string fileName,string tableName,string colName)
{
	//�����ֵ���Ϣ������������ڣ�ֱ�ӳ�����ʾ
	if(cm.findFile(fileName)==INDEX_FILE)
	{
		cout<<"There is index "<<fileName<<" already"<<endl;
		return ;
	}



	//��IndexManager����������




	//���ֵ���Ϣ����������
	cm.addIndex(fileName,tableName,colName);
	cout<<"Create index "<<fileName<<" successfully"<<endl;
}

//�����С���������Ϣ������
void API::createTable(string tableName,vector<string> col,vector<string> type,vector<int> uniq,string primKey)
{
	//�����ֵ���Ϣ��������Ѿ����ڣ�ֱ�ӳ�����ʾ
	if(cm.findFile(tableName)==TABLE_FILE)
	{
		cout<<"There is table "<<tableName<<" already"<<endl;
		return ;
	}
	if(cm.addTable(tableName,col,type,uniq,primKey))
		cout<<"Create table "<<tableName<<" successfully"<<endl;
}


//�����������������������һ��ֵ�Լ�������¼�������ļ��еĴ�λ�ã������ֵ���뵽�������ļ���
//���е�λ���ǿ�ƫ�ƺͼ�¼�ڿ��е�λ��
int API::insertIndexItem(string fileName,string colName,string value,int block,int index)
{

	return 1;
}

//�����������������Լ�������ϵ�һ��ֵ������������¼�������ļ��ж�Ӧ��λ��
int API::getIndexItem(string fileName,string colName,string value,int * block,int * index)
{
	return 1;
}
