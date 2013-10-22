/************************����MiniSQL��RecordManager������ĺ���ʵ��********************************/

#include "RecordManager.h"
#include "BufferManager.h"
#include "API.h"

#include <iostream>

//�궨�壬���С���߼���
#define BLOCK_SIZE 4096
#define AND 11
#define OR 12
class BufferManager;
class API;


//�ⲿ��������
extern BufferManager bm;
extern API ap;


//���ݸ����ı����������еļ�¼ȫ��ɾ��
int RecordManager::deleteValue(string tableName)
{
	//�����ļ��ڵ�
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());
	if(fp!=NULL)
	{
		BufferManager::BlockInfo * bp=fp->blockHead;

		//����ļ��ڵ�����û�п�ڵ���أ�����ļ��ж�ȡһ���飬�������ҵ��ļ��ڵ�����
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}
		while(1)
		{
			//ѭ���˳���������ȡ��ʧ��
			if(bp==NULL)
				break;
			//��lru��1����ʾ����ļ����Ӧ�Ŀ�ʹ����һ��
			bp->lru++;
			//���ڴ�����ȫ����0,ʹ������Ϊ0
			memset(bp->address,0,BLOCK_SIZE);
			bp->usage=0;

			//�����������û����һ���飬��ȡ�ļ��е���һ������й���
			if(bp->nextBlock==NULL)
				bp=bm.getBlock(tableName.c_str(),bp);
			else
				bp=bp->nextBlock;
		}

	}
	//����ɾ��������Ŀ
	return ap.getRecordNum(tableName);
}


//���ݸ����ı�����һ��where������ɾ�����е����������ļ�¼
int RecordManager::deleteValue(string tableName, string colName1,string cond1,string operater1)
{
	//�õ��ļ��ڵ�
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());
	int deleteNum=0;

	if(fp!=NULL)
	{
		vector<string> collName;
		vector<string> collType;

		//�õ���������б�����������б�
		collName=ap.getCollName(tableName);
		collType=ap.getCollType(tableName);
		char * p;
		char * tmpp;
		char value[255];
		string type;
		int typeLen;
		int recordLen;
		//����������еļ�¼����
		recordLen=ap.calcuteLenth(tableName);


		BufferManager::BlockInfo * bp=fp->blockHead;
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}

		while(1)
		{
			if(bp==NULL)
				break;
			bp->lru++;
			p=bp->address;

			//�˴�ÿ��*P���жϣ�����һ���ֶεĿ�ʼ�ֽڣ����Ե�*PΪ0ʱ��������Ѿ�������
			while(p-bp->address<bp->usage)
			{
				tmpp=p;    //��tmpp��¼ÿ����¼�Ŀ�ʼλ��
				//�Լ�¼�е�ÿ���ֶν��б���
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					//�ҵ���Ӧ�ıȽ��ֶΣ���ȡ��ֵ
					if(collName.at(j)==colName1)
					{
						memcpy(value,p,typeLen);
						break;
					}
					p=p+typeLen;
				}
				//����ֶε�ֵ����Ƚ�������������ֶ�ɾ��,�������usage��ȥ��¼����,ͬʱ��pָ��ָ����һ����¼��
				//Ҳ����ɾ��ǰԭ����¼��λ��
				if(fullFillCond(type,value,cond1,operater1))
				{
					bp->usage=bp->usage-recordLen;
					//��tmpp��ָ���һ����¼ɾ��������̾��ǽ�����ļ�¼ǰ��һ����¼���ȣ����һ����¼��0
					//�������Ա�֤��¼�Ľ��ܴ�洢
					char * ip;
					for(ip=tmpp;ip!=bp->address+bp->usage;ip++)
						*ip=*(ip+recordLen);
					for(int i=0;i<recordLen;i++,ip++)
						*ip=0;
					//��p�ر�ָ������µ�λ��
					p=tmpp;
					deleteNum++;
					bp->dirty=1;
				}
				//����ֶε�ֵ������Ƚ���������ֱ�ӽ�pָ����ǰ��һ��recordLen����
				else
					p=tmpp+recordLen;
			}

			if(bp->nextBlock==NULL)
				bp=bm.getBlock(tableName.c_str(),bp);
			else
				bp=bp->nextBlock;
		}
	}
	return deleteNum;
}

//���ݸ����ı���������where�����Լ���������֮����߼�����ɾ�����������ļ�¼
int RecordManager::deleteValue(string tableName,string colName1,string cond1,string operater1,
		string colName2,string cond2,string operater2,int logic)
{
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());
	int deleteNum=0;
	if(fp!=NULL)
	{
		vector<string> collName;
		vector<string> collType;
		collName=ap.getCollName(tableName);
		collType=ap.getCollType(tableName);
		int flag1;
		int flag2;
		char * p;
		char * tmpp;
		char value[255];
		string type;
		int typeLen;

		//���㲢�����¼����
		int recordLen;
		recordLen=ap.calcuteLenth(tableName);

		BufferManager::BlockInfo * bp=fp->blockHead;
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}
		while(1)
		{
			if(bp==NULL)
				break;
			bp->lru++;    //�����lru����1
			p=bp->address;

			while(p-bp->address<bp->usage)
			{
				tmpp=p;
				//ȡ����һ��where������Ӧ�ֶε�ֵ�����������ж�
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					if(collName.at(j)==colName1)
					{
						memcpy(value,p,typeLen);
						break;
					}
					p=p+typeLen;
				}
				flag1=fullFillCond(type,value,cond1,operater1);


				p=tmpp; //������Ϊ�ֶε���ʼ��ַ
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					if(collName.at(j)==colName2)
					{
						memcpy(value,p,typeLen);
						break;
					}
					p=p+typeLen;
				}
				flag2=fullFillCond(type,value,cond2,operater2);


				//���ͬʱ��������where������Ϊand�߼�ʱ��
				//�����������е�һ��������ΪOR�߼�ʱ�������ɾ������
				if((logic==AND&&flag1&&flag2)||(logic==OR&&(flag1||flag2)))
				{
					bp->usage=bp->usage-recordLen;
					//��tmpp��ָ���һ����¼ɾ��������̾��ǽ�����ļ�¼ǰ��һ����¼���ȣ����һ����¼��0
					//�������Ա�֤��¼�Ľ��ܴ�洢
					char * ip;
					for(ip=tmpp;ip!=bp->address+bp->usage;ip++)
						*ip=*(ip+recordLen);
					for(int i=0;i<recordLen;i++,ip++)
						*ip=0;

					p=tmpp;
					deleteNum++;
					bp->dirty=1;
				}
				else
					p=tmpp+recordLen;
			}

			if(bp->nextBlock==NULL)
				bp=bm.getBlock(tableName.c_str(),bp);
			else
				bp=bp->nextBlock;
		}
	}
	return deleteNum;
}



//ͨ�������ļ����������еı��еļ�¼��
int RecordManager::selectRecord(string tableName)
{
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());

	if(fp!=NULL)
	{
		int recordLen=ap.calcuteLenth(tableName);
		vector<string> collName=ap.getCollName(tableName);
		vector<string> collType=ap.getCollType(tableName);
		char * p;
		char value[255];
		memset(value,0,255);  //��������0������������ַ���ʱû�н�β
		int valueLen;
		string type;

		for(unsigned int i=0;i<collName.size();i++)
			cout<<collName.at(i)<<" ";
		cout<<endl;

		//����ļ��ڵ�����û�п�ڵ���أ�����ļ��ж�ȡһ���飬�������ҵ��ļ��ڵ�����
		BufferManager::BlockInfo * bp=fp->blockHead;
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}
		while(1)
		{
			if(bp==NULL)
				break;
			bp->lru++;
			p=bp->address;

			while(p-bp->address<bp->usage)
			{
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					valueLen=ap.calcuteLenth2(type);
					memcpy(value,p,valueLen);
					p=p+valueLen;

					//���ն�Ӧ�����ͣ����ֶε�ֵ���
					if(type=="int")
					{
						int * x;
						x=(int *)value;
						cout<<(*x)<<" ";
					}
					else if(type=="float")
					{
						float * x;
						x=(float *)value;
						cout<<(*x)<<" ";
					}
					else
						cout<<value<<" ";
				}
				cout<<endl;
			}

			if(bp->nextBlock==NULL)
				bp=bm.getBlock(tableName.c_str(),bp);
			else
				bp=bp->nextBlock;
		}
		return ap.getRecordNum(tableName);
	}
	else
		return 0;
}


//ͨ��һ���������ļ�����һ��where���������ұ��еļ�¼��
int RecordManager::selectRecord(string tableName,string colName1,string cond1,string operater1)
{
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());

	if(fp!=NULL)
	{
		int selectNum=0;
		int recordLen=ap.calcuteLenth(tableName);
		vector<string> collName=ap.getCollName(tableName);
		vector<string> collType=ap.getCollType(tableName);
		char * p;
		char * tmp;
		int typeLen=0;
		string type;
		char value[255];
		memset(value,0,255);

		//���ֶ����������
		for(unsigned int i=0;i<collName.size();i++)
			cout<<collName.at(i)<<" ";
		cout<<endl;

		BufferManager::BlockInfo * bp=fp->blockHead;
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}
		while(1)
		{
			if(bp==NULL)
				break;
			bp->lru++;  //�鱻ʹ��һ��

			p=bp->address;

			while((int)(*p)!=0)
			{
				tmp=p;

				//�ڼ�¼�ֽ����ҵ���Ӧ�ֶε�ֵ
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					if(collName.at(j)==colName1)
					{
						memcpy(value,p,typeLen);
						break;
					}
					p=p+typeLen;
				}

				//�����������������ָ��ָ����һ����¼
				if(!fullFillCond(type,value,cond1,operater1))
				{
					p=tmp+recordLen;
					continue;
				}
				//�����������������������¼���
				selectNum++;                              //�����lru��һ
				p=tmp;                                    //����ָ�ؼ�¼����ʼλ��
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					memcpy(value,p,typeLen);
					p=p+typeLen;

					//���ֽ�ת��Ϊ��Ӧ������ֵ�������
					if(collType.at(j)=="int")
					{
						int * x;
						x=(int *)value;
						cout<<(*x)<<" ";
					}
					else if(collType.at(j)=="float")
					{
						float * x;
						x=(float *)value;
						cout<<(*x)<<" ";
					}
					else
						cout<<value<<" ";
				}
				cout<<endl;
			}
			if(bp->nextBlock==NULL)
				bp=bm.getBlock(tableName.c_str(),bp);
			else
				bp=bp->nextBlock;
		}
		return selectNum;
	}
	return 0;
}


//ͨ��һ���������ļ���������where�����Լ�һ���߼������������ұ��еļ�¼��
int RecordManager::selectRecord(string tableName,string colName1,string cond1,string operater1,
		string colName2,string cond2,string operater2,int logic)
{
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());
	if(fp!=NULL)
	{
		int selectNum=0;
		int recordLen=ap.calcuteLenth(tableName);
		vector<string> collName=ap.getCollName(tableName);
		vector<string> collType=ap.getCollType(tableName);
		int flag1;
		int flag2;
		char * p;
		char * tmp;
		int typeLen=0;
		string type;
		char value[255];
		memset(value,0,255);

		for(unsigned int i=0;i<collName.size();i++)
			cout<<collName.at(i)<<" ";
		cout<<endl;

		BufferManager::BlockInfo * bp=fp->blockHead;
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}
		while(1)
		{
			if(bp==NULL)
				break;
			bp->lru++;

			p=bp->address;

			while((int)(*p)!=0)
			{
				//�ж�where����1
				tmp=p;
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					if(collName.at(j)==colName1)
					{
						memcpy(value,p,typeLen);
						break;
					}
					p=p+typeLen;
				}
				flag1=fullFillCond(type,value,cond1,operater1);

				//�ж�where����2
				p=tmp;
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					if(collName.at(j)==colName2)
					{
						memcpy(value,p,typeLen);
						break;
					}
					p=p+typeLen;
				}
				flag2=fullFillCond(type,value,cond2,operater2);


				//�����������
				if((logic==AND&&flag1&&flag2)||(logic==OR&&(flag1||flag2)))
						selectNum++;
				//��������������������һ������¼���������ж�
				else
				{
					p=tmp+recordLen;
					continue;
				}


				p=tmp;   //����ָ���¼����ʼ��ַ
				//���ݵ��������������¼
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					memcpy(value,p,typeLen);
					p=p+typeLen;
					if(collType.at(j)=="int")
					{
						int * x;
						x=(int *)value;
						cout<<(*x)<<" ";
					}
					else if(collType.at(j)=="float")
					{
						float * x;
						x=(float *)value;
						cout<<(*x)<<" ";
					}
					else
						cout<<value<<" ";
				}
				cout<<endl;
			}
			if(bp->nextBlock==NULL)
				bp=bm.getBlock(tableName.c_str(),bp);
			else
				bp=bp->nextBlock;
		}
		return selectNum;
	}
	return 0;
}


//�����ṩ�ı����Ͳ���ļ�¼�ֽڣ������ݿ��в����¼��
int RecordManager::insertRecord(string tableName,char * s)
{

	//�õ���Ӧ���ļ���
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());

	int recordLen=ap.calcuteLenth(tableName);
	if(fp!=NULL)
	{
		BufferManager::BlockInfo * bp=fp->blockHead;

		//�õ��ļ���Ӧ�ĵ�һ����

		//����ļ��ڵ���û�п��������ȡ��һ���飬ͬʱ�������ҵ��ļ��ڵ�����
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}
		//����Ƿǿ��ļ�
		if(bp!=NULL)
		{
			while(1)
			{
				//��ʹ�õĿ��lru����1
				bp->lru++;
				//����ļ����Ӧ�Ŀ��л�û��д�����򽫼�¼д�������β�������뵽�ļ��м䣬������ĩβ
				if(bp->usage<=4096-recordLen)
				{
					char * p;
					p=bp->address;
						p=p+bp->usage;
					memcpy(p,s,recordLen);
					bp->usage=bp->usage+recordLen;
					bp->dirty=1;
					bp->lru++;
					cout<<"Add a record to table"<<endl;
					return 1;
				}

				//��������еĿ�����꣬��Ҫ�õ��滻�µĿ��������,�����ν�µĿ������ļ����жԾͶε�
                //��������еĿ黹û�����ֱ꣬��ָ�������е���һ����
				if(bp->nextBlock==NULL)
				{
					if(bm.getBlock(tableName.c_str(),bp)!=NULL)
						bp=bm.getBlock(tableName.c_str(),bp);
					else
						break;
				}
				else
					bp=bp->nextBlock;
			}
		}

		//����Ǹ����ļ��������һ���µĿտ飬�������ҵ��ļ��ڵ�����
		if(bp==NULL)
		{
			bp=bm.addEmptyBlock(bp);
			fp->blockHead=bp;
		}
		//��û�н���¼���뵽�����ļ��ζ�Ӧ�Ŀ��У������һ���տ飬������¼����տ飬Ҳ���ǽ���¼�����ļ�ĩβ
		else
			bp=bm.addEmptyBlock(bp);
		memcpy(bp->address,s,recordLen);
		bp->usage=bp->usage+recordLen;
		bp->lru++;
		bp->dirty=1;
		cout<<"Add a record to table"<<endl;
		return 1;
	}
	else
	{
		cout<<"Fail to find file block of "<<tableName<<endl;
		return 0;
	}
}


//�ж�ĳ��ֵ�ڱȽϷ�operater���Ƿ��������cond,���Ϸ���1�������Ϸ���0
int RecordManager::fullFillCond(string type,char * value,string scond,string operater)
{
	int tmp;
	//�Ƚ�value �� cond����value �� cond�Ĳ��Ϊtmp
	const char * cond;
	cond=scond.c_str();
	if(type=="int")
	{
		int * v;
		v=(int *)value;
		int cv;
		cv=atoi(cond);
		tmp=(*v)-cv;
	}

	else if(type=="float")
	{
		float * v;
		v=(float *)value;
		float cv;
		cv=(float)atof(cond);


		if((*v)<cv)
			tmp=-1;
		else if((*v)==cv)
			tmp=0;
		else
			tmp=1;
	}
	else
	{
		tmp=strcmp(value,cond);
	}

	//ͨ����ͬ��operater�����ߵĲ�����ȷ���Ƿ���������
	if(operater=="<")
	{
		if(tmp<0)
			return 1;
		else
			return 0;
	}
	else if(operater=="<=")
	{
		if(tmp<=0)
			return 1;
		else
			return 0;
	}
	else if(operater==">")
	{
		if(tmp>0)
			return 1;
		else
			return 0;
	}
	else if(operater==">=")
	{
		if(tmp>=0)
			return 1;
		else
			return 0;
	}
	else if(operater=="=")
	{
		if(tmp==0)
			return 1;
		else
			return 0;
	}

	//��ʾ�Ƚϳ���
	else
	{
		cout<<"Fail to compare values"<<endl;
		return -1;
	}
}
