/************************包括MiniSQL中RecordManager类里面的函数实现********************************/

#include "RecordManager.h"
#include "BufferManager.h"
#include "API.h"

#include <iostream>

//宏定义，块大小，逻辑符
#define BLOCK_SIZE 4096
#define AND 11
#define OR 12
class BufferManager;
class API;


//外部变量声明
extern BufferManager bm;
extern API ap;


//根据给定的表名，将表中的记录全部删除
int RecordManager::deleteValue(string tableName)
{
	//查找文件节点
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());
	if(fp!=NULL)
	{
		BufferManager::BlockInfo * bp=fp->blockHead;

		//如果文件节点下面没有块节点挂载，则从文件中读取一个块，并将它挂到文件节点下面
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}
		while(1)
		{
			//循环退出条件：读取块失败
			if(bp==NULL)
				break;
			//将lru加1，表示这个文件块对应的块使用了一次
			bp->lru++;
			//将内存数据全部清0,使用量设为0
			memset(bp->address,0,BLOCK_SIZE);
			bp->usage=0;

			//如果块链表中没有下一个块，读取文件中的下一个块进行挂载
			if(bp->nextBlock==NULL)
				bp=bm.getBlock(tableName.c_str(),bp);
			else
				bp=bp->nextBlock;
		}

	}
	//返回删除的行数目
	return ap.getRecordNum(tableName);
}


//根据给定的表名和一个where条件，删除表中的满足条件的记录
int RecordManager::deleteValue(string tableName, string colName1,string cond1,string operater1)
{
	//得到文件节点
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());
	int deleteNum=0;

	if(fp!=NULL)
	{
		vector<string> collName;
		vector<string> collType;

		//得到表的属性列表和属性类型列表
		collName=ap.getCollName(tableName);
		collType=ap.getCollType(tableName);
		char * p;
		char * tmpp;
		char value[255];
		string type;
		int typeLen;
		int recordLen;
		//计算这个表中的记录长度
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

			//此处每次*P的判断，都是一个字段的开始字节，所以当*P为0时，这个块已经被读完
			while(p-bp->address<bp->usage)
			{
				tmpp=p;    //用tmpp记录每条记录的开始位置
				//对记录中的每个字段进行遍历
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					//找到对应的比较字段，提取其值
					if(collName.at(j)==colName1)
					{
						memcpy(value,p,typeLen);
						break;
					}
					p=p+typeLen;
				}
				//如果字段的值满足比较条件，则将这个字段删除,并将块的usage减去记录长度,同时将p指针指到下一条记录，
				//也就是删除前原来记录的位置
				if(fullFillCond(type,value,cond1,operater1))
				{
					bp->usage=bp->usage-recordLen;
					//将tmpp所指向的一条记录删除，其过程就是将后面的记录前移一个记录长度，最后一条记录清0
					//这样可以保证记录的紧密存存储
					char * ip;
					for(ip=tmpp;ip!=bp->address+bp->usage;ip++)
						*ip=*(ip+recordLen);
					for(int i=0;i<recordLen;i++,ip++)
						*ip=0;
					//将p重表指向这个新的位置
					p=tmpp;
					deleteNum++;
					bp->dirty=1;
				}
				//如果字段的值不满足比较条件，则直接将p指针向前移一个recordLen即可
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

//根据给出的表名和两个where条件以及两个条件之间的逻辑符，删除满足条件的记录
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

		//计算并保存记录长度
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
			bp->lru++;    //将块的lru增加1
			p=bp->address;

			while(p-bp->address<bp->usage)
			{
				tmpp=p;
				//取出第一个where条件对应字段的值，并做条件判断
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


				p=tmpp; //重新置为字段的起始地址
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


				//如果同时满足两个where条件且为and逻辑时，
				//或者满足其中的一个条件且为OR逻辑时，则进行删除操作
				if((logic==AND&&flag1&&flag2)||(logic==OR&&(flag1||flag2)))
				{
					bp->usage=bp->usage-recordLen;
					//将tmpp所指向的一条记录删除，其过程就是将后面的记录前移一个记录长度，最后一条记录清0
					//这样可以保证记录的紧密存存储
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



//通过给定文件名查找所有的表中的记录。
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
		memset(value,0,255);  //将数组置0，否则在输出字符串时没有结尾
		int valueLen;
		string type;

		for(unsigned int i=0;i<collName.size();i++)
			cout<<collName.at(i)<<" ";
		cout<<endl;

		//如果文件节点下面没有块节点挂载，则从文件中读取一个块，并将它挂到文件节点下面
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

					//按照对应的类型，将字段的值输出
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


//通过一个给定的文件名和一个where条件来查找表中的记录。
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

		//将字段名首先输出
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
			bp->lru++;  //块被使用一次

			p=bp->address;

			while((int)(*p)!=0)
			{
				tmp=p;

				//在记录字节中找到对应字段的值
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

				//如果不满足条件，则将指针指向下一条记录
				if(!fullFillCond(type,value,cond1,operater1))
				{
					p=tmp+recordLen;
					continue;
				}
				//如果满足条件，则将这条件记录输出
				selectNum++;                              //将块的lru加一
				p=tmp;                                    //重新指回记录的起始位置
				for(unsigned int j=0;j<collName.size();j++)
				{
					type=collType.at(j);
					typeLen=ap.calcuteLenth2(type);
					memcpy(value,p,typeLen);
					p=p+typeLen;

					//将字节转化为对应的类型值进行输出
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


//通过一个给定的文件名和两个where条件以及一个逻辑操作符来查找表中的记录。
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
				//判断where条件1
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

				//判断where条件2
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


				//如果满足条件
				if((logic==AND&&flag1&&flag2)||(logic==OR&&(flag1||flag2)))
						selectNum++;
				//如果不满足条件，则从下一条件记录开如重新判断
				else
				{
					p=tmp+recordLen;
					continue;
				}


				p=tmp;   //重新指向记录的起始地址
				//根据的数据类型输出记录
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


//根据提供的表名和插入的记录字节，向数据块中插入记录。
int RecordManager::insertRecord(string tableName,char * s)
{

	//得到对应的文件块
	BufferManager::FileInfo * fp=bm.getFile(tableName.c_str());

	int recordLen=ap.calcuteLenth(tableName);
	if(fp!=NULL)
	{
		BufferManager::BlockInfo * bp=fp->blockHead;

		//得到文件对应的第一个块

		//如果文件节点中没有块链表，则读取第一个块，同时将这个块挂到文件节点下面
		if(bp==NULL)
		{
			bp=bm.getBlock(tableName.c_str(),bp);
			fp->blockHead=bp;
		}
		//如果是非空文件
		if(bp!=NULL)
		{
			while(1)
			{
				//对使用的块的lru增加1
				bp->lru++;
				//如果文件块对应的块中还没有写满，则将记录写入这个块尾，即插入到文件中间，而不是末尾
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

				//如果链表中的块遍历完，则要得到替换新的块放入链表,这个所谓新的块是在文件中有对就段的
                //如果链表中的块还没遍历完，直接指向链表中的下一个块
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

		//如果是个空文件，刚添加一个新的空块，并将它挂到文件节点下面
		if(bp==NULL)
		{
			bp=bm.addEmptyBlock(bp);
			fp->blockHead=bp;
		}
		//还没有将记录插入到各个文件段对应的块中，则查找一个空块，并将记录放入空块，也就是将记录插入文件末尾
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


//判断某个值在比较符operater下是否符合条件cond,符合返回1，不符合返回0
int RecordManager::fullFillCond(string type,char * value,string scond,string operater)
{
	int tmp;
	//比较value 和 cond，将value 和 cond的差记为tmp
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

	//通过不同的operater和两者的差，来最后确定是否满足条件
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

	//提示比较出错
	else
	{
		cout<<"Fail to compare values"<<endl;
		return -1;
	}
}
