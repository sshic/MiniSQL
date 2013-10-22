/************************些文件包括MiniSQL中BufferManager类里面的函数实现************************************/
/************************作者:陈矫彦  时间：2009年10月*******************************************************/

#include"stdafx.h"
#include "BufferManager.h"
#include "CatalogManager.h"
#include <iostream>
#include <string>
#define BLOCK_SIZE 4096   //定义一个文件块的存储大小
#define UNKNOWN_FILE 8    //定义未知文件类型
#define TABLE_FILE 9      //定义表文件类型
#define INDEX_FILE 10     //定义索引文件类型
using namespace std;
class CatalogManager;

//全局变量外部声明
extern CatalogManager cm;

//构造函数，预先为文件节点和文件块节点分配好空间，初始化变量
BufferManager::BufferManager()
{
	totalBlock=0;
	totalFile=0;
	for(int i=0;i<MAX_BLOCK_NUM;i++)
	{
		//分配文件块数据存储空间，并置0
		b[i].address=new char[BLOCK_SIZE];
		memset(b[i].address,0,BLOCK_SIZE);
		//为文件块初始化变量
		b[i].offsetNum=-1;
		b[i].dirty=0;
		b[i].lock=0;
		b[i].lru=0;
		b[i].usage=-1;
		b[i].nextBlock=NULL;
		b[i].preBlock=NULL;
	}
	for(int i=0;i<MAX_FILE_NUM;i++)
	{
		f[i].type=-1;
		//为文件节点文件名存储分配空间，并初始化为0
		f[i].fileName=new char[126];
		memset(f[i].fileName,0,126);

		//初始化文件节点变量
		f[i].recordNum=-1;
		f[i].recordLength=-1;
		f[i].usage=-1;
		f[i].blockHead=NULL;
		f[i].nextFile=NULL;
		f[i].preFile=NULL;
	}
}

//析构函数，把预先分配的文件节点和文件块空间释放
BufferManager::~BufferManager()
{
	for(int i=0;i<MAX_BLOCK_NUM;i++)
	{
		delete [] b[i].address;
	}
	for(int i=0;i<MAX_FILE_NUM;i++)
	{
		delete [] f[i].fileName;
	}

}

//查找文件节点，将找到的或者新分配使用的节点插入文件节点链表
BufferManager::FileInfo * BufferManager::getFile(const char * fileName)
{
	string s(fileName);
	BufferManager::BlockInfo * bp;
	BufferManager::FileInfo * fp;


//首先在已有的文件块中查找相应的文件名，如果找到就返回，否则要使空余的文件块或者替换文件块
	for(fp=fileHead;fp!=NULL;fp=fp->nextFile)
		if(!strcmp(fp->fileName,fileName))         
			return fp;	              


	if(fp==NULL)
	{
		//如果totalFile<MAX_FILE_NUM,使用空余的文件块
		if(totalFile<MAX_FILE_NUM)
		{
			for(int i=totalFile;i<MAX_FILE_NUM;i++)
				if(f[i].usage==-1)
				{
					fp=&f[i];
					break;
				}
			totalFile++;
			fp->usage=0;
			strcpy(fp->fileName,fileName);

			//将文件新的文件块插入到文件链表的头部
			//如果文件链表中已经有节点
			if(fileHead!=NULL)
			{
				fileHead->preFile=fp;
				fp->nextFile=fileHead;
				fp->preFile=NULL;
				fileHead=fp;
			}
			//如果文件链表中还没有节点
			else
			{
				fp->nextFile=NULL;
				fp->preFile=NULL;
				fileHead=fp;
			}
		}
		//如果totalFile>=MAX_FILE_NUM,替换文件块
		else
		{
			fp=fileHead;
			memset(fp->fileName,0,126); //清空原来的文件名
			strcpy(fp->fileName,fileName);
			for(bp=fp->blockHead;bp!=NULL;bp++)
			{			
				bp->usage=0;
				//将文件下面的块全部写回文件
				if(!flush(fileName,bp))
				{
					cout<<"Failed to flush block"<<endl;
					return NULL;
				}
			}
			fp->blockHead=NULL;
			//保留文件块在块链表中的位置不变
		}
	}

//从字典文件信息中读入信息，放入文件块中对应的属性中
	int n[3];
	cm.getFileInfo(s,n);
	//如果这个文件块对应的是一个table
	if(n[0]==TABLE_FILE)
	{
		fp->type=TABLE_FILE;
		fp->recordNum=n[1];
		fp->recordLength=n[2];
	}
	//如果这个文件块对应的是一个index
	else if(n[0]==INDEX_FILE)
	{
		fp->type=INDEX_FILE;
		fp->recordLength=-1;          //如果是索引文件，不考虑记录长度和记录数目
		fp->recordNum=-1;
	}
	//如果字典信息中不存在这个文件
	else
	{
		cout<<"File not found"<<endl;
		return NULL;
	}


	return fp;
}


//查找文件块，将查找到的或者新插入的文件块放入到指定的文件块链表位置
BufferManager::BlockInfo * BufferManager::getBlock(const char * fileName,BufferManager::BlockInfo * pos)
{
	FILE * fp;
	BufferManager::BlockInfo * bp;

	//返回回替换的块
	bp=findReplaceBlock();

	//设置块在文件中对应的offsetNum
	if(pos==NULL)
		bp->offsetNum=0;
	else
		bp->offsetNum=pos->offsetNum+1;

	if(bp!=NULL)
	{
		if(fp=fopen(fileName,"rb+"))
		{  
			fseek(fp,bp->offsetNum*BLOCK_SIZE,0);
			//当文件读完时,如果是读了0个块，也就是文件本来为空时，则返回NULL
			if(fread(bp->address,1,BLOCK_SIZE,fp)==0)   
			{
				fclose(fp);
				bp->offsetNum=0;
				return NULL;
			}
			fclose(fp);
			
		}
		else
		{
			cout<<"Fail to open the file "<<fileName<<endl;
			return NULL;
		}
	
	}
	else
		return NULL;

	//如果查找的块是正在被使用过，则只要将这个块从原来的链表中分离
	if(bp->usage!=-1)
	{
		bp->preBlock->nextBlock=bp->nextBlock;
		bp->nextBlock->preBlock=bp->preBlock;
	}
	else                                      //如果查找的块没有被使用
	{
		totalBlock++;
		bp->usage=0;
	}
	//将新的块插入到新的链表中
	//如果pos不是NULL且不是最后一个节点
	if(pos!=NULL&&pos->nextBlock!=NULL)
	{
		bp->preBlock=pos->nextBlock->preBlock;
		bp->nextBlock=pos->nextBlock;
		pos->nextBlock->preBlock=bp;
		pos->nextBlock=bp;
	}
	//如果pos不是NULL且是最后一个节点
	else if(pos!=NULL&&pos->nextBlock==NULL)
	{
		bp->nextBlock=NULL;
		bp->preBlock=pos;
		pos->nextBlock=bp;
	}
	//如果pos是NULL
	else
	{
		bp->preBlock=NULL;
		bp->nextBlock=NULL;
	}

	bp->lru=0;
	//查找文件块的使用量，并赋给usage
	bp->usage=findUsage(fileName,bp->address);
	
	return bp;
}


//向块链表中增加一个空块，开始时这个空块不对应文件中某个位置
BufferManager::BlockInfo * BufferManager::addEmptyBlock(BufferManager::BlockInfo * pos)
{
	BufferManager::BlockInfo * bp;

	//返回替换的块
	bp=findReplaceBlock();

	//如果替换的块是始用过的，则从原来的链表中脱离
	if(bp->usage!=-1)
	{
		bp->preBlock->nextBlock=bp->nextBlock;
		bp->nextBlock->preBlock=bp->preBlock;
	}
	//如果替换的块是使用过的，则设置块的usage，同时将totalBlock加一
	else
	{
		totalBlock++;
		bp->usage=0;
	}

	//清空块数据存储内存
	memset(bp->address,0,BLOCK_SIZE);

	//将这个空块放入链表使用时，设置它在文件中对应的偏移
	if(pos!=NULL)
		bp->offsetNum=pos->offsetNum+1;
	else 
		bp->offsetNum=0;
	//设置这个新块的属性值
	bp->dirty=0;
	bp->lock=0;
	bp->lru=0;
	bp->usage=0;

	bp->nextBlock=NULL;
	bp->preBlock=NULL;
	
	//插入位置不是块头
	if(pos!=NULL)
	{
		bp->preBlock=pos;
		pos->nextBlock=bp;
	}

	return bp;

}


//查找替换的块，首先在空余的块中查找，如果空余的块用完，直接使用LRU算法来替换块
BufferManager::BlockInfo * BufferManager::findReplaceBlock()
{
	BufferManager::FileInfo * fp;
	BufferManager::FileInfo * tmpfp;
	BufferManager::BlockInfo * bp;
	BufferManager::BlockInfo * LRUBlock;

	//如果还有没有被使用的block，则直接拿来使用，返回一个地址就可以了
	if(totalBlock<MAX_BLOCK_NUM)
	{
		for(int i=totalBlock;i<MAX_BLOCK_NUM;i++)
			if(b[i].usage==-1)
			{
				LRUBlock=&b[i];
				return LRUBlock;
			}
	}

	//如果不存在空余的block，使用lru算法进行块替换
	long flag=100000;

	for(fp=fileHead;fp!=NULL;fp=fp->nextFile)
		for(bp=fp->blockHead;bp!=NULL;bp=bp->nextBlock)
		{
			if(bp->lru<flag&&bp->lock!=1){
				flag=bp->lru;
				LRUBlock=bp;
				tmpfp=fp;
			}
		}

	//在查找空余块，如果块是脏的，则要先将它写回文件
	if(LRUBlock->dirty)                         
	{
		if(!flush(tmpfp->fileName,LRUBlock))
		{	
			cout<<"Fail to flush the block"<<endl;
			return NULL;
		}
		LRUBlock->dirty=0;
	}
	return LRUBlock;	
}

//根据给定的文件名，将块写回文件
int BufferManager::flush(const char * fileName,BufferManager::BlockInfo * b)
{
	//如果原来的块没有脏数据，则不用写文件，真接返回0
	if(!b->dirty)
		return 1;
	else
	{
		FILE * fp;
		if(fp=fopen(f->fileName,"rb+"))   //打开文件
		{  
			fseek(fp,b->offsetNum*BLOCK_SIZE,0);
			if(fwrite(b->address,BLOCK_SIZE,1,fp)!=1)            //如果写入出错，则返回0
				return 0;
		}
	    else
		{
			cout<<"Fail to flush the block of file "<<fileName<<endl;
			return 0;
		}
		
		b->dirty=0;             //将脏数据位重新设为0
		return 1;
	}
}

//将内存中的所有数据写回文件，也就是对每个数据块写回
int BufferManager::flushAll()
{
	BufferManager::BlockInfo * bp;
	BufferManager::FileInfo * fp;
	for(fp=fileHead;fp!=NULL;fp=fp->nextFile)
	{
		string s(fp->fileName);
		if(cm.findFile(s)!=UNKNOWN_FILE)
		{
			for(bp=fp->blockHead;bp!=NULL;bp=bp->nextBlock)
				if(flush(fp->fileName,bp)==0)
					return 0;
		}

	}
	return 1;
}

//用来查找一个块中已经被记录使用的字节数目
int BufferManager::findUsage(const char * fileName,const char * address)
{
	string s(fileName);
	int recordLen;
	recordLen=cm.calcuteLenth(s);
	const char * p;
	p=address;
	//以一条记录的长度为步长，对块内容进行遍历
	while(p-address<BLOCK_SIZE)
	{
		const char * tmp;
		tmp=p;
		int i;
		//对每一条记录的每个字节进行遍历，如果发现有非0元素，退出，
		//如果一条记录没有非0元素，则可以为这条记录区是非使用区
		for(i=0;i<recordLen;i++)
		{
			if(*tmp!=0)
				break;
			tmp++;
		}
		if(i==recordLen)
				break;

		p=p+recordLen;
	}
	//遍历停止点和起始址之差即为块使用字节数
	return p-address;
}

