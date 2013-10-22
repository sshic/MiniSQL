/************************Щ�ļ�����MiniSQL��BufferManager������ĺ���ʵ��************************************/
/************************����:�½���  ʱ�䣺2009��10��*******************************************************/

#include"stdafx.h"
#include "BufferManager.h"
#include "CatalogManager.h"
#include <iostream>
#include <string>
#define BLOCK_SIZE 4096   //����һ���ļ���Ĵ洢��С
#define UNKNOWN_FILE 8    //����δ֪�ļ�����
#define TABLE_FILE 9      //������ļ�����
#define INDEX_FILE 10     //���������ļ�����
using namespace std;
class CatalogManager;

//ȫ�ֱ����ⲿ����
extern CatalogManager cm;

//���캯����Ԥ��Ϊ�ļ��ڵ���ļ���ڵ����ÿռ䣬��ʼ������
BufferManager::BufferManager()
{
	totalBlock=0;
	totalFile=0;
	for(int i=0;i<MAX_BLOCK_NUM;i++)
	{
		//�����ļ������ݴ洢�ռ䣬����0
		b[i].address=new char[BLOCK_SIZE];
		memset(b[i].address,0,BLOCK_SIZE);
		//Ϊ�ļ����ʼ������
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
		//Ϊ�ļ��ڵ��ļ����洢����ռ䣬����ʼ��Ϊ0
		f[i].fileName=new char[126];
		memset(f[i].fileName,0,126);

		//��ʼ���ļ��ڵ����
		f[i].recordNum=-1;
		f[i].recordLength=-1;
		f[i].usage=-1;
		f[i].blockHead=NULL;
		f[i].nextFile=NULL;
		f[i].preFile=NULL;
	}
}

//������������Ԥ�ȷ�����ļ��ڵ���ļ���ռ��ͷ�
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

//�����ļ��ڵ㣬���ҵ��Ļ����·���ʹ�õĽڵ�����ļ��ڵ�����
BufferManager::FileInfo * BufferManager::getFile(const char * fileName)
{
	string s(fileName);
	BufferManager::BlockInfo * bp;
	BufferManager::FileInfo * fp;


//���������е��ļ����в�����Ӧ���ļ���������ҵ��ͷ��أ�����Ҫʹ������ļ�������滻�ļ���
	for(fp=fileHead;fp!=NULL;fp=fp->nextFile)
		if(!strcmp(fp->fileName,fileName))         
			return fp;	              


	if(fp==NULL)
	{
		//���totalFile<MAX_FILE_NUM,ʹ�ÿ�����ļ���
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

			//���ļ��µ��ļ�����뵽�ļ������ͷ��
			//����ļ��������Ѿ��нڵ�
			if(fileHead!=NULL)
			{
				fileHead->preFile=fp;
				fp->nextFile=fileHead;
				fp->preFile=NULL;
				fileHead=fp;
			}
			//����ļ������л�û�нڵ�
			else
			{
				fp->nextFile=NULL;
				fp->preFile=NULL;
				fileHead=fp;
			}
		}
		//���totalFile>=MAX_FILE_NUM,�滻�ļ���
		else
		{
			fp=fileHead;
			memset(fp->fileName,0,126); //���ԭ�����ļ���
			strcpy(fp->fileName,fileName);
			for(bp=fp->blockHead;bp!=NULL;bp++)
			{			
				bp->usage=0;
				//���ļ�����Ŀ�ȫ��д���ļ�
				if(!flush(fileName,bp))
				{
					cout<<"Failed to flush block"<<endl;
					return NULL;
				}
			}
			fp->blockHead=NULL;
			//�����ļ����ڿ������е�λ�ò���
		}
	}

//���ֵ��ļ���Ϣ�ж�����Ϣ�������ļ����ж�Ӧ��������
	int n[3];
	cm.getFileInfo(s,n);
	//�������ļ����Ӧ����һ��table
	if(n[0]==TABLE_FILE)
	{
		fp->type=TABLE_FILE;
		fp->recordNum=n[1];
		fp->recordLength=n[2];
	}
	//�������ļ����Ӧ����һ��index
	else if(n[0]==INDEX_FILE)
	{
		fp->type=INDEX_FILE;
		fp->recordLength=-1;          //����������ļ��������Ǽ�¼���Ⱥͼ�¼��Ŀ
		fp->recordNum=-1;
	}
	//����ֵ���Ϣ�в���������ļ�
	else
	{
		cout<<"File not found"<<endl;
		return NULL;
	}


	return fp;
}


//�����ļ��飬�����ҵ��Ļ����²�����ļ�����뵽ָ�����ļ�������λ��
BufferManager::BlockInfo * BufferManager::getBlock(const char * fileName,BufferManager::BlockInfo * pos)
{
	FILE * fp;
	BufferManager::BlockInfo * bp;

	//���ػ��滻�Ŀ�
	bp=findReplaceBlock();

	//���ÿ����ļ��ж�Ӧ��offsetNum
	if(pos==NULL)
		bp->offsetNum=0;
	else
		bp->offsetNum=pos->offsetNum+1;

	if(bp!=NULL)
	{
		if(fp=fopen(fileName,"rb+"))
		{  
			fseek(fp,bp->offsetNum*BLOCK_SIZE,0);
			//���ļ�����ʱ,����Ƕ���0���飬Ҳ�����ļ�����Ϊ��ʱ���򷵻�NULL
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

	//������ҵĿ������ڱ�ʹ�ù�����ֻҪ��������ԭ���������з���
	if(bp->usage!=-1)
	{
		bp->preBlock->nextBlock=bp->nextBlock;
		bp->nextBlock->preBlock=bp->preBlock;
	}
	else                                      //������ҵĿ�û�б�ʹ��
	{
		totalBlock++;
		bp->usage=0;
	}
	//���µĿ���뵽�µ�������
	//���pos����NULL�Ҳ������һ���ڵ�
	if(pos!=NULL&&pos->nextBlock!=NULL)
	{
		bp->preBlock=pos->nextBlock->preBlock;
		bp->nextBlock=pos->nextBlock;
		pos->nextBlock->preBlock=bp;
		pos->nextBlock=bp;
	}
	//���pos����NULL�������һ���ڵ�
	else if(pos!=NULL&&pos->nextBlock==NULL)
	{
		bp->nextBlock=NULL;
		bp->preBlock=pos;
		pos->nextBlock=bp;
	}
	//���pos��NULL
	else
	{
		bp->preBlock=NULL;
		bp->nextBlock=NULL;
	}

	bp->lru=0;
	//�����ļ����ʹ������������usage
	bp->usage=findUsage(fileName,bp->address);
	
	return bp;
}


//�������������һ���տ飬��ʼʱ����տ鲻��Ӧ�ļ���ĳ��λ��
BufferManager::BlockInfo * BufferManager::addEmptyBlock(BufferManager::BlockInfo * pos)
{
	BufferManager::BlockInfo * bp;

	//�����滻�Ŀ�
	bp=findReplaceBlock();

	//����滻�Ŀ���ʼ�ù��ģ����ԭ��������������
	if(bp->usage!=-1)
	{
		bp->preBlock->nextBlock=bp->nextBlock;
		bp->nextBlock->preBlock=bp->preBlock;
	}
	//����滻�Ŀ���ʹ�ù��ģ������ÿ��usage��ͬʱ��totalBlock��һ
	else
	{
		totalBlock++;
		bp->usage=0;
	}

	//��տ����ݴ洢�ڴ�
	memset(bp->address,0,BLOCK_SIZE);

	//������տ��������ʹ��ʱ�����������ļ��ж�Ӧ��ƫ��
	if(pos!=NULL)
		bp->offsetNum=pos->offsetNum+1;
	else 
		bp->offsetNum=0;
	//��������¿������ֵ
	bp->dirty=0;
	bp->lock=0;
	bp->lru=0;
	bp->usage=0;

	bp->nextBlock=NULL;
	bp->preBlock=NULL;
	
	//����λ�ò��ǿ�ͷ
	if(pos!=NULL)
	{
		bp->preBlock=pos;
		pos->nextBlock=bp;
	}

	return bp;

}


//�����滻�Ŀ飬�����ڿ���Ŀ��в��ң��������Ŀ����ֱ꣬��ʹ��LRU�㷨���滻��
BufferManager::BlockInfo * BufferManager::findReplaceBlock()
{
	BufferManager::FileInfo * fp;
	BufferManager::FileInfo * tmpfp;
	BufferManager::BlockInfo * bp;
	BufferManager::BlockInfo * LRUBlock;

	//�������û�б�ʹ�õ�block����ֱ������ʹ�ã�����һ����ַ�Ϳ�����
	if(totalBlock<MAX_BLOCK_NUM)
	{
		for(int i=totalBlock;i<MAX_BLOCK_NUM;i++)
			if(b[i].usage==-1)
			{
				LRUBlock=&b[i];
				return LRUBlock;
			}
	}

	//��������ڿ����block��ʹ��lru�㷨���п��滻
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

	//�ڲ��ҿ���飬���������ģ���Ҫ�Ƚ���д���ļ�
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

//���ݸ������ļ���������д���ļ�
int BufferManager::flush(const char * fileName,BufferManager::BlockInfo * b)
{
	//���ԭ���Ŀ�û�������ݣ�����д�ļ�����ӷ���0
	if(!b->dirty)
		return 1;
	else
	{
		FILE * fp;
		if(fp=fopen(f->fileName,"rb+"))   //���ļ�
		{  
			fseek(fp,b->offsetNum*BLOCK_SIZE,0);
			if(fwrite(b->address,BLOCK_SIZE,1,fp)!=1)            //���д������򷵻�0
				return 0;
		}
	    else
		{
			cout<<"Fail to flush the block of file "<<fileName<<endl;
			return 0;
		}
		
		b->dirty=0;             //��������λ������Ϊ0
		return 1;
	}
}

//���ڴ��е���������д���ļ���Ҳ���Ƕ�ÿ�����ݿ�д��
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

//��������һ�������Ѿ�����¼ʹ�õ��ֽ���Ŀ
int BufferManager::findUsage(const char * fileName,const char * address)
{
	string s(fileName);
	int recordLen;
	recordLen=cm.calcuteLenth(s);
	const char * p;
	p=address;
	//��һ����¼�ĳ���Ϊ�������Կ����ݽ��б���
	while(p-address<BLOCK_SIZE)
	{
		const char * tmp;
		tmp=p;
		int i;
		//��ÿһ����¼��ÿ���ֽڽ��б�������������з�0Ԫ�أ��˳���
		//���һ����¼û�з�0Ԫ�أ������Ϊ������¼���Ƿ�ʹ����
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
	//����ֹͣ�����ʼַ֮�Ϊ��ʹ���ֽ���
	return p-address;
}

