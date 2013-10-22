/****************************���ļ�����MiniSQl��BufferManager��Ķ��� **********************/
/****************************���ߣ��½���   ʱ�䣺2009��10��********************************/
#ifndef BufferManager_H
#define BufferManager_H
#include "stdafx.h"
#define MAX_BLOCK_NUM 40       //����Ԥ�ȷ�����ļ������Ŀ
#define MAX_FILE_NUM 5         //����Ԥ�ȷ�����ļ��ڵ����Ŀ

class BufferManager{
public:
	//�����ļ�����Ϣ�ʹ洢�ռ�
	struct BlockInfo{
		int offsetNum;         //���Ӧ���ļ�ƫ��λ�ã������ļ��Ķ�д
		int dirty;             //������λ
		int lock;              //�����
		int lru;               //���ʹ����������LRU�㷨
		int usage;             //���������ڴ�ʹ�õ㣬���Ϊ-1����ʾ����黹û�б����뵽��ά����
		char * address;        //�������ڴ���ʼ��ַ
		BlockInfo * nextBlock; //ָ����һ���ļ���ڵ�
		BlockInfo * preBlock;  //ָ��ǰһ���ļ���ڵ�
	};
	//�����ļ��ڵ�洢��Ϣ�ʹ洢�ռ�
	struct FileInfo{
		int type;              //�ļ�����
		char * fileName;       //�ļ���
		int recordNum;         //���ļ���¼�еļ�¼��Ŀ
		int recordLength;      //���ļ��еļ�¼����
		int usage;             //��־��û�б����뵽��ά����û��Ϊ-1����Ϊ0
		BlockInfo * blockHead; //ָ���ļ�������
		FileInfo * nextFile;   //ָ����һ���ļ��ڵ�
		FileInfo * preFile;    //ָ��ǰһ���ļ��ڵ�
	};
	
	BufferManager();
	~BufferManager();	
	FileInfo * getFile(const char * fileName);
	BlockInfo * getBlock(const char * fileName,BlockInfo * pos);
	BlockInfo * addEmptyBlock(BlockInfo * pos);
	int  flushAll();    
	int  flush(const char * fileName,BlockInfo * b);
private:
	FileInfo * fileHead;
	FileInfo f[MAX_FILE_NUM];
	BlockInfo  b[MAX_BLOCK_NUM];
	int totalBlock;
	int totalFile;
	BlockInfo * findReplaceBlock();
	int findUsage(const char * fileName,const char * address);
};
#endif