/****************************此文件包括MiniSQl中BufferManager类的定义 **********************/
/****************************作者：陈矫彦   时间：2009年10月********************************/
#ifndef BufferManager_H
#define BufferManager_H
#include "stdafx.h"
#define MAX_BLOCK_NUM 40       //定义预先分配的文件块的数目
#define MAX_FILE_NUM 5         //定义预先分配的文件节点的数目

class BufferManager{
public:
	//定义文件块信息和存储空间
	struct BlockInfo{
		int offsetNum;         //块对应的文件偏移位置，用于文件的读写
		int dirty;             //脏数据位
		int lock;              //块的锁
		int lru;               //最近使用量，用于LRU算法
		int usage;             //块数据中内存使用点，如果为-1，表示这个块还没有被插入到二维链表
		char * address;        //块数据内存起始地址
		BlockInfo * nextBlock; //指向下一个文件块节点
		BlockInfo * preBlock;  //指向前一个文件块节点
	};
	//定义文件节点存储信息和存储空间
	struct FileInfo{
		int type;              //文件类型
		char * fileName;       //文件名
		int recordNum;         //表文件记录中的记录数目
		int recordLength;      //表文件中的记录长度
		int usage;             //标志有没有被插入到二维链表，没有为-1，有为0
		BlockInfo * blockHead; //指向文件块链表
		FileInfo * nextFile;   //指向下一个文件节点
		FileInfo * preFile;    //指向前一个文件节点
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