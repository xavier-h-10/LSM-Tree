#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "bloomfilter.h"
#include "buffer.h"
#include <map>
#include <string>
#include <vector>
#include <string>
#include <queue>
#include <list>
#define MAX_SIZE 2097152  

using namespace std;

const string deleted="~DELETED~";


class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:

	skiplist *memtable=nullptr; 
	int nowsize=0;  //记录当前内存大小

	int mxlevel=0;  //当前最高层数

	vector< list<Buffer*> > buffer;

	vector<pair<uint64_t, string> > map; //合并所用大表

	string root;   //根目录,通过构造函数传递

	string read_from_table(const string &path, uint32_t l, uint32_t r);

	string find_from_table(uint64_t key); //从table中找数据

	void merge_table(Buffer* p);

	void merge_sort(const vector<pair<uint64_t,string> > &map1);

	void coalesce(); //合并操作

	void write_to_disk(); //table写回磁盘

	void init();  //初始化，读磁盘

public:
	KVStore(const string &dir);

	~KVStore();

	void put(uint64_t key, const string &s) override;

	string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

};
