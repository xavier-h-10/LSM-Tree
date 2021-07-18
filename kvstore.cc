#include "kvstore.h"
#include "skiplist.h"
#include "bloomfilter.h"
#include "utils.h"
#include <string>
#include <map>
#include <fstream>
#include <cstdio>
#include <algorithm>

using namespace std;

KVStore::KVStore(const string &dir): KVStoreAPI(dir)
{
	root=dir;
	memtable=new skiplist(); //初始化跳表
	buffer.clear();
	if(!utils::dirExists(dir))
	{
		utils::mkdir(dir);
	}
	init();
}

void KVStore::init()
{
	string path="";
	vector<string> ret;
	vector<string>::iterator it;
	nowsize=10272;  //初始化大小 header+bloom filter
	while(true)
	{
		path=root+"/level-"+to_string(mxlevel);
		//////cout<<"init:path="<<path<<endl;
		if(utils::dirExists(path))  //存在当前level
		{
			ret.clear();
			utils::scanDir(path,ret);
			it=ret.begin();
			while(it!=ret.end())
			{
				if(!utils::isSST(*it)) //不是table文件,删除
				{
					it=ret.erase(it);
				}
				else
				{
					++it;
				}
			}
			sort(ret.rbegin(),ret.rend());
			list<Buffer*> tmp1;
			tmp1.clear();
			for(int i=0;i<ret.size();i++)
			{
				string filename=path+"/"+ret[i];
				fstream in(filename, ios::binary | ios::in);
				if(!in.is_open())
				{
					//////cout<<"can not open SST file:"+filename<<endl;
					return;
				}

				//////cout<<"filename="<<filename<<endl;

				Buffer* tmp=new Buffer;
				in.read((char*)(&tmp->time),sizeof(tmp->time));    // 开始读文件
				in.read((char*)(&tmp->size),sizeof(tmp->size));
				in.read((char*)(&tmp->mn),sizeof(tmp->mn));
				in.read((char*)(&tmp->mx),sizeof(tmp->mx));
				in.read((char*)(&tmp->filter->array),sizeof(tmp->filter->array)); //读bloom fliter

				//////cout<<"tmp.time="<<tmp->time<<endl;
				//////cout<<"size="<<tmp->size<<endl;
				//////cout<<"mn="<<tmp->mn<<endl;
				//////cout<<"mx="<<tmp->mx<<endl;

				uint64_t p;
				uint32_t q;
				for(int i=0;i<tmp->size;i++)
				{
					in.read((char*)(&p),sizeof(p));
					in.read((char*)(&q),sizeof(q));
					tmp->index.emplace_back(make_pair(p,q));
					//////cout<<"p="<<p<<" "<<"q="<<q<<endl;
				}

				in.close();
				tmp->path=filename;   //buffer中存路径，方便再次打开table
				tmp1.emplace_back(tmp);
				//////cout<<"read file "<<filename<<" success"<<endl;
				//delete tmp;
				//tmp=nullptr;
			}
			mxlevel++; //找下一层
			buffer.emplace_back(tmp1);
		}
		else  //没找到当前level,退出循环
		{
			mxlevel--;
			break;
		}
	}
	if(mxlevel<0) mxlevel=0;
}



KVStore::~KVStore()
{
	write_to_disk();


	/* 回收buffer */
	int size=buffer.size();
	list<Buffer*>::iterator it;
	for(int i=0;i<size;i++)
	{
		for(it=buffer[i].begin();it!=buffer[i].end();++it)
		{
			Buffer* p=*it;
			delete p->filter;
			p->filter=nullptr;
			delete p;
			p=nullptr;
		}
	}

}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const string &s)
{
	// if(key==9994)
	// {
	// 	//cout<<"put called:"<<key<<" "<<s<<endl;
	// }

	if(nowsize+s.length()+13>MAX_SIZE)  //key:8B  offset:4B 
	{
		// if(s==deleted)
		// {
		// 	//cout<<"deleted->disk"<<endl;
		// }
		write_to_disk();
	}
	memtable->add(key,s);
	nowsize=nowsize+s.length()+13;
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
string KVStore::get(uint64_t key)
{
	string str=memtable->find(key);
	// if(key==2) //cout<<"get 2"<<str<<endl;
	if(str==deleted) return "";        //删除标记，表示未找到
	if(str!="") return str;             //memtable找到结果，直接返回
	str=find_from_table(key);
	if(str=="" || str==deleted) return "";
	return str;
}

string KVStore::find_from_table(uint64_t key)
{
	if(buffer.size()==0) return "";  //没有buffer
	////cout<<"size="<<buffer[0].size();
	list<Buffer*>::iterator it;
	////cout<<"mxlevel="<<mxlevel<<endl;
	for(int i=0;i<=mxlevel;i++)
	{
		////cout<<"hello 1"<<endl;
		////cout<<"size="<<buffer[0].size();
		for(it=buffer[i].begin();it!=buffer[i].end();++it)
		{
			Buffer* p=*it;
			//if(key==2)//cout<<"hello"<<endl;
			//if(key==2)//cout<<p->size<<" "<<p->mn<<" "<<p->mx<<" "<<endl;
			////cout<<p->filter->search(key)<<endl;
			if(p->mn<=key && key<=p->mx && p->filter->search(key)) // 满足这些条件，开始二分查找
			{
				//if(key==2) //cout<<"search:"<<" "<<p->mn<<" "<<p->mx<<" "<<p->path<<endl;
				int l=0,r=p->index.size()-1;
				////cout<<"l="<<l<<" "<<"r="<<r<<endl;
				while(l<=r)
				{
					int mid=(l+r)>>1;
					////cout<<"mid="<<mid<<" key="<<p->index[mid].first<<endl;
					if(p->index[mid].first==key)
					{
						if(mid==(p->index.size()-1))    // 最后一个value,文件直接读完
						{
							return read_from_table(p->path,p->index[mid].second,-1);
						}
						else                             // 从这个offset读到下一个offset-1
						{
							return read_from_table(p->path,p->index[mid].second,p->index[mid+1].second-1);
						}
					}
					if(p->index[mid].first>key) r=mid-1;
					else l=mid+1;
				}
			}
		}
	}
	return "";
}

string KVStore::read_from_table(const string &path, uint32_t l, uint32_t r)
{
	fstream in(path, ios::binary | ios::in);
	if(!in.is_open())
	{
		//cout<<"can not open SST file: "+path<<endl;
		return "";
	}

    char tmp;
    string ans="";
    if(!in.seekg(l,ios::beg))
    {
    	//cout<<"can not read data from "+path<<endl;
    	return "";
    }
	if(r!=-1)  
	{
		for(uint32_t i=l;i<r;++i)
		{
			in.read((char*)(&tmp),sizeof(tmp));   
			ans+=tmp;
		}

	}
	else
	{
		in.seekg(0,ios::end);
		int len=in.tellg();
		in.seekg(l,ios::beg);
		for(uint32_t i=l;i<len-1;++i)
		{
			in.read((char*)(&tmp),sizeof(tmp));
			ans+=tmp;
		}
		in.read((char*)(&tmp),sizeof(tmp));   
		if(tmp!=' ' && tmp!='\0') ans+=tmp;
	}
	in.close();
	return ans;
}


/**
 * Delete the given key-value pair if it exists.
 * Returns false if the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	string ans=get(key);

	put(key,deleted);
	if(ans=="")
	{
		return false;
	}
	else
	{
		return true;
	}
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	memtable->clear();    // 删内存
	int size=buffer.size();
	for(int i=0;i<size;i++)     //reset的时候要把buffer以及里面的指针都删去
	{
		while(!buffer[i].empty())
		{
			Buffer* p=buffer[i].front();
			delete p;
			p=nullptr;
			buffer[i].pop_front();
		}
	}
	buffer.clear();


	int tmp_level=0;      //删除磁盘中所有table
	vector<string> ret;
	while(true)
	{
		string path=root+"/level-"+to_string(tmp_level);
		////cout<<path<<endl;
		if(utils::dirExists(path))                   //先删文件,再删空目录
		{
			utils::scanDir(path,ret);
			for(int i=0;i<ret.size();i++)
			{
				utils::rmfile(path+"/"+ret[i]);
			}
			utils::rmdir(path);
		}
		else
		{
			break;
		}
		tmp_level++;
	}

}


void KVStore::write_to_disk()
{
	//cout<<"write_to_disk called"<<endl;
	Node *head=memtable->head;
	if(head==nullptr) return; //没有数据，直接返回
	//遍历memtable并计算offset
	vector< pair<uint64_t,string> > v;  //存memtable的数据
	vector<uint32_t> offset; //记录每个key对应的offset
	uint32_t now=0;
	bloomfilter *filter=new bloomfilter();   //临时filter
	while(head->down)
	{
		head=head->down;
	}
	head=head->right;   // 最底层跳表最左边为-1 不要读进去
	if(head==nullptr)
	{
		return;   // 跳表中没有数据，直接返回
	}

	string path=root+"/level-0";
	if(!utils::dirExists(path))
	{
		utils::mkdir(path);
	}
	uint64_t time=utils::get_time();
	string filename=path+"/"+to_string(time)+".sst";
	//cout<<"write to disk:"<<filename<<endl;
	fstream out(filename,ios::binary | ios::out);
	if(!out.is_open())
	{
		//cout<<"write_to_disk error: can not open file"+filename<<endl;
		return;
	}

	while(head)
	{
		uint64_t key=head->key;
		string val=head->val;
		//cout<<"key="<<key<<"val="<<val<<endl;
		v.emplace_back(make_pair(key,val));
		offset.emplace_back(now);
		now+=val.length()+1;
		head=head->right;
		filter->insert(key);   // 生成bloom filter
	}
	uint64_t size=v.size();
	uint64_t mn=v[0].first;
	uint64_t mx=v[size-1].first;

	//输出head
	out.write((char*)(&time),sizeof(time));
	out.write((char*)(&size),sizeof(size));
	out.write((char*)(&mn),sizeof(mn));
	out.write((char*)(&mx),sizeof(mx));

	//输出bloom filter
	out.write((char*)(&filter->array),sizeof(filter->array));

	Buffer *tmp=new Buffer;

	//输出索引区
	uint32_t offset_0=10272+12*size;  //value1的基准偏移量
	for(int i=0;i<size;i++)
	{
		offset[i]+=offset_0;
		out.write((char*)(&v[i].first),sizeof(v[i].first));
		out.write((char*)(&(offset[i])),sizeof(offset[i]));
		tmp->index.emplace_back(make_pair(v[i].first,offset[i]));
	}

	//输出数据区
	for(int i=0;i<size;i++)
	{
		out.write((char*)v[i].second.c_str(),v[i].second.length()+1);
	}

	out.close();

	//清空memtable
	memtable->clear();  //清空memtable
	nowsize=10272;      //重置内存大小

	/* 将写回disk的table缓存到内存中 */
	tmp->time=time;
	tmp->size=size;
	tmp->mn=mn;
	tmp->mx=mx;
	tmp->path=filename;
	tmp->filter=filter;
	if(buffer.size()==0) buffer.emplace_back(list<Buffer*>());
	buffer[0].emplace_front(tmp);
	////cout<<"emplace_front"<<" "<<tmp->path<<endl;
	coalesce();    //进行合并操作
}

void KVStore::coalesce()
{
	////cout<<"coalesce called"<<endl;
	if(buffer.size()==0) return;
	if(buffer[0].size()<=2) return;  //排除不需要合并的情况
	//cout<<"coalesce buffer[0].size:"<<buffer[0].size()<<endl;
	list<Buffer*>::iterator it;
	it=buffer[0].begin();

	Buffer* p=*it;
	uint64_t mn=p->mn;
	uint64_t mx=p->mx;
	++it;
	//cout<<"need to coalesce nowsize="<<nowsize<<endl;
	for(;it!=buffer[0].end();++it)  //统计覆盖键的区间
	{
		p=*it;
		mn=min(p->mn,mn);
		mx=max(p->mx,mx);
	}

	if(buffer.size()==1)   //没有level-1,插入空层
	{
		buffer.emplace_back(list<Buffer*>()); 
	}

	map.clear();

	vector<Buffer*>tmp2;  //需要合并的
	list<Buffer*> tmp3;  //新的level-1
	tmp2.clear();
	tmp3.clear();
	for(it=buffer[1].begin();it!=buffer[1].end();it++)
	{
		p=*it;
		if(!((p->mx<mn) || (p->mn>mx)))
		{
			tmp2.emplace_back(p);
		}
		else
		{
			tmp3.emplace_front(p);
		}
	}

	int tmp_size=tmp2.size();
	for(int i=0;i<tmp_size;i++)
	{
		p=tmp2[i];
		//cout<<"merge_table filename="<<p->path<<endl;
		merge_table(p);
		delete p;
		p=nullptr;
	}

	buffer[1].clear();
	buffer[1]=tmp3;

	// for(it=buffer[1].begin();it!=buffer[1].end();)
	// {
	// 	p=*it;
	// 	if(!((p->mx<mn) || (p->mn>mx)))    // level-1中相交的,进行merge
	// 	{
	// 		cout<<"merge_table filename="<<p->path<<endl;
	// 		merge_table(p);
	// 		delete p;
	// 		p=nullptr;
	// 		buffer[1].erase(it);
	// 	}
	// 	else
	// 	{
	// 		++it;
	// 	}
	// }

	while(!buffer[0].empty())   //必须从后往前遍历,后面较旧
	{
		p=buffer[0].back();
		merge_table(p);
		delete p;
		p=nullptr;
		buffer[0].pop_back();

	}

	string dir=root+"/level-1";
	if(!utils::dirExists(dir))          //不存在level-1目录,先生成
	{
		utils::mkdir(dir);
		buffer.emplace_back(list<Buffer*>());   //生成level-1的buffer
		mxlevel=1;
	}

	nowsize=10272;                        //读到所有数据，开始生成table
	int t=map.size();
	uint64_t key=0;
	string val="";
	Buffer *tmp=new Buffer;
	int last=-1;
	vector<uint32_t> offset;
	uint32_t now=0;
	for(int i=0;i<=t;i++)
	{
		if(nowsize+val.length()+13>MAX_SIZE || (i==t && nowsize!=10272))      
		{
			tmp->time=utils::get_time();
			tmp->mn=map[last+1].first;
			tmp->mx=map[i-1].first;
			tmp->size=i-last-1;
			tmp->path=root+"/level-1/"+to_string(tmp->time)+".sst"; //输出到level-1中

		//	cout<<"merge mn="<<tmp->mn<<" mx="<<tmp->mx<<" mn="<<map[last+1].second<<" mx="<<map[i-1].second<<endl;
			fstream out(tmp->path,ios::binary | ios::out);
			if(!out.is_open())
			{
				return;
			}

			//输出head
			out.write((char*)(&tmp->time),sizeof(tmp->time));
			out.write((char*)(&tmp->size),sizeof(tmp->size));
			out.write((char*)(&tmp->mn),sizeof(tmp->mn));
			out.write((char*)(&tmp->mx),sizeof(tmp->mx));

			//cout<<"merge table write head"<<endl;

			//输出bloom filter
			out.write((char*)(&tmp->filter->array),sizeof(tmp->filter->array));

			//cout<<"merge table write bloom filter"<<endl;

			//输出索引区
			uint32_t offset_0=10272+12*tmp->size;  //value1的基准偏移量
			for(int j=0;j<tmp->size;j++)
			{
				//cout<<"merge table write index:"<<j+last+1<<endl;
				out.write((char*)(&map[j+last+1].first),sizeof(map[j+last+1].first));
				out.write((char*)(&(offset_0)),sizeof(offset_0));
				tmp->index.push_back(make_pair(map[j+last+1].first,offset_0));
				offset_0+=map[j+last+1].second.length()+1;
			//	if(j!=0) offset_0++;
			}

			//cout<<"merge table write offset"<<endl;

			//输出数据区
			for(int j=0;j<tmp->size;j++)
			{
				out.write((char*)map[j+last+1].second.c_str(),map[j+last+1].second.length()+1);
			//cout<<map[j+last+1].second.length()<<endl;
			}
			//cout<<"merge table write data"<<endl;

			out.close();

			buffer[1].emplace_front(tmp);  //插入新的buffer

			tmp=new Buffer;       // 分配新的buffer, bloom filter

			//cout<<"new tmp"<<endl;
			nowsize=10272;
			last=i-1;
			if(i==t) break;
		}
	//	int key1=key;
		int key=map[i].first;
	//	int test=val.length();
		string val=map[i].second;
		int val1=val.length();
		nowsize=nowsize+val.length()+13;
		//cout<<"merge table write nowsize="<<nowsize<<" i="<<i<<endl;
		offset.push_back(now);
		now+=val.length()+1;
		tmp->filter->insert(key);
	}

	int mx_num=4;  // level-1 最多放四个
	int now_level=1;
	int res;
	string new_path,old_path;
	while(buffer[now_level].size()>mx_num)  //数量超出最大限制，放到下面一层
	{
		res=buffer[now_level].size()-mx_num;  //放到下面一层的个数
		new_path=root+"/level-"+to_string(now_level+1);
		if(!utils::dirExists(new_path))
		{
			utils::mkdir(new_path);
			buffer.emplace_back(list<Buffer*>());
			mxlevel=max(mxlevel,now_level+1);
		}
		for(int i=1;i<=res;i++)
		{
			Buffer *p=buffer[now_level].back();
			buffer[now_level].pop_back();
			old_path=p->path;
			new_path=root+"/level-"+to_string(now_level+1)+"/"+to_string(p->time)+".sst";
			utils::move(old_path,new_path);        //移动文件
			p->path=new_path;
			buffer[now_level+1].emplace_front(p);
		}
		now_level++;
		mx_num<<=1;
	}

}


void KVStore::merge_table(Buffer *p)  //键值对读进内存,调用merge_sort
{
	string filename=p->path;
	fstream in(filename,ios::binary | ios::in);
	if(!in.is_open())
	{
		cout<<"merge_table: can not open SST file "+filename<<endl;
		return;
	}
	//cout<<"merge_table started:"<<filename<<endl;
	int size=p->index.size();
	uint64_t key=0;
	uint32_t l=0,r=0;
	char tmp;
	string val;
	if(p->index.size()==0) return;     //没有索引,直接返回
	if(!in.seekg(p->index[0].second,ios::beg))
	{
		cout<<"merge_table: can not move position "+filename<<endl;
		return;
	}

	vector<pair<uint64_t, string> > map1; //取出键值对,插入map1,之后与map归并
	map1.clear();

	for(int i=0;i<size-1;++i)
	{
		key=p->index[i].first;
		l=p->index[i].second;
		r=p->index[i+1].second;
		val="";
		for(uint32_t j=l;j<r-1;++j)
		{
			in.read((char*)(&tmp),sizeof(tmp));
			val+=tmp;
		}
		in.read((char*)(&tmp),sizeof(tmp));   
	if(tmp!=' ' && tmp!='\0') val+=tmp;
		//if(key==0) cout<<"get key=0 val="<<val<<" size="<<val.size()<<l<<" "<<r<<endl;
		//if(i==0) cout<<"get merge value:"<<val<<endl;
		map1.emplace_back(make_pair(key,val));
	}

	key=p->index[size-1].first;
	l=p->index[size-1].second;
	in.seekg(0,ios::end);
	r=in.tellg();
	in.seekg(l,ios::beg);
	val="";
	for(uint32_t i=l;i<r-1;++i)  //读最后一个键值对
	{
		in.read((char*)(&tmp),sizeof(tmp));
		val+=tmp;
	}
	in.read((char*)(&tmp),sizeof(tmp));   
	if(tmp!=' ' && tmp!='\0') val+=tmp;
	map1.emplace_back(make_pair(key,val));

	merge_sort(map1);

	in.close();

	utils::rmfile(filename);   // merge结束,数据读到内存后,把文件删掉
}


void KVStore::merge_sort(const vector<pair<uint64_t, string> > &map1) {
    int l = 0, l1 = 0;
    int r = map.size() - 1, r1 = map1.size() - 1;
    vector<pair<uint64_t, string> > res;
    res.clear();

    while (l <= r && l1 <= r1) {
        if (map[l].first < map1[l1].first) {
            res.push_back(map[l]);
            l++;
        } else if (map[l].first > map1[l1].first) {
            res.push_back(map1[l1]);
            l1++;
        } else {
            res.push_back(map1[l1]);   //遇到相同键值对,保留map1
            l++;
            l1++;
        }
    }
    for (int i = l; i <= r; i++) res.push_back(map[i]);
    for (int i = l1; i <= r1; i++) res.push_back(map1[i]);
    map=res;
}




//void KVStore::merge_sort(const vector<pair<uint64_t, string> > &map1) //加了删除最后一层删除标记的版本,可以去除注释替换上面的版本
//{
//    int l = 0, l1 = 0;
//    int r = map.size() - 1, r1 = map1.size() - 1;
//    vector<pair<uint64_t, string> > res;
//    res.clear();
//
//    while (l <= r && l1 <= r1)
//    {
//        if (map[l].first < map1[l1].first)
//        {
//            if(mxlevel>1 || map[l].second!=deleted)
//            {
//                res.push_back(map[l]);
//            }
//            l++;
//        }
//        else if (map[l].first > map1[l1].first)
//        {
//            if(mxlevel>1 || map1[l1].second!=deleted)
//            {
//                res.push_back(map1[l1]);
//            }
//            l1++;
//        }
//        else
//        {
//            // res.push_back(map1[l1]);
//            if(mxlevel>1 || map1[l1].second!=deleted)   //map1更加新，若map1[l1]为删除标记，则直接不加入
//            {
//                res.push_back(map1[l1]);   //遇到相同键值对,保留map1
//            }
//            l++;
//            l1++;
//        }
//    }
//    for (int i = l; i <= r; i++)
//    {
//        if(mxlevel>1 || map[i].second!=deleted)
//        {
//            res.push_back(map[i]);
//        }
//    }
//    for (int i = l1; i <= r1; i++)
//    {
//        if(mxlevel>1 || map1[i].second!=deleted)
//        {
//            res.push_back(map1[i]);
//        }
//    }
//    map=res;
//}