#include <cstdio>
#include <iostream>
#include "bloomfilter.h"
#include "MurmurHash3.h"

using namespace std;

void bloomfilter::insert(uint64_t key)
{
	memset(hash,0,sizeof(hash));
	MurmurHash3_x64_128(&key,sizeof(key),1,hash);
	for(int i=0;i<4;i++)
	{
		array[hash[i]%len]=true;
	}
}

bool bloomfilter::search(uint64_t key)
{
	memset(hash,0,sizeof(hash));
	MurmurHash3_x64_128(&key,sizeof(key),1,hash);
	for(int i=0;i<4;i++)
	{
		if(!array[hash[i]%len])
		{
			return false;
		}
	}
	return true;
}

bloomfilter::bloomfilter()
{
	memset(array,0,sizeof(array));

}

bloomfilter::~bloomfilter()
{
}

bool bloomfilter::clear()
{
	memset(array,0,sizeof(array));
	return true;
}

// int main()
// {
// 	long long key=103122;
// 	unsigned int hash[4]={0};
// 	MurmurHash3_x64_128(&key,sizeof(key),1,hash);
// 	for(int i=0;i<4;i++)
// 	{
// 		cout<<hash[i]<<" ";
// 	}
// 	cout<<endl;
// 	return 0;
// }