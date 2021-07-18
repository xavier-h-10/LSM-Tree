#pragma once

#include "bloomfilter.h"
#include <vector>
#include <string>

class Buffer
{
public:
	uint64_t time;
	uint64_t size;
	uint64_t mn;
	uint64_t mx;
	bloomfilter *filter;
	std::string path;
	std::vector<std::pair<uint64_t, uint32_t> > index;
	Buffer()
	{
		time=size=mn=mx=0;
		index.clear();
		path="";
		filter=new bloomfilter();
	}
	~Buffer()
	{
		delete filter;
		filter=nullptr;
	}
};
