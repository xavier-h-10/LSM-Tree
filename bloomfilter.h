#pragma once

#include <iostream>
#include <cstring>
#include "MurmurHash3.h"

class bloomfilter
{
private:
	const static int len=10240;
	unsigned int hash[4];

public:
	bloomfilter();
	~bloomfilter();
	void insert(uint64_t key);
	bool search(uint64_t key);
	bool clear();
	bool array[len];

};