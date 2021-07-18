#pragma once

#include <vector>
#include <string>

struct Node
{
    Node *right,*down;   //向右向下足矣
    uint64_t key;
    std::string val;
    Node(Node *right,Node *down,uint64_t key,std::string val):right(right),down(down),key(key),val(val){}
};

class skiplist 
{

public:
    skiplist();
    std::string find(uint64_t target);
    void add(uint64_t num, std::string str);
    bool erase(uint64_t num);
    void clear();

    Node *head;
    int size;

};

