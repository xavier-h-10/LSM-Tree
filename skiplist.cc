#include <string>
#include <vector>
#include <iostream>
#include "skiplist.h"

using namespace std;

// int main()
// {
// 	skiplist list;
// 	uint64_t i;
// 	list.add(5,"fdsa");
// 	list.add(5,"fff");
// 	cout<<list.find(5)<<endl;
// 	list.clear();
// 	list.add(2,"newhaha");
// 	cout<<list.find(2)<<endl;
// 	cout<<list.find(3)<<endl;
// 	cout<<list.find(5)<<endl;
// 	cout<<list.find(100)<<endl;
// 	cout<<list.find(234)<<endl;
// }

skiplist::skiplist()
{
	 head=new Node(NULL,NULL,-1,"");
	 size=0;
}


string skiplist::find(uint64_t target)
{
	//if(target==2) cout<<"find called"<<endl;
	Node *p=head;
	while(p)
	{
		while(p->right && p->right->key<target)
		{   //寻找目标区间，基本就是这个思路
			p=p->right;
		}
		if(!p->right || target<p->right->key)
		{ //没找到目标值，则继续往下走
			p=p->down;
		}
		else
		{         //找到目标值，结束
		//	cout<<"p->val="<<p->right->val<<endl;
			//if(target==2) cout<<"find it !"<<p->right->val<<endl;
			return p->right->val;
		}
	}
	return "";
}

void skiplist::add(uint64_t num, string str)
{
	// if(num==9994) 
	// 	cout<<"add skiplist: num="<<num<<" str="<<str<<endl;
	vector<Node*> pathList;    //从上至下记录搜索路径
	Node *p=head;
	bool find_it=false;
	while(p)
	{
		while(p->right && p->right->key<num)
		{ 
			p=p->right;
		}
		if(p->key==num)
		{
			find_it=true;
			break;
		}
		if(p->right && p->right->key==num)
		{
			find_it=true;
			p=p->right;
			break;
		}
		pathList.push_back(p);
		p=p->down;
	}
	if(find_it)
	{
		//if(num==9994) cout<<"num=9994 find it val="<<find(num)<<endl;
		while(p)
		{
			p->val=str;
			p=p->down;
		}
		return;
	}

	bool  insertUp=true;
	Node* downNode=NULL;
	while(insertUp && pathList.size()>0)
	{   //从下至上搜索路径回溯，50%概率
		Node *insert=pathList.back();
		pathList.pop_back();
		insert->right=new Node(insert->right,downNode,num,str); //add新结点
		//cout<<"now insert"<<insert->right->val<<endl;
		downNode=insert->right;    //把新结点赋值为downNode
		insertUp=(rand()&1)==0;   //50%概率
	}
	if(insertUp)
	{  //插入新的头结点，加层
		head=new Node(new Node(NULL,downNode,num,str),head,-1,"");
	}

	// if(num==9994) 
	// 	cout<<"add find skiplist: num="<<num<<" str="<<find(num)<<endl;
}    

bool skiplist::erase(uint64_t num) 
{
	Node *p=head;
	bool seen=false;
	while(p)
	{
		while(p->right && p->right->key<num)
		{
			p=p->right;
		}
		if(!p->right ||p->right->key>num)
		{
			p=p->down;
        }
        else
        {    //搜索到目标结点，进行删除操作，结果记录为true，继续往下层搜索，删除一组目标结点
        	seen=true;
        	p->right=p->right->right;
        	p=p->down;
        }
    }
    return seen;
}

void skiplist::clear()
{
	Node *p=head;
	Node *p1,*p2;
	while(p)
	{
		p1=p->right;
		while(p1)
		{
			p2=p1;
			p1=p1->right;
			delete p2;
			p2=nullptr;
		}
		p1=p;
		p=p->down;
		delete p1;
		p1=nullptr;
	}
	head=new Node(NULL,NULL,-1,"");
	size=0;
}

