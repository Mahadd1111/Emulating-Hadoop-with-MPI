#ifndef LIST_H
#define LIST_H

#include<stdbool.h>

struct Node{
    char key[256];
    char value[256];
    struct Node* next;
};

typedef struct combinernode{
    char key[256];
    struct valuenode* values;
    struct combinernode* next;
}CombinerNode;

typedef struct valuenode{
    char value[256];
    struct valuenode* next;
}ValueNode;

// For each Local Mapper LinkedList
struct Node* createNode(char* s1,char*s2);
void insertNode(struct Node** head, struct Node* newNode);
void printList(struct Node* head);

// For the Combiner LinkedList
void insertCombinerKeyVal(CombinerNode** head, char*key, char* value);
void insertCombinerVal(CombinerNode** head,char* value);
void printCombiner(CombinerNode* head);
bool checkKeyPresence(CombinerNode* head,char* key);
CombinerNode* checkKeyLocation(CombinerNode* head,char* key);
int maxValCount(CombinerNode* head);


#endif