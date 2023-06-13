#include "list.h"
#include<unistd.h>
#include<stdlib.h>
#include<stdio.h>
#include<time.h>
#include<string.h>
#include<stdbool.h>
#include<mpi.h>




struct Node* createNode(char* s1,char*s2){
    struct Node* newNode = (struct Node*)malloc(sizeof(struct Node));
    strcpy(newNode->key,s1);
    strcpy(newNode->value,s2);
    newNode->next = NULL;
    return newNode;
}

void insertNode(struct Node** head, struct Node* newNode){
    if(*head == NULL){
        *head = newNode;
    }
    else{
        struct Node* current = *head;
        while(current->next!=NULL){
            current=current->next;
        }
        current->next = newNode;
    }
}

void printList(struct Node* head){
    struct Node* current = head;
    while(current!=NULL){
        printf("(%s : %s)\n",current->key,current->value);
        current = current->next;
    }
}

void insertCombinerKeyVal(CombinerNode** head, char*key, char* value){
    CombinerNode* newNode = (CombinerNode*)malloc(sizeof(CombinerNode));
    ValueNode* newVal = (ValueNode*)malloc(sizeof(ValueNode));
    strcpy(newVal->value,value);newVal->next=NULL;
    strcpy(newNode->key,key); newNode->values=newVal;
    newNode->next=NULL;
    if(*head == NULL){
        *head = newNode;
        return;
    }
    CombinerNode* current = *head;
    while(current->next!=NULL){
        current = current->next;
    }
    current->next=newNode;
}

void insertCombinerVal(CombinerNode** head,char* value){
    ValueNode* newNode = (ValueNode*)malloc(sizeof(ValueNode));
    strcpy(newNode->value,value);newNode->next=NULL;
    ValueNode* current = (*head)->values;
    while(current->next!=NULL){
        current = current->next;
    }
    current->next=newNode;
}

bool checkKeyPresence(CombinerNode* head,char* key){
    CombinerNode* current = head;
    while(current!=NULL){
        if(strcmp(key,current->key)==0){
            return true;
        }
        current=current->next;
    }
    return false;
}

int maxValCount(CombinerNode* head){
    CombinerNode* currentKey = head;
    int maxCount = -1;
    while(currentKey!=NULL){
        int valCount=0;
        ValueNode* currentVal=currentKey->values;
        while(currentVal!=NULL){
            valCount++;
            currentVal=currentVal->next;
        }
        if(valCount>=maxCount){maxCount=valCount;}
        currentKey=currentKey->next;
    }
    return maxCount;
}

CombinerNode* checkKeyLocation(CombinerNode* head,char* key){
    CombinerNode* current = head;
    while(current!=NULL){
        if(strcmp(key,current->key)==0){
            return current;
        }
        current=current->next;
    }
}

void printCombiner(CombinerNode* head){
    printf("\n");
    CombinerNode* currentKey = head;
    while(currentKey!=NULL){
        printf("\n Key Is %s ---------------------------\n",currentKey->key);
        ValueNode* currentVal=currentKey->values;
        while(currentVal!=NULL){
            printf("%s",currentVal->value);
            currentVal=currentVal->next;
        }
        currentKey=currentKey->next;
    }
    printf("\n\n");
}

