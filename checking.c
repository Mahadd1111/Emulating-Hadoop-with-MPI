#include<unistd.h>
#include<stdlib.h>
#include<stdio.h>
#include<time.h>
#include<string.h>
#include<stdbool.h>
#include<fcntl.h>
#include<mpi.h>
#include "list.h"

int L, M, N;
int** MatrixA;
int* TempA;
int* TempB;
int** MatrixB;
int** MatrixC;
int rank,size;

// Extract the array dimensions from file name
void getArrayDimensions(char file[],int* n1,int*n2){
        int j = 0; int i=6;
        char num1[256]={'\0'};
        char num2[256]={'\0'};
        while(file[i]!='x'){num1[j++]=file[i++];}
        j=0;i++;
        while(file[i]!='_'){num2[j++]=file[i++];}
        *n1 = atoi(num1);
        *n2 = atoi(num2);
}

void readArraysFromFile(char* filename1,char*filename2){
    FILE* fptr1;
    fptr1=fopen(filename1,"rb");
    if(fptr1==NULL){printf("Unable to Open File.\n");return;}
    fseek(fptr1,0,SEEK_SET);
    for(int i=0;i<L;i++){
        fread(MatrixA[i],sizeof(int),M,fptr1);
    }
    fclose(fptr1);
    FILE* fptr2;
    fptr2 = fopen(filename2,"rb");
    if(fptr2==NULL){printf("Unable to Open File.\n");return;}
    fseek(fptr2,0,SEEK_SET);
    for(int i=0;i<M;i++){
        fread(MatrixB[i],sizeof(int),N,fptr2);
    }
    fclose(fptr2);
    for(int i=0;i<L;i++){
        for(int j=0;j<M;j++){
            *(TempA+i*M+j)=MatrixA[i][j];
        }
    }
    for(int i=0;i<M;i++){
        for(int j=0;j<N;j++){
            *(TempB+i*N+j)=MatrixB[i][j];
        }
    }
}

void initialiseData(int argc,char*argv[]){
    if(argc!=3){printf("Please Give 2 File Arguments.\n");return;}
    char file1[256]; strcpy(file1,argv[1]);
    char file2[256]; strcpy(file2,argv[2]);
    int n1,n2,n3,n4=0;
    getArrayDimensions(file1,&n1,&n2);
    getArrayDimensions(file2,&n3,&n4);
    if(n2!=n3){printf("The Dimensions are incompatible to multiply.\n");return;}
    else{L=n1;M=n2;N=n4;}
    // Initialize arrays with Given Dimensions
    MatrixA = (int**)malloc(L*sizeof(int*));
    for(int i=0;i<L;i++){MatrixA[i]=(int*)malloc(M*sizeof(int));}
    MatrixB = (int**)malloc(M*sizeof(int*));
    for(int i=0;i<M;i++){MatrixB[i]=(int*)malloc(N*sizeof(int));}
    MatrixC = (int**)malloc(L*sizeof(int*));
    for(int i=0;i<L;i++){MatrixC[i]=(int*)malloc(N*sizeof(int));}
    TempA = (int*)malloc(L*M*sizeof(int));
    TempB = (int*)malloc(M*N*sizeof(int));
    if(rank==0){
        readArraysFromFile(file1,file2);
    }
    MPI_Bcast(TempA,L*M,MPI_INT,0,MPI_COMM_WORLD);
    MPI_Bcast(TempB,M*N,MPI_INT,0,MPI_COMM_WORLD);
    for(int i=0;i<M;i++){
        for(int j=0;j<N;j++){
            MatrixB[i][j]=*(TempB+i*N+j);
        }
    }
    for(int i=0;i<L;i++){
        for(int j=0;j<N;j++){
            MatrixA[i][j]=*(TempA+i*N+j);
        }
    }
}

void serialMultiplication(){
    for(int i=0;i<L;i++){
        for(int j=0;j<N;j++){
            MatrixC[i][j]=0;
            for(int k=0;k<M;k++){
                MatrixC[i][j]+=MatrixA[i][k]*MatrixB[k][j];
            }
            printf("%d\t",MatrixC[i][j]);
        }
        printf("\n");
    }
}

int main(int argc, char* argv[]){
    //Mpi Environment
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&size);
    initialiseData(argc,argv);
    serialMultiplication();
}