#include<unistd.h>
#include<stdlib.h>
#include<stdio.h>
#include<time.h>
#include<string.h>
#include<stdbool.h>
#include<fcntl.h>
#include<mpi.h>
#include "list.h"

// LxM MxN = LxN 
int L, M, N;
int** MatrixA;
int* TempA;
int* TempB;
int** MatrixB;
int** MatrixC;
int** LocalA;
int* RecvA;
int* localRows;
int* localNumVals;
int counter=0;

// Global Variables
struct Node* Context; struct Node* CombinedContext=NULL; char*** Context2; char* TempContext;
CombinerNode* SortedContext=NULL; int numCombinerKeys =0; char*** SortedContext2; char* TempSortedContext;
int rank; int size; int rowA_per_process; int totalPairs=0; int maxCount=0;
bool* Mappers; int numMappers=0;
bool* Reducers; int numReducers=0; int firstReducer;
int start_row_a; int end_row_a; int start_col_b; int end_col_b;int local_num_rows;
int reducer_start_row; int reducer_end_row; int reducer_row_pp; int local_reducer_row;

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
    Mappers = (bool*)malloc(sizeof(bool)*size);
    Reducers = (bool*)malloc(sizeof(bool)*size);
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

void assignMappersReducers(){
    memset(Mappers,false,size*sizeof(bool));
    memset(Reducers,false,size*sizeof(bool));
    // Root Decide dynamically the assignment method
    if(rank==0){
        char pname[256]; int plen;
        MPI_Get_processor_name(pname,&plen);
        printf("Master With Process ID %d running on %s\n",rank,pname);
        srand(time(0));
        int strategy = rand()%2;
        //int strategy =0;
        if(strategy==0){
            printf("Strategy1\n");
            for(int i=1;i<size;i++){
                if(i<(int)(size-1)*0.6){Mappers[i]=true;numMappers++;printf("Task Map Assigned to Process %d\n",i);}
                else{Reducers[i]=true;numReducers++;printf("Task Reduce Assigned to Process %d\n",i);}
            }
        }
        else{
            printf("Strategy 2\n");
            for(int i=1;i<size;i++){
                if(i>=(int)(size-1)*0.6){Reducers[i]=true;numReducers++;printf("Task Reduce Assigned to Process %d\n",i);}
                Mappers[i]=true;numMappers++;printf("Task Map Assigned to Process %d\n",i);
            }
        }

    }
    MPI_Bcast(Mappers,size,MPI_C_BOOL,0,MPI_COMM_WORLD);
    MPI_Bcast(Reducers,size,MPI_C_BOOL,0,MPI_COMM_WORLD);
    
}

void sendInputToMappers(){
    MPI_Bcast(&numMappers,1,MPI_INT,0,MPI_COMM_WORLD);
    rowA_per_process = L/numMappers;
    if(rank==0){
        int colB_per_process = M/numMappers;
        //printf("RPP = %d , CPP = %d\n",rowA_per_process,colB_per_process);
        int offset_row=0; int offset_col = 0;
        for(int i=1;i<=numMappers;i++){
            start_row_a=offset_row;
            //start_col_b = offset_col;
            start_col_b = 0;
            if(i==numMappers){
                end_row_a = L-1;
                end_col_b = N-1;
            }
            else{
                end_row_a = offset_row + rowA_per_process -1;
                end_col_b = offset_col + colB_per_process -1;
            }
            end_col_b = M-1; // rough
            MPI_Send(&start_row_a,1,MPI_INT,i,1,MPI_COMM_WORLD);
            MPI_Send(&end_row_a,1,MPI_INT,i,2,MPI_COMM_WORLD);
            MPI_Send(&start_col_b,1,MPI_INT,i,3,MPI_COMM_WORLD);
            MPI_Send(&end_col_b,1,MPI_INT,i,4,MPI_COMM_WORLD);
            offset_row += rowA_per_process;
            offset_col += colB_per_process;
        }
        // printf("Rank %d has send All\n",rank);
    }
    else if(Mappers[rank]==true){
        MPI_Recv(&start_row_a,1,MPI_INT,0,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        MPI_Recv(&end_row_a,1,MPI_INT,0,2,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        MPI_Recv(&start_col_b,1,MPI_INT,0,3,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        MPI_Recv(&end_col_b,1,MPI_INT,0,4,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        //printf("Rank %d has Recieved ALl\n",rank);
    }
    // printf("Rank %d has Reached this Point\n",rank);
    MPI_Barrier(MPI_COMM_WORLD);
}

// Every Process Executes the mapper function
void mapperFunction(){
    char pname[256]; int plen;
    MPI_Get_processor_name(pname,&plen);
    printf("Process %d Recieved Task Map on Machine %s\n",rank,pname);
    // Matrix A -> (i,j) : (A,j,A[i][j])
    for(int i=start_row_a;i<=end_row_a;i++){
        for(int j=start_col_b;j<=end_col_b;j++){
            for(int k=0;k<N;k++){
                int val = i-(rowA_per_process*(rank-1));
                // if(i==5 && j==2){
                //     printf("Mapper %d, Before : (%d,%d),(A,%d,%d)\n",rank,i,k,j,MatrixA[i][j]);
                // }
                //printf("Mapper %d : (%d,%d),(A,%d,%d)\n",rank,i,k,j,LocalA[val][j]);
                //printf("Mapper %d : (%d,%d),(B,%d,%d)\n",rank,i,k,j,MatrixB[j][k]);
                //char str_i[2]; str_i[0]=i+'0'; str_i[1]='\0';
                //char str_j[2]; str_j[0]=j+'0'; str_j[1]='\0';
                //char str_k[2]; str_k[0]=k+'0'; str_k[1]='\0';
                //char str_v1[2]; str_v1[0]=MatrixA[i][j]+'0'; str_v1[1]='\0';
                //char str_v2[2]; str_v2[0]=MatrixB[j][k]+'0'; str_v2[1]='\0';
                char str_i[256]; sprintf(str_i,"%d",i);
                char str_j[256]; sprintf(str_j,"%d",j);
                char str_k[256]; sprintf(str_k,"%d",k);
                char str_v1[256];sprintf(str_v1,"%d",MatrixA[i][j]);
                char str_v2[256]; sprintf(str_v2,"%d",MatrixB[j][k]);
                char result_a_key[256]; char result_a_val[256]; char result_b_key[256];char result_b_val[256];
                strcpy(result_a_key,"(");strcat(result_a_key,str_i);strcat(result_a_key,",");strcat(result_a_key,str_k);strcat(result_a_key,")");
                strcpy(result_b_key,"(");strcat(result_b_key,str_i);strcat(result_b_key,",");strcat(result_b_key,str_k);strcat(result_b_key,")");
                strcpy(result_a_val,"(A,");strcat(result_a_val,str_j);strcat(result_a_val,",");strcat(result_a_val,str_v1);strcat(result_a_val,")");
                strcpy(result_b_val,"(B,");strcat(result_b_val,str_j);strcat(result_b_val,",");strcat(result_b_val,str_v2);strcat(result_b_val,")");
                //printf("Mapper %d : %s , %s\n",rank,result_a_key,result_a_val);
                //printf("Mapper %d : %s , %s\n",rank,result_b_key,result_b_val);
                struct Node* n1 = createNode(result_a_key,result_a_val);
                struct Node* n2 = createNode(result_b_key,result_b_val);
                insertNode(&Context,n1); insertNode(&Context,n2);
                counter+=2;
            }
        }
    }
}

void gatherAllPairs(){
    if(Mappers[rank]==true){
        MPI_Send(&counter,1,MPI_INT,0,0,MPI_COMM_WORLD);
    }
    else if(rank==0){
        for(int i=1;i<=numMappers;i++){
            int num=0;
            MPI_Recv(&num,1,MPI_INT,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            totalPairs=totalPairs+num;
        }
    }
    MPI_Bcast(&totalPairs,1,MPI_INT,0,MPI_COMM_WORLD);
    Context2=malloc(totalPairs*sizeof(char**));
    for(int i=0;i<totalPairs;i++){
        Context2[i]=malloc(2*sizeof(char));
        for(int j=0;j<2;j++){
            Context2[i][j]=malloc(256*sizeof(char));
        }
    }
    
    if(rank==0){
        // root will recieve all the individual contexts
        int currentIndex=0;
        for(int i=1;i<=numMappers;i++){
            char key[256]; char value[256]; int nums;
            MPI_Recv(&nums,1,MPI_INT,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            for(int j=0;j<nums;j++){
                //printf("Before %d : %d-----------------\n",rank,j);
                MPI_Recv(key,256,MPI_CHAR,i,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                MPI_Recv(value,256,MPI_CHAR,i,2,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                strcpy(Context2[currentIndex][0],key); strcpy(Context2[currentIndex][1],value);
                currentIndex++;
            }
        }
        // printf("Context2 in root: \n");
        // for(int i=0;i<totalPairs;i++){
        //     printf("%d - > %s %s\n",i,Context2[i][0],Context2[i][1]);
        // }
    }
    else if(Mappers[rank]==true){
        MPI_Send(&counter,1,MPI_INT,0,0,MPI_COMM_WORLD);
        struct Node* current=Context;
        
        while(current!=NULL){
            MPI_Send(current->key,256,MPI_CHAR,0,1,MPI_COMM_WORLD);
            MPI_Send(current->value,256,MPI_CHAR,0,2,MPI_COMM_WORLD);
            current=current->next;
        }
        
    }
    //printf("Rank %d Reached Here\n",rank);
    
    // Now we must BroadCast this to all Processes
    int numElements = totalPairs*2;
    TempContext = malloc(numElements* 256 *sizeof(char));
    if(rank==0){
        int index =0;
        for(int i=0;i<totalPairs;i++){
            for(int j=0;j<2;j++){
                strcpy(&TempContext[index],Context2[i][j]);
                index+=256;
            }
        }
    }
    MPI_Bcast(TempContext,numElements*256,MPI_CHAR,0,MPI_COMM_WORLD);
    if(rank!=0){
        for(int i=0;i<totalPairs;i++){
            for(int j=0;j<2;j++){
                strncpy(Context2[i][j],&TempContext[(i*2+j)*256],256);
            }
        }
    }
}

void performCombiner(){
    for(int i=0;i<totalPairs;i++){
        char key[256];strcpy(key,Context2[i][0]);
        char value[256];strcpy(value,Context2[i][1]);
        if(checkKeyPresence(SortedContext,key)==false){
            insertCombinerKeyVal(&SortedContext,key,value);
            numCombinerKeys++;
        }
        else{
            CombinerNode* current = checkKeyLocation(SortedContext,key);
            insertCombinerVal(&current,value);
        }
    }
}

void sendInputToReducers(){
    if(rank==0){
        maxCount = maxValCount(SortedContext);
    }
    MPI_Bcast(&numCombinerKeys,1,MPI_INT,0,MPI_COMM_WORLD);
    MPI_Bcast(&maxCount,1,MPI_INT,0,MPI_COMM_WORLD);
    //printf("Rank %d , CombinerKeys %d, maxCount %d\n",rank,numCombinerKeys,maxCount);
    int thing = maxCount+1;
    SortedContext2=(char***)malloc(numCombinerKeys*sizeof(char**));
    for(int i=0;i<numCombinerKeys;i++){
        SortedContext2[i]=(char**)malloc(thing*sizeof(char*));
        for(int j=0;j<maxCount+1;j++){
            SortedContext2[i][j]=malloc(256*sizeof(char));
        }
    }
    if(rank==0){
        // First Convert to 3D array
        CombinerNode* currentNode = SortedContext;
        int idx=0;
        while(currentNode!=NULL){
            int validx=0;
            ValueNode* currentValue = currentNode->values;
            strcpy(SortedContext2[idx][validx],currentNode->key);
            validx++;
            while(currentValue!=NULL){
                strcpy(SortedContext2[idx][validx],currentValue->value);
                validx++;
                currentValue=currentValue->next;
            }
            currentNode=currentNode->next;
            idx++;
        }
    }
    int numElements = numCombinerKeys*(maxCount+1);
    TempSortedContext = malloc(numElements* 256 *sizeof(char));
    if(rank==0){
        int index =0;
        for(int i=0;i<numCombinerKeys;i++){
            for(int j=0;j<maxCount+1;j++){
                strcpy(&TempSortedContext[index],SortedContext2[i][j]);
                index+=256;
            }
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Bcast(TempSortedContext,numElements*256,MPI_CHAR,0,MPI_COMM_WORLD);
    if(rank!=0){
        for(int i=0;i<numCombinerKeys;i++){
            for(int j=0;j<maxCount+1;j++){
                strncpy(SortedContext2[i][j],&TempSortedContext[(i*(maxCount+1)+j)*256],256);
            }
        }
    }
    // if(rank==7){
    //     printf("Printing Sorted Context 2\n");
    //     for(int i=0;i<numCombinerKeys;i++){
    //         printf("\n\n%s : ",SortedContext2[i][0]);
    //         for(int j=1;j<maxCount+1;j++){
    //             printf("%s,",SortedContext2[i][j]);
    //         }
    //         printf("\n\n");
    //     }
    // }
}

void divideReduceTasks(){
    MPI_Bcast(&numReducers,1,MPI_INT,0,MPI_COMM_WORLD);
    // Divide Rows of Combiner amongst number of reducers
    reducer_row_pp = (int)(L/numReducers)*8;
    for(int i=0;i<size;i++){
        if(Reducers[i]==true){
            firstReducer=i;
            break;
        }
    }
    if(rank==0){
        int offset_row = 0;
        for(int i=firstReducer;i<size;i++){
            reducer_start_row=offset_row;
            if(i==size-1){
                reducer_end_row= (L*L)-1;
            }
            else{
                reducer_end_row=offset_row+reducer_row_pp-1;
            }
            MPI_Send(&reducer_start_row,1,MPI_INT,i,1,MPI_COMM_WORLD);
            MPI_Send(&reducer_end_row,1,MPI_INT,i,2,MPI_COMM_WORLD);
            offset_row+=reducer_row_pp;
        }
    }
    else{
        for(int i=firstReducer;i<size;i++){
            if(rank==i){
                MPI_Recv(&reducer_start_row,1,MPI_INT,0,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                MPI_Recv(&reducer_end_row,1,MPI_INT,0,2,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                //printf("Rank %d , SR: %d, ER %d \n",rank,reducer_start_row,reducer_end_row);
            }
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

void reducerFunction(){
    MPI_File fh;
    int rc;
    char data[256];
    MPI_File_open(MPI_COMM_WORLD, "reducer_output.txt", MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
    if(Reducers[rank]==true){
        char pname[256]; int plen;
    MPI_Get_processor_name(pname,&plen);
    printf("Process %d Recieved Task Reduce on Machine %s\n",rank,pname);
        for(int i=reducer_start_row;i<=reducer_end_row;i++){
            int sum=0;
            char key[256]; strcpy(key,SortedContext2[i][0]);
            for(int j=1;j<maxCount+1;){
                char val1[256];char val2[256];
                strcpy(val1,SortedContext2[i][j]);strcpy(val2,SortedContext2[i][j+1]);
                // int num1 = val1[5]-'0'; int num2=val2[5]-'0';
                int num1; int num2; int temp1;int temp2;
                sscanf(val1,"(A,%d,%d)",&temp1,&num1);sscanf(val2,"(B,%d,%d)",&temp2,&num2);
                //printf("xx: %s = %d\n",val1,num1);
                sum = sum + (num1*num2); 
                j+=2;
            }
            char data[256];
            sprintf(data,"%s:%d\n",key,sum);
            MPI_File_write_shared(fh, data, strlen(data), MPI_CHAR, MPI_STATUS_IGNORE);
            //printf("P%d , %s , Sum = %d\n",rank,key,sum);
        }
        printf("Process %d has completed task Reduce\n",rank);
    }
    MPI_File_close(&fh);
}

void readResultsFromFile(){
    FILE* fptr;
    fptr = fopen("reducer_output.txt","r");
    if(fptr==NULL){
        printf("Error: Cannot Open File.\n");
        return;
    }
    char line[256];
    while(fgets(line,sizeof(line),fptr)!=NULL){
        // int row = line[1]-'0'; int col= line[3]-'0';
        // char* token = strtok(line, ":");
        // token = strtok(NULL, ":"); 
        // int num = atoi(token);
        int row;int col; int num;
        sscanf(line,"(%d,%d):%d",&row,&col,&num);
        MatrixC[row][col]=num;
    }
    fclose(fptr);
    FILE* fptr2;
    fptr2=fopen("final_result.txt","w");
    printf("Job is Complete\n");
    //printf("\nResultant Matrix: \n");
    for(int i=0;i<M;i++){
        for(int j=0;j<N;j++){
            //printf("%d  ",MatrixC[i][j]);
            fprintf(fptr2,"%d  ",MatrixC[i][j]);
        }
        //printf("\n");
        fprintf(fptr2,"\n");
    }
}

int main(int argc,char* argv[]){
    //Mpi Environment
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&size);
    //printf("SIZE: %d\n",size);
    //printf("CP1: Process %d ------\n",rank);
    // Read Command Line Arguments and get dimensions
    initialiseData(argc,argv);
    assignMappersReducers();
    sendInputToMappers();
    //printf("CP2: Process %d ------\n",rank);
    // if(rank!=0){
    //     printf("Process %d , sr %d, er %d, sc %d, ec %d\n",rank,start_row_a,end_row_a,start_col_b,end_col_b);
    // }
    if(Mappers[rank]==true){
        mapperFunction();
        printf("Process %d has completed task Map\n",rank);
    }
    //printf("Rank %d Counter is %d \n",rank,counter);
    gatherAllPairs();
    if(rank==0){
        performCombiner();
    }
    sendInputToReducers();
    divideReduceTasks();
    FILE* fptr;
    fptr = fopen("reducer_output.txt","w");
    fclose(fptr);
    reducerFunction();
    if(rank==0){
        readResultsFromFile();
    }
    MPI_Finalize();
    return 0;
}







// printf("Process %d ------\n",rank);
//     for(int i=0;i<size;i++){printf("%d\t",Mappers[i]);}
//     printf("\n");
//     for(int i=0;i<size;i++){printf("%d\t",Reducers[i]);}
//     printf("\n");


// for(int i=0;i<L;i++){
//         for(int j=0;j<M;j++){
//             printf("%d ",MatrixA[i][j]);
//         }
//         printf("\n");
//     }
//     printf("\n");
//     for(int i=0;i<M;i++){
//         for(int j=0;j<N;j++){
//             printf("%d ",MatrixB[i][j]);
//         }
//         printf("\n");
//     }


// printf("Rank %d Printing List\n",rank);
    // printList(Context);
    // if(rank==0){
    //     for(int i=0; i<totalPairs;i++){
    //         for(int j=0;j<2;j++){
    //             printf("%s ",Context2[i][j]);
    //         }
    //     printf("\n");
    //     }
    // }


// MPI_File_open(MPI_COMM_WORLD,"reducer_output.txt",MPI_MODE_CREATE | MPI_MODE_WRONLY,MPI_INFO_NULL,&fh);
            // MPI_File_write_ordered(fh,data,strlen(data),MPI_CHAR,MPI_STATUS_IGNORE);
            // MPI_File_close(&fh);
//MPI_File fh;
