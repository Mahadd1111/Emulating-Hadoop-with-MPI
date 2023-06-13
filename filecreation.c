#include<unistd.h>
#include<stdlib.h>
#include<stdio.h>
#include<time.h>
#include<mpi.h>

void initializeArray(int rows, int cols,int arr[rows][cols]){
    srand(time(0));
    for(int i=0;i<rows;i++){
        for(int j=0;j<cols;j++){
            int num = rand()%10;
            printf("%d",num);
            arr[i][j] = num;
        }
        printf("\n");
    }
}

void writeArrayToFile(int (*arr)[], int rows, int cols,char file[]){
    FILE* fptr;
    fptr = fopen(file,"wb");
    if(fptr==NULL){
        printf("Error Opening File\n");
        return;
    }
    fwrite(arr,sizeof(int),rows*cols,fptr);
    fclose(fptr);
}

int main(){
    int rows = 32;
    int cols = 32;
    char filename[256]="Array_32x32_2.dat";
    int array[rows][cols];
    initializeArray(rows,cols,array);
    writeArrayToFile(array,rows,cols,filename);
    return 0;
}
