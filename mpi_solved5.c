/******************************************************************************
* FILE: mpi_bug5.c
* DESCRIPTION: 
*   This is an "unsafe" program. It's behavior varies depending upon the
*   platform and MPI library
* AUTHOR: Blaise Barney 
* LAST REVISED: 01/24/09
* Hint: If possible, try to run the program on two different machines,
* which are connected through a network. You should see uneven timings;
* try to understand/explain them.
******************************************************************************/
#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>

#define MSGSIZE 2000

int main (int argc, char *argv[])
{
int        numtasks, rank, i, tag=111, dest=1, source=0, count=0;
char       data[MSGSIZE];
double     start, end, result;
MPI_Status status;

MPI_Init(&argc,&argv);
MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
MPI_Comm_rank(MPI_COMM_WORLD, &rank);

if (rank == 0) {
  printf ("mpi_bug5 has started...\n");
  if (numtasks > 2) 
    printf("INFO: Number of tasks= %d. Only using 2 tasks.\n", numtasks);
  }

/*

 With additional print statements added the output looks like

mpi_bug5 has started...
Count= 10  Time= 0.000032 sec.
Count= 20  Time= 0.000009 sec.
Count= 30  Time= 0.000008 sec.
Count= 40  Time= 0.000009 sec.
Count= 50  Time= 0.000008 sec.
Count= 60  Time= 0.000008 sec.
Count= 70  Time= 0.048171 sec.
TASK 1: finished 10
Count= 80  Time= 0.120698 sec.
TASK 1: finished 20
Count= 90  Time= 0.120814 sec.
TASK 1: finished 30
Count= 100  Time= 0.120884 sec.
TASK 1: finished 40

 The first task uses a non blocking send so it continues to send data until the 
 buffer becomes full. At that point, after 70 loops, we see that both the tasks
 begin to move at the same rate as the send becomes blocking until there is space
 in the buffer

 SOLUTION: use a synchronous blocking send. The output with extra printing is

mpi_bug5 has started...
Count= 10  Time= 0.109197 sec.
TASK 1: finished 10
Count= 20  Time= 0.121722 sec.
TASK 1: finished 20
Count= 30  Time= 0.123102 sec.
TASK 1: finished 30
Count= 40  Time= 0.126087 sec.
TASK 1: finished 40
Count= 50  Time= 0.126073 sec.
TASK 1: finished 50
Count= 60  Time= 0.126135 sec.
TASK 1: finished 60
Count= 70  Time= 0.126106 sec.
TASK 1: finished 70

 */

/******************************* Send task **********************************/
if (rank == 0) {

  /* Initialize send data */
  for(i=0; i<MSGSIZE; i++)
     data[i] =  'x';

  start = MPI_Wtime();
  while (1) {
    MPI_Ssend(data, MSGSIZE, MPI_BYTE, dest, tag, MPI_COMM_WORLD);
    count++;
    if (count % 10 == 0) {
      end = MPI_Wtime();
      printf("Count= %d  Time= %f sec.\n", count, end-start);
      start = MPI_Wtime();
      }
    }
  }

/****************************** Receive task ********************************/
// int counter = 1;
if (rank == 1) {
  while (1) {
    MPI_Recv(data, MSGSIZE, MPI_BYTE, source, tag, MPI_COMM_WORLD, &status);
    /* Do some work  - at least more than the send task */
    result = 0.0;
    for (i=0; i < 1000000; i++) 
      result = result + (double)random();
    /* DEBUGGING
    if(counter % 10 == 0)
      printf("TASK 1: finished %d\n", counter);
    counter++;
    */
    }
  }

MPI_Finalize();
return 0;
}

