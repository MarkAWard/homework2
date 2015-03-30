/* Parallel sample sort
 */
#include <stdio.h>
#include <unistd.h>
#include <mpi.h>
#include <stdlib.h>
#define ROOT 0

static int compare(const void *a, const void *b)
{
  int *da = (int *)a;
  int *db = (int *)b;

  if (*da > *db)
    return 1;
  else if (*da < *db)
    return -1;
  else
    return 0;
}

int main( int argc, char *argv[])
{
  int rank, numtasks;
  int i, j, N, nsamples, total_incoming, incoming_offset;
  int *vec, *samples, *gathered_samples, 
    *splitters, *bucket_counts, *bucket_offset, 
    *final_counts, *incoming;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

  /* Number of random numbers per processor (this should be increased
   * for actual tests or could be passed in through the command line */
  N = atoi(argv[1]);
  nsamples = (N / 100) > 100 ? 100 : (N / 100);
  nsamples = nsamples > 5 ? nsamples : 5;

  /* seed random number generator differently on every core */
  srand((unsigned int) (rank + 393919));

  /* fill vector with random integers */
  vec = calloc(N, sizeof(int));
  for (i = 0; i < N; ++i) {
    vec[i] = rand();
  }
  //printf("rank: %d, first entry: %d, nsamples: %d\n", rank, vec[0], nsamples);

  /* sort locally */
  qsort(vec, N, sizeof(int), compare);

  /* randomly sample s entries from vector or select local splitters,
   * i.e., every N/P-th entry of the sorted vector */
  samples = calloc(nsamples, sizeof(int));
  for ( i = 0; i < nsamples; i++ )
    samples[i] = vec[ (i + 1) * ( N / (nsamples + 1) ) ];

  /* every processor communicates the selected entries
   * to the root processor; use for instance an MPI_Gather */
  if( rank == ROOT )
    gathered_samples = calloc(nsamples * numtasks, sizeof(int));
  MPI_Gather(samples, nsamples, MPI_INT, 
	     gathered_samples, nsamples, MPI_INT,
	     ROOT, MPI_COMM_WORLD);
  free(samples);

  /* root processor does a sort, determinates splitters that
   * split the data into P buckets of approximately the same size */
  if ( rank == ROOT ) 
    qsort(gathered_samples, nsamples * numtasks, sizeof(int), compare);

  /* root process broadcasts splitters */
  splitters = calloc(numtasks-1, sizeof(int));
  if ( rank == ROOT ) {
    for( i = 0; i < numtasks-1; i++ )
      splitters[i] = gathered_samples[ (i+1) * (nsamples) + i]; 
  }
  MPI_Bcast(splitters, numtasks-1, MPI_INT, ROOT, MPI_COMM_WORLD);
  if ( rank == ROOT )
    free(gathered_samples);

  /* every processor uses the obtained splitters to decide
   * which integers need to be sent to which other processor (local bins) */
  bucket_counts = calloc(numtasks, sizeof(int));
  bucket_offset = calloc(numtasks, sizeof(int));
  final_counts = calloc(numtasks, sizeof(int));
  for ( i = 0, j = 0; i < N && j < numtasks-1; i++ ) {
    while ( j < numtasks-1 && vec[i] > splitters[j] )
      bucket_offset[++j] = i;
    bucket_counts[j] += 1;
  }
  bucket_counts[numtasks-1] = N - bucket_offset[numtasks-1];
  free(splitters);
  /*  
  printf("\nRANK %d counts: ", rank);
  for(i=0; i < numtasks; ++i)
    printf("%d ", bucket_counts[i]);
  printf("\nRANK %d offset: ", rank);
  for(i=0; i < numtasks; ++i)
    printf("%d ", bucket_offset[i]);
  */

  /* send and receive: either you use MPI_AlltoallV, or
   * (and that might be easier), use an MPI_Alltoall to share
   * with every processor how many integers it should expect,
   * and then use MPI_Send and MPI_Recv to exchange the data */
  MPI_Alltoall(bucket_counts, 1, MPI_INT, final_counts, 1, MPI_INT, MPI_COMM_WORLD);

  total_incoming = 0;
  for ( i = 0; i < numtasks; i++)
    total_incoming += final_counts[i];
  incoming = calloc(total_incoming, sizeof(int));

  MPI_Request *requests = calloc(numtasks, sizeof(MPI_Request));
  for ( i = 0; i < numtasks; i++ )
    MPI_Isend(vec + bucket_offset[i], bucket_counts[i],
	      MPI_INT, i, 999, MPI_COMM_WORLD, requests + i);

  incoming_offset = 0;
  MPI_Status status;
  for ( i = 0; i < numtasks; i++ ) {
    MPI_Recv(incoming + incoming_offset, final_counts[i], MPI_INT, 
	     i, 999, MPI_COMM_WORLD, &status);
    incoming_offset += final_counts[i];
  }

  for( i = 0; i < numtasks; i++)
    MPI_Wait(requests + i, &status);

  /* do a local sort */
  qsort(incoming, total_incoming, sizeof(int), compare);


  /*
   * Write output to a file
   */
  {
    FILE* fd = NULL;
    char filename[256];
    snprintf(filename, 256, "output%02d.txt", rank);
    fd = fopen(filename,"w+");
    if(NULL == fd) {
      printf("Error opening file \n");
      return 1;
    }
    for(i = 0; i < total_incoming; ++i)
      fprintf(fd, "%d\n", incoming[i]);

    fclose(fd);
  }


  free(vec);
  free(final_counts);
  free(bucket_counts);
  free(bucket_offset);
  free(incoming);
  free(requests);

  MPI_Finalize();

  return 0;
}
