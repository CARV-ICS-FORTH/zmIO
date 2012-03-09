
#define MPI_INT		0
#define MPI_CHAR	0
#define MPI_BYTE	0
#define MPI_COMM_WORLD	0
#define MPI_ANY_TAG	0
#define MPI_ANY_SOURCE  0
typedef int MPI_Status;

static int MPI_Init(void *a, void *b)
{
	return 0;
}

static int MPI_Comm_rank(int a, int *rank)
{
	*rank = 0;
	return 0;
}
static int MPI_Comm_size(int a, int *size)
{
	*size = 1;
	return 0;
}

static void MPI_Finalize(void )
{
	return;
}
static int MPI_Send(void * a, int b , int c, int d, int e, int f)
{
	return 0;
} 
static int MPI_Recv(void * a, int b , int c, int d, int e, int f, int *g)
{
	return 0;
} 
static int MPI_Bcast(void *a, int b, int c, int d, int e)
{
	return 0;
}

