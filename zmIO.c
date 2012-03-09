/* z'mIO.c
 *(Zoe - Markos)'s Benchmark
 *
 * Contributors:
 * Markos Fountoulakis, Zoe Sebepou, Dimitris Apostolou, Manolis Marazakis
 */
#include "license.h"

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <signal.h>
#include <getopt.h>
#include <sys/syscall.h>
#include <sched.h>
#include <pthread.h>
#include <sys/time.h>

#ifdef NO_LIBAIO
	#include "libaio.h"
#else
	#include <libaio.h>
#endif
/*#include <linux/aio.h>*//* no POSIX aio support due to limited queue depth */

#ifdef NO_MPI
	#include "nullmpi.h"
#else
	#include <mpi.h>
#endif


#define READ		0
#define WRITE		1
#define MAX_LINE 	100
#define PAT_NO   	3
#define REQ_NO   	10

enum
{
	SEQUENTIAL_PATTERN = 0,
	STRIDED_PATTERN,
	RANDOM_PATTERN
};

enum
{
	READ_OPERATION = 0,
	WRITE_OPERATION,
	READWRITE_OPERATION,
	REREAD_OPERATION,
	REWRITE_OPERATION
};

extern char *optarg;

enum
{
	SYSTEM = 1,
	SEQUENT,
	STRIDED,
	RANDOM ,
	TIME   ,
	SEEK   ,
	//REQUEST,
	//DATA   ,
	IO_OUT ,
	DIRECT ,
	SYNC   ,
	FILEIO ,
	TOUCH  ,
	HELP
};

struct option options[] = {
    { "system"  , 1, NULL, SYSTEM   },
    { "sequent" , 1, NULL, SEQUENT  },
    { "strided" , 1, NULL, STRIDED  },
    { "random"  , 1, NULL, RANDOM   },
    { "time"    , 1, NULL, TIME     },
    { "seek"    , 1, NULL, SEEK     },
//    { "request" , 1, NULL, REQUEST  },
//    { "data"    , 1, NULL, DATA     },
    { "io_out"  , 1, NULL, IO_OUT   },
    { "direct"  , 1, NULL, DIRECT   },
    { "sync"    , 1, NULL, SYNC     },
    { "file"    , 1, NULL, FILEIO   },
    { "touch"   , 1, NULL, TOUCH    },
    { "help"    , 0, NULL, HELP     },
    {0,0,0,0},
};

int MAX_NCPUS;

int data   = 0;
int io_out = 1;
int seek = 0;
int direct = 0;
int syncio = 0;
int touch = 0;
int fileio = 0;
int etime  = 1;
int filelen = 0;
int max_dev_no = 0;
int * shared;
cpu_set_t *cpu_mask;
int * option;
int * tmp_mlens;
int * tmp_dlens;
char ** machine;
char ** device;
char * system_file = NULL;
int max_request_size = 0;

static volatile int stop_benchmark = 0;
static volatile int start_benchmark = 0;

struct aio_context /* per thread asynchronus I/O context */
{
	int fd;				/* file descriptor of device */
	const char *machine_name;	/* machine pathname */
	const char *device_name;	/* pathname of file or block device */
	int shared;			/* 0 if the device is not shared */
	cpu_set_t cpu_mask;
	uint32_t block_size;
	uint64_t device_size;

	unsigned queue_depth;		/* max number of outstanding I/Os */
	unsigned outstanding_ios;	/* current number of outstanding I/Os */

	io_context_t ctx;		/* libaio context */

	struct iocb **cb_list;		/* control blocks list should contain *
					 * queue_depth number of elements     */
	struct iocb *cb_buf;		/* the actual space of the cb list */
	struct io_event *events;	/* queue_depth number of events */

	char *buffers;			/* at most queue_depth I/O buffers */
	uint32_t max_buffer_size;	/* I/O buffer maximum size */

	/* statistics */
	uint64_t total_bytes;
	uint64_t total_ios;
	uint64_t total_time_msec;

	unsigned int seed;		/* random generator seed per thread */
	int flip;
	uint64_t current_offset;
};

struct tmp_info{
	
	char ** dev;
	char * machine;
	int  depth;
	int  * shared;
	cpu_set_t *cpu_mask;
	pthread_t *threads;
};
struct pattern
{
	int type;
	int frequency;
	int read;
	int write;
	int readwrite;
	int reread;
	int rewrite;
	int unit;
	int seek_distance;
	int flip;
};

struct request
{	
	int size;
	int f;
};

int submit_aio(struct aio_context *context, uint32_t nbytes, int operation, uint64_t offset);
void complete_aio(struct aio_context *context);
struct aio_context init_work(struct tmp_info *tmpInfo);
int find_pattern( unsigned int *seedp );
int find_operation( struct pattern * pat, unsigned int *seedp );

void *do_work(void *ctx);
void do_pattern_sequential(struct aio_context *context);
void do_pattern_strided(struct aio_context *context);
void do_pattern_random(struct aio_context *context);
void (*do_pattern[])(struct aio_context *) = {do_pattern_sequential, do_pattern_strided, do_pattern_random};
void do_operation(struct aio_context *context, int req_size, int op, uint64_t offset);

struct pattern ** pats;
struct request ** requests;

int parse_args(int argc, char * argv[] );
void broadcast_args(void);
int check_pattern(int id);
int check_validity(void);
void print_args(void);
struct tmp_info * mapping(char * hostid, struct tmp_info * tmpInfo);
int resolve_pattern(int pat_id, char * pattern);
void resolve_request(int req_cnt, char * request);
struct pattern ** init_patterns(struct pattern ** pats);
struct request ** init_requests(struct request ** reqs);
void print_cpuset(int label, cpu_set_t *cpumask);

/* prints cpu_set_t ... */
void print_cpuset(int label, cpu_set_t *cpumask)
{
    int i, t = 0;

    for (i = 0; i < CPU_SETSIZE; i++) 
        if (CPU_ISSET(i, cpumask)) {
            t++;
         }

   return;
}

/* returns values in the range [0,1) */
static double my_rand(unsigned int *seedp)
{
	return ((double)rand_r(seedp)) / (((double)RAND_MAX)+1);
}

 
static void stop_handler(int signum)
{
	stop_benchmark = 1;
}

static void sigint_handler(int signum)
{
	fprintf(stderr, "*** SIGINT ***\n\n");
	stop_handler(signum);
}


void destroy_aio_context(struct aio_context *context)
{
	free(context->cb_buf);
	free(context->cb_list);
	free(context);
	return;
}

struct aio_context *create_aio_context
(
	int fd, const char *machine_name, const char *device_pathname, int shared, cpu_set_t cpu_mask, uint32_t block_size, 
	uint64_t device_size, unsigned queue_depth, uint32_t max_req_size
)
{
	struct aio_context *context;
	long alignment;

	errno = 0;
	alignment = fpathconf(fd, _PC_REC_XFER_ALIGN);
	if(-1 == alignment)
	{
		if (errno != 0)	goto alignment_error;
		alignment = getpagesize();
	}
	context = calloc(1, sizeof(struct aio_context));
	if (!context)
	{
		goto context_error;
	}
	memset(&context->ctx, 0, sizeof(io_context_t));
	if (syscall(SYS_io_setup, queue_depth, &context->ctx))
	{
		perror("io_setup");
		return NULL;
	}

	context->cb_list = calloc(queue_depth, sizeof(struct iocb *));
	if (!(context->cb_list))
	{
		goto cb_list_error;
	}

	context->cb_buf = calloc(queue_depth, sizeof(struct iocb));
	if (!(context->cb_buf))
	{
		goto cb_buf_error;
	}

	context->events = calloc(queue_depth, sizeof(struct io_event));
	if (!(context->events))
	{
		goto events_error;
	}

	context->max_buffer_size = ((max_req_size+alignment-1)/alignment)*alignment;

	if (posix_memalign((void**)(void *)&(context->buffers), alignment, queue_depth*context->max_buffer_size))
	{
		goto buffers_error;
	}
	context->fd = fd;
	context->device_name = device_pathname;
	context->machine_name = machine_name;
	context->shared = shared;
	context->cpu_mask = cpu_mask;
	context->block_size = block_size;
	context->device_size = device_size;
	context->queue_depth = queue_depth;
	context->outstanding_ios = 0;

	context->total_bytes = 0;
	context->total_ios = 0;
	context->total_time_msec = 0;
	context->current_offset = 0;
	context->seed = time(NULL); //one per thread

	return context;
	
buffers_error:
	free(context->cb_buf);
events_error:
	free(context->events);
cb_buf_error:
	free(context->cb_list);
cb_list_error:
	free(context);
context_error:
alignment_error:
	fprintf(stderr, "Memory Allocation error!\n");
	return NULL;
}

struct aio_context calculate_statistics(struct aio_context **contexts, int num_of_contexts)
{
	int i;
	struct aio_context context;

	memset(&context, 0, sizeof(struct aio_context));

	for (i = 0 ; i < num_of_contexts ; ++i)
	{
		if (context.total_time_msec == 0)
		{
			context.total_time_msec = contexts[i]->total_time_msec;
		}
		if  (context.total_time_msec < contexts[i]->total_time_msec)
		{
			context.total_time_msec =  contexts[i]->total_time_msec;
		}
		context.total_ios += contexts[i]->total_ios;
		context.total_bytes += contexts[i]->total_bytes;
	}
	return context;
}

void print_statistics(FILE *fd, struct aio_context *context)
{
	fprintf(fd, "===============================STATISTICS=======================================\n");
	fprintf(fd, "|| Total MBytes transferred = %.2lf\n", (double)(context->total_bytes / (1024 * 1024)));
	fprintf(fd, "|| Total number of I/Os = %"PRIu64"\n", context->total_ios);
	fprintf(fd, "|| Duration of experiment in seconds = %"PRIu64"\n", context->total_time_msec / 1000);
	fprintf(fd, "|| Throughput  = %.2lf (Mbytes/sec)\n", (((double)context->total_bytes)/((double)(1024 * 1024 * context->total_time_msec))) * 1000 );
	fprintf(fd, "|| IOPS  = %.2lf\n", (((double)context->total_ios)/((double)context->total_time_msec)) * 1000 );
	fprintf(fd, "==================================END===========================================\n");
}

void print_device_statistics(FILE *fd, struct aio_context *context)
{
	fprintf(fd, "================================================================================\n");
	fprintf(fd, "|| Machine = %s\n", context->machine_name);
	fprintf(fd, "|| Device = %s\n", context->device_name);
	fprintf(fd, "|| Queue depth = %u\n", context->queue_depth);
	fprintf(fd, "|| Total MBytes transfered = %.2lf\n", (double)(context->total_bytes / (1024 * 1024)));
	fprintf(fd, "|| Total number of I/Os = %"PRIu64"\n", context->total_ios);
	fprintf(fd, "|| Duration of experiment in seconds = %"PRIu64"\n", context->total_time_msec / 1000);
	fprintf(fd, "|| Throughput  = %.2lf (MBytes/sec)\n", (((double)context->total_bytes)/((double)(1024 * 1024 * context->total_time_msec))) * 1000 );
	fprintf(fd, "|| IOPS  = %.2lf \n", (((double)context->total_ios)/((double)context->total_time_msec)) * 1000 );
	fprintf(fd, "================================================================================\n");
}

static struct iocb *allocate_iocb(struct aio_context *context)
{
	int i;

	for(i = 0 ; i < context->queue_depth ; ++i)
	{
		if (context->cb_list[i] == NULL)
		{
			context->cb_list[i] = &context->cb_buf[i];
			return &context->cb_buf[i];
		}
	}
	return NULL;
}

static void free_iocb(struct aio_context *context, struct iocb* cb)
{
	context->cb_list[cb - context->cb_buf] = NULL;
}

void complete_aio(struct aio_context *context)
{
	int i;
	int num;
	struct iocb *cb;

	num = syscall(SYS_io_getevents, context->ctx, 1, context->queue_depth, context->events, NULL);
	if (num < 1)
	{
		perror("io_getevents");
		return;
	}
	for (i = 0 ; i < num ; ++i)
	{
		cb = context->events[i].obj;
		context->total_bytes += cb->u.c.nbytes;
		++(context->total_ios);
		free_iocb(context, cb);
		--(context->outstanding_ios);
	}

	return;
}

/*
 * submits an asynchronus I/O
 *
 * return 1 for busy, 0 for success, -1 for error
 */
int submit_aio(struct aio_context *context, uint32_t nbytes, int operation, uint64_t offset)
{
        struct iocb *cb;

	assert(nbytes && nbytes <= context->max_buffer_size);
	assert(operation == READ_OPERATION || operation == WRITE_OPERATION);

	if(context->outstanding_ios == context->queue_depth)
	{
		complete_aio(context);
	}

	cb = allocate_iocb(context);
	assert(cb != NULL);

	cb->aio_fildes = context->fd;
	cb->u.c.buf = context->buffers +
		      ( (cb - context->cb_buf) * context->max_buffer_size);
	cb->u.c.nbytes = nbytes;
	cb->u.c.offset = offset;

	if(touch) {
		memset(cb->u.c.buf, 0x42, nbytes);
#if 0
		int i; 
		for(i = 0 ; i < (nbytes / sizeof(double)) ; i++) { 
			*(double *) (&cb->u.c.buf[i])= 3.14159*i; 
		}
#endif
	}

	if (operation == READ_OPERATION)
	{
		cb->aio_lio_opcode = IO_CMD_PREAD;
		if (syscall(SYS_io_submit, context->ctx, 1, &cb) != 1)
		{
			free_iocb(context, cb);
			perror("aio_write");
			return -1;
		}
		++(context->outstanding_ios);
		//check_for_completions(context);
	}
	if (operation == WRITE_OPERATION)
	{
		cb->aio_lio_opcode = IO_CMD_PWRITE;
		if (syscall(SYS_io_submit, context->ctx, 1, &cb) != 1)
		{
			free_iocb(context, cb);
			perror("aio_write");
			return -1;
		}
		++(context->outstanding_ios);
		//check_for_completions(context);
	}
	return 0;
}

int init_parse_space(int size)
{
	pats = (struct pattern **)malloc(PAT_NO*sizeof(struct pattern *));
	requests = (struct request **)malloc(REQ_NO*sizeof(struct request *));

	if ( pats == NULL || requests == NULL ) {
		fprintf(stderr, "ERROR: Memory allocation error. Exiting....\n");
		return 1;
	}
	pats = init_patterns(pats);
	requests = init_requests(requests);
	return 0;
}


void broadcast_error(int *error, int rank, int size)
{
	int i;
	for (i = 0 ; i < size ; i++)
	{
		if (i != rank)
			MPI_Send(error, 1 , MPI_INT, i, 1, MPI_COMM_WORLD); 
	}
}

void listen_for_error(int *error, int rank, int size)
{
	int i;
	MPI_Status status;
	for (i=0; i < size ; ++i)
	{
		if (i != rank)
			MPI_Recv(error, 1, MPI_INT, i , 1 , MPI_COMM_WORLD, &status);
	}
}


int main(int argc,char **argv)
{
	int error = 0;
	int i, rank, size, len = MAX_LINE;
	struct tmp_info * tmpInfo;
	struct aio_context ctx;
	struct aio_context *stats_buffer;
	struct aio_context **stats;
	char * hostid;

	int tmp[1];

	MAX_NCPUS = sysconf(_SC_NPROCESSORS_ONLN);
	//if(MAX_NCPUS > 1) { --MAX_NCPUS; }
	fprintf(stderr, "MAX_NCPUS=%i\n", MAX_NCPUS);

	MPI_Status status;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	tmpInfo = (struct tmp_info *)malloc(sizeof(struct tmp_info));
	init_parse_space( size );

 	if (rank == 0){
		stats_buffer = (struct aio_context *)malloc(size*sizeof(struct aio_context));
		stats = (struct aio_context **)malloc(size*sizeof(struct aio_context *));
		for (i=0 ; i < size ; ++i)
		{
			stats[i] = &stats_buffer[i];
		}

	 	error = parse_args(argc, argv);
		MPI_Bcast(&error, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (error)
		{
			MPI_Finalize();
			exit(-1);
		}

		tmp[0] = max_dev_no;

		for ( i = 1; i < size; i++)
			MPI_Send(tmp, 1 , MPI_INT, i, 1, MPI_COMM_WORLD); 
	}
	
	if ( rank != 0 ){
		MPI_Bcast(&error, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (error)
		{
			MPI_Finalize();
			exit(-1);
		}


		MPI_Recv(tmp, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, &status);
		max_dev_no = tmp[0];

		machine = (char **)malloc(max_dev_no*sizeof(char*));
		device  = (char **)malloc(max_dev_no*sizeof(char*));
		shared  = (int  * )malloc(max_dev_no*sizeof(int));	
		tmp_mlens = (int *)malloc(max_dev_no*sizeof(int));
		tmp_dlens = (int *)malloc(max_dev_no*sizeof(int));	
		
	}
	option  = (int *)malloc((6 + 3*max_dev_no + REQ_NO*2 + PAT_NO*8 )*sizeof(int));		
	
	broadcast_args();
	hostid = (char *)malloc(len*sizeof(char));
        gethostname(hostid, len);
		
	tmpInfo = mapping( hostid, tmpInfo );
 	if ( tmpInfo == NULL ){
		printf("ERROR: %s there is no listed device for this host. Exiting....\n",hostid);
		MPI_Finalize(); 
		exit(-1);
	}	
	else{
		//print_args();
		for(i=0; i < max_dev_no; i++){
			if ( tmpInfo->dev[i] != NULL )
			printf("\nrank:%d / found device: %s",rank, tmpInfo->dev[i]);
		}

		ctx = init_work(tmpInfo);
	}

	if (rank == 0)
	{
		stats_buffer[0] = ctx;
		for (i = 1 ; i < size ; ++i)
		{
			MPI_Recv(&stats_buffer[i], sizeof(struct aio_context), MPI_BYTE,  MPI_ANY_SOURCE , MPI_ANY_TAG , MPI_COMM_WORLD, &status);
			//fprintf(stderr, "%d RECEIVED\n", rank);
		}
		ctx = calculate_statistics(stats, size);
		print_statistics(stderr, &ctx);
	}
	else
	{
		MPI_Send(&ctx, sizeof(struct aio_context) , MPI_BYTE, 0, 1 , MPI_COMM_WORLD); 
		//fprintf(stderr, "%s %d SENT\n", tmpInfo->machine, rank);
	}

	MPI_Finalize();
	return 0;
}

int parse_args( int argc, char * argv[] )
{
	
	int value, tmp;
	char * pattern;

	while ( ( value = getopt_long(argc, argv," ", options, NULL) ) >= 0 ){
		
		switch (value) {
			case SYSTEM:
				filelen = strlen(optarg);
				system_file = (char *) malloc(filelen + 1);
	    			strcpy(system_file, optarg);
	    		break;
			case SEQUENT:
				tmp = strlen(optarg);		
				pattern = (char *)malloc(tmp + 1);
				strcpy(pattern, optarg);
				if (resolve_pattern(SEQUENTIAL_PATTERN, pattern)) return 1;
				if (check_pattern(SEQUENTIAL_PATTERN)) return 1;
				free(pattern);
				pattern = NULL;
			break;
			case STRIDED:
				tmp = strlen(optarg);		
				pattern = (char *)malloc(tmp + 1);
				strcpy(pattern, optarg);
				if (resolve_pattern(STRIDED_PATTERN, pattern)) return 1;
				if (check_pattern(STRIDED_PATTERN)) return 1;
				free(pattern);
				pattern = NULL;	
			break;
			case RANDOM:
				tmp = strlen(optarg);		
				pattern = (char *)malloc(tmp + 1);
				strcpy(pattern, optarg);
				if (resolve_pattern(RANDOM_PATTERN, pattern)) return 1;
				if (check_pattern(RANDOM_PATTERN)) return 1;
				free(pattern);
				pattern = NULL;
			break;
			/*case REQUEST:
				tmp = strlen(optarg);
				if ( req_cnt >= 10 ){
					printf("Given requests > request limit (10). Exiting...\n");
					return 1;
				}
				if (strcmp(optarg,":") == 0 || strlen(optarg) == 0 ){
					printf("Unacceptable request format. Exiting...\n");
					return 1;
				}
				request = (char *)malloc(tmp + 1);
				strcpy(request, optarg);
				resolve_request(req_cnt, request);
				req_cnt +=1 ;	
			break;
			case DATA:
				data = atoi(optarg);
				if ( data <= 0 ){
                                        printf("Total amount of data must be > 0.\n");
					return 1;
                                }
			break;*/	
			case SEEK:
				seek = atoi(optarg);
				if ( seek < 0 ){
                                        printf("Seek distance must be >= 0.\n");
					return 1;
                                }	
			break;
			case TIME:
				etime = atoi(optarg);
				if ( etime < 0 ){ 
                                        printf("Execution Time limit must be > 0 in seconds.\n");
					return 1;
                                }
                        break;
			case IO_OUT:
				io_out = atoi(optarg);
				if ( io_out < 0 ){
					printf("Number of I/O outstanding requests must be > 0.\n");
					return 1;
				}
			break;
			case TOUCH:
				touch = atoi(optarg);
				printf("TOUCH: %i\n", touch);
				if ( touch != 0 && touch != 1){
					printf("Wrong indicator for \"touch data in buffer\" flag.\n");
					return 1;
				}
			break;
			case FILEIO:
				fileio = atoi(optarg);
				printf("FILEIO: %i\n", fileio);
				if ( fileio != 0 && fileio != 1){
					printf("Wrong indicator for File I/O.\n");
					return 1;
				}
			break;
			case SYNC:
				syncio = atoi(optarg);
				printf("SYNC: %i\n", syncio);
				if ( syncio != 0 && syncio != 1){
					printf("Wrong indicator for Synchronous I/O.\n");
					return 1;
				}
			break;
			case DIRECT:
				direct = atoi(optarg);
				if ( direct != 0 && direct != 1){
					printf("Wrong indicator for Direct I/O.\n");
					return 1;
				}
			break;
			case HELP:
				printf("Usage: z'mIO -- Raw device Benchmark\n\n");
				printf("options:\n");
			  	printf(" --system  filename: System's configuration file\n");
			  	printf(" --sequent \"f:no/\"|operations|\"unit:B\": Sequential access pattern\n");
			  	printf(" --strided \"f:no/\"|operations|\"unit:B\": Strided access pattern\n");
			  	printf(" --random  \"f:no/\"|operations|\"unit:B\": Random  access pattern\n");
				printf("   A pattern has frequency no%% [1,100], request size B bytes.\n");
				printf("   Operations are defined per pattern and are:\n");
				printf("   r=read, w=write, rw=read-write, rr=re-read, ww=re-write\n");
				printf("   operations=\"r:no/w:no/rw:no/rr:no/ww:no\": frequency no%% [1, 100]\n");
//			  	printf(" --data    'size' : Total amount of data in bytes ( > 0 && > block)\n");
			  	printf(" --time    value: Time limit of benchmark's execution in seconds\n");
			  	printf(" --io_out  value: Number of I/O outstanding requests\n");
			  	printf(" --direct  value: Direct I/O (0: not set / 1:set)\n");
			  	printf(" --sync    value: Synchronous I/O (0: not set / 1:set)\n");
			  	printf(" --file    value: File I/O instead of block device I/O (0: not set / 1:set)\n");
			  	printf(" --touch   value: Touch data-buffer at every I/O (0: not set / 1:set)\n");
			  	printf(" --help: Print options usage and exit\n");
				printf("\n");
				printf("The system configuration file has one line per thread accessing a device/file:\n");
				printf("hostname.mydomain.com filename\n");
				printf("\n");
				printf("===============================================================================\n");
				printf("                                  Example\n");
				printf("===============================================================================\n");
				printf("System configuration file with name myconf and contents:\n");
				printf(" localhost /dev/sda\n");
				printf("\n");
				printf("Command line with parameters:\n");
				printf(" ./zmIO --system myconf --sequent \"f:50/r:100/unit:65536\" --random \"f:50/r:30/w:70/unit:4096\" --direct 1 --time 30 --io_out 64\n");
				printf("\n");
				printf("In this run, one thread opens block device /dev/sda for direct I/O.\n");
				printf("Total run time of the experiment is 30 seconds.\n");
				printf("There are at most 64 outstanding I/Os in the device queue.\n");
				printf("There is 50%% chance for an operation to be either sequential or random.\n");
				printf("In the sequential case streaming reads of 64KB are performed.\n");
				printf("In the random case, there is 30%% reads and 70%% writes for 4KB accesses.\n");


			return 1;
			break;
		}
	}
	return check_validity();
}

void broadcast_args(void)
{

	int rank, size, i=0, j=0, k=1;
	
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	if ( rank == 0 ){				//copy to option array the parsed arguments for easy broadcast
		option[j++] = filelen;
		option[j++] = data;
		option[j++] = etime;
		option[j++] = seek; //IMPROVE
		option[j++] = io_out;
		option[j++] = direct;
		
		for( i=0; i < PAT_NO; i++){
			option[j++] = pats[i]->type;
			option[j++] = pats[i]->frequency;
			option[j++] = pats[i]->read;
			option[j++] = pats[i]->write;
			option[j++] = pats[i]->readwrite;
			option[j++] = pats[i]->reread;
			option[j++] = pats[i]->rewrite;
			option[j++] = pats[i]->unit;

			if (i == STRIDED_PATTERN ){
				pats[i]->seek_distance = seek;
			}
			if (max_request_size == 0)
				max_request_size = pats[i]->unit;
			else
				max_request_size = (max_request_size < pats[i]->unit) ? pats[i]->unit : max_request_size;
		} 
		for (i=0; i < REQ_NO; i++){
			option[j++] = requests[i]->size;
			option[j++] = requests[i]->f;
		}
		for( i=0; i < max_dev_no; i++ )
			option[j++] = shared[i];
 
		for( i=0; i < max_dev_no; i++) 		//lenghts of the machine and device names
			option[j++] = tmp_mlens[i];
		
		for( i=0; i < max_dev_no; i++) 
			option[j++] = tmp_dlens[i];
	}

	MPI_Bcast(option, ( 6 + 3*max_dev_no + REQ_NO*2 + PAT_NO*8 ), MPI_INT, 0, MPI_COMM_WORLD);
	
    	if ( rank != 0 && option[0] != 0 ){	//if you are not proc 0, then allocate space for the string arguments(machines-devices)
		filelen = option[0];
		system_file = (char *)malloc( filelen + 1);
		
		for ( i=0; i < max_dev_no; i++ ){
			tmp_mlens[i] = option[6 + REQ_NO*2 + PAT_NO*8 + max_dev_no + i];
			machine[i] = (char *)malloc( tmp_mlens[i] + 1 );
		}
		for ( i=0; i < max_dev_no; i++ ){	
			tmp_dlens[i] = option[6 + REQ_NO*2 + PAT_NO*8 + 2*max_dev_no + i];
			device[i] = (char *)malloc( tmp_dlens[i] + 1 );
		}
	}
									//Broadcast the string arguments
    	MPI_Bcast(system_file, filelen, MPI_CHAR, 0, MPI_COMM_WORLD);
	
	for ( i=0; i < max_dev_no; i++ ){
	    	MPI_Bcast(machine[i], tmp_mlens[i], MPI_CHAR, 0, MPI_COMM_WORLD);
	    	MPI_Bcast( device[i], tmp_dlens[i], MPI_CHAR, 0, MPI_COMM_WORLD);		
	}
									//At the end, copy in permanent places the arguments that you have read
	if ( rank != 0 ){
		data   = option[k++];
		etime  = option[k++];
		seek   = option[k++];
		io_out = option[k++];
		direct = option[k++];		

		for ( i=0; i < PAT_NO; i++){
			pats[i]->type = option[k++];
			pats[i]->frequency = option[k++];
			pats[i]->read = option[k++];
			pats[i]->write = option[k++];
			pats[i]->readwrite = option[k++];
			pats[i]->reread = option[k++];
			pats[i]->rewrite = option[k++];
			pats[i]->unit = option[k++];

			if (i == STRIDED_PATTERN ){
				pats[i]->seek_distance = seek;
			}
			if (max_request_size == 0)
				max_request_size = pats[i]->unit;
			else
				max_request_size = (max_request_size < pats[i]->unit) ? pats[i]->unit : max_request_size;
		}
		for ( i=0; i < REQ_NO; i++){
			requests[i]->size = option[k++];
			requests[i]->f = option[k++];		
		}
		for ( i=0; i < max_dev_no; i++)
			shared[i] = option[k++];
	}
}
void print_args(void)
{
	
	int my_rank, size, i;
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	
	printf("\n-------RANK: %d --------\n\n", my_rank);
	printf("system file: %s\n",system_file);
	printf("data       : %d\n",data);
	printf("io_out  : %d\n",io_out);
	printf("direct  : %d\n",direct);
		
	for ( i=0; i < PAT_NO; i++){
		printf("\nPattern id = %d\n", i);
		printf("frequency : %d",pats[i]->frequency);
		printf(" read     : %d", pats[i]->read);
		printf(" write    : %d", pats[i]->write);
		printf(" readwrite: %d", pats[i]->readwrite);	
		printf(" reread   : %d", pats[i]->reread);	
		printf(" rewrite  : %d", pats[i]->rewrite);
		printf(" unit     : %d", pats[i]->unit);
		printf(" flip     : %d", pats[i]->flip);
	}
	/*for ( i=0; i < REQ_NO; i++){
		printf("\nRequest = %d\n", i);
		printf("size: %d",requests[i]->size);
		printf(" f   : %d",requests[i]->f);
	}*/
		
}

struct tmp_info * mapping(char * hostid, struct tmp_info * tmpInfo )
{
	
	int i=0, size, j=0;
	int found = 0;

	MPI_Comm_size(MPI_COMM_WORLD, &size);

	tmpInfo = malloc(sizeof(struct tmp_info));
	tmpInfo->dev = (char **)malloc((max_dev_no+1)*sizeof(char *)); //last one should be NULL
	tmpInfo->cpu_mask = (cpu_set_t *)malloc(max_dev_no*sizeof(cpu_set_t));
	tmpInfo->shared = (int *)malloc(max_dev_no*sizeof(int));
	tmpInfo->threads = (pthread_t *)malloc(max_dev_no*sizeof(pthread_t));
	tmpInfo->machine = malloc(sizeof(char)*(strlen(hostid)+1)); 
	strcpy(tmpInfo->machine, hostid);

	for ( i=0; i < max_dev_no + 1; i++){
		
		tmpInfo->dev[i] = NULL;
	}
	for ( i=0; i < max_dev_no; i++ ){
	
		if ( strcmp(machine[i],hostid) == 0 ){			//find your device if you are listed in conf file
			tmpInfo->dev[j] = malloc(sizeof(char)*(strlen(device[i])+1)); 
			
			strcpy(tmpInfo->dev[j], device[i]);
			tmpInfo->shared[j] = shared[i];
			found = 1;
                        j++;
		}	
	}
	if ( found == 0 ) return NULL;
	
	return tmpInfo;
}

struct pattern ** init_patterns(struct pattern ** pats)
{
	
	int i=0;

	for( i=0; i < PAT_NO; i++){
		pats[i] = (struct pattern *)malloc(sizeof(struct pattern));
		pats[i]->type = -1;
		pats[i]->frequency = 0;
		pats[i]->read  = 0;
		pats[i]->write = 0;
		pats[i]->readwrite= 0;
		pats[i]->reread = 0;
		pats[i]->rewrite = 0;
		pats[i]->unit = 0;
	}
	return pats;
}

struct request ** init_requests(struct request ** reqs)
{
	int i=0;

	for( i=0; i < REQ_NO; i++){
		reqs[i] = (struct request *)malloc(sizeof(struct request));
		reqs[i]->size = 0;
		reqs[i]->f    = 0;
	}
	return reqs;
}

int check_pattern(int id)
{	
	int sum;
	
	sum = pats[id]->read + pats[id]->write + pats[id]->readwrite + pats[id]->reread + pats[id]->rewrite;

	if ( sum < 0 || sum != 100){
		if ( id == 0 )
			printf("Sequential pattern: Operation's frequency must sum to 100.Exiting...\n");
		else if ( id == 1 )
			printf("Strided pattern: Operation's frequency must sum to 100.Exiting...\n");			
		else if ( id == 2 )
			printf("Random pattern: Operation's frequency must sum to 100.Exiting...\n");
		return 1;		
	}
	return 0;
}

int check_validity(void)
{
	FILE * fp;

	int   i=0, size, error=0;
	char *line = malloc(MAX_LINE*sizeof(char));
	char *token = NULL;
	int   pattern_prob=0;
	cpu_set_t mask;

	MPI_Comm_size(MPI_COMM_WORLD, &size);
	
	if ( (fp = fopen(system_file,"r")) == NULL ){
		printf("Error: No system's configuration file found. Exiting...\n");
		return 1;
	}
	else{
		while ( (fgets(line,MAX_LINE,fp) != NULL) ){ max_dev_no++; }
		fclose(fp);
	
		tmp_mlens = (int *)malloc(max_dev_no*sizeof(int));
		tmp_dlens = (int *)malloc(max_dev_no*sizeof(int));	
		machine = (char **)malloc(max_dev_no*sizeof(char*));
		device  = (char **)malloc(max_dev_no*sizeof(char*));
		shared  = (int  * )malloc(max_dev_no*sizeof(int));
		cpu_mask  = (cpu_set_t * )malloc(max_dev_no*sizeof(cpu_set_t));
		
           	fp = fopen(system_file,"r");		//get the names of the machines/devices/shared_dev from the config file
							
		 while ( (fgets(line,MAX_LINE,fp) != NULL) ){

			token = strtok(line," ");
			if ( token == NULL){ error = 1; break;}
			tmp_mlens[i] = strlen(token);
			machine[i] = (char *)malloc((strlen(token)+1)*sizeof(char));
			strcpy(machine[i],token);						
			//fprintf(stderr, "machine[%i]=[%s]\n", i, token);

			token = strtok( NULL," \n");
			if(token == NULL) {
				token = strtok( NULL," ");
			}
			if ( token == NULL){ error = 1; break;}						
			tmp_dlens[i] = strlen(token);				
			device[i] = (char *)malloc((strlen(token)+1)*sizeof(char));
			strcpy(device[i],token);
			//fprintf(stderr, "device[%i]=[%s]\n", i, token);

			token = strtok ( NULL," ");
/*			if ( token == NULL){ error = 1; break;}
			if ( atoi(token) != 0 && atoi(token) != 1 ){
				printf("Wrong indicator of shared or not device. Exiting....\n");
				return 1;
			}
			shared[i] = atoi(token);*/

			token = strtok ( NULL," \n");
			CPU_ZERO(&mask); 
			if ( token != NULL){ 
				CPU_SET(atoi(token) % MAX_NCPUS, &mask); 
			}
			cpu_mask[i] = mask;
			//fprintf(stderr, "cpu_mask[%i]=0x%X (mask=0x%X, token=%s) \n", i, cpu_mask[i], mask, token);

			i++;
	        }
	        fclose(fp);
	}
	if ( error == 1 ){
		printf("Error: In system file parsing. Exiting...\n");		
		return 1;
	}
	//see if the probabilities sum in 100
	pattern_prob = 0;
	for( i=0; i < PAT_NO; i++){
	//pattern_prob = pats[0]->frequency + pats[1]->frequency + pats[2]->frequency;
		pattern_prob += pats[i]->frequency;
	}
	
	if ( pattern_prob != 100 ){
		printf("Error: Pattern's aggregate probability must be 100. Exiting...\n");		
		return 1;
	}
		
/*	for (i=0; i < REQ_NO; i ++) request_prob += requests[i]->f;
	
	if ( request_prob != 100 ){
		printf("Error: Request aggregate probability must sum to 100. Exiting...\n");
                MPI_Finalize();		
		exit(-1);
	}
*/
	return 0;
}

int parse_token(int pat_id, char * token)
{
	
	int tmp;
	char * value;

	value = strtok(token,":");
	pats[pat_id]->flip = 100;
	if ( strcmp(value,"f") == 0 ){
		value = strtok(NULL,":");	
		tmp = atoi(value);
		if ( tmp < 0 || tmp > 100 ){
			printf("Error: pattern frequency must be >= 0 && < 100. Exiting...\n");
			return 1;
		}	
		pats[pat_id]->frequency = tmp;
	}
	else if ( strcmp(value,"r") == 0){
		value = strtok(NULL,":");	
		tmp = atoi(value);
		if ( tmp < 0 || tmp > 100 ){
			printf("Error: read frequency must be >= 0 && < 100. Exiting...\n");
			return 1;
		}	
		pats[pat_id]->read = tmp;
	} 
	else if ( strcmp(value,"w") == 0){
		value = strtok(NULL,":");	
		tmp = atoi(value);
		if ( tmp < 0 || tmp > 100 ){
			printf("Error: write frequency must be >= 0 && < 100. Exiting...\n");
			return 1;
		}	
		pats[pat_id]->write = tmp;
	}
	else if ( strcmp(value,"rw") == 0){
		value = strtok(NULL,":");	
		tmp = atoi(value);
		if ( tmp < 0 || tmp > 100 ){
			printf("Error: read-write frequency must be >= 0 && < 100. Exiting...\n");
			return 1;
		}	
		pats[pat_id]->readwrite = tmp;
	}
	else if ( strcmp(value,"rr")==0){
		value = strtok(NULL,":");	
		tmp = atoi(value);
		if ( tmp < 0 || tmp > 100 ){
			printf("Error: re-read frequency must be >= 0 && < 100. Exiting...\n");
			return 1;
		}	
		pats[pat_id]->reread = tmp;
	}
	else if ( strcmp(value,"ww")==0){
		value = strtok(NULL,":");	
		tmp = atoi(value);
		if ( tmp < 0 || tmp > 100 ){
			printf("Error: re-write frequency must be >= 0 && < 100. Exiting...\n");
			return 1;
		}	
		pats[pat_id]->rewrite = tmp;
	}
	else if ( strcmp(value,"unit")==0){
		value = strtok(NULL,":");	
		tmp = atoi(value);
		if ( tmp < 0  ){
			printf("Request size must be > 0. Exiting...\n");
			return 1;
		}	
		pats[pat_id]->unit = tmp;
	}
	else if ( strcmp(value,"flip")==0){
		value = strtok(NULL,":");	
		tmp = atoi(value);
		if ( tmp < 0 || tmp > 100 ){
			printf("Error: flip prob. must be >= 0 && < 100. Exiting...\n");
			return 1;
		}	
		pats[pat_id]->flip = tmp;
	}
	else{
		printf("Error: unacceptable pattern format: %s. Exiting...\n", value);
		return 1;
	}
	return 0;
}

int resolve_pattern(int pat_id, char * pattern)
{
	
	int i , cnt=0;
	char * token[6];

	memset(pats[pat_id], 0, sizeof(struct pattern));
	pats[pat_id]->type = pat_id;

	if (pattern[0] != 'f' ){
		printf("Not valid pattern format given (pattern frequency missing).Exiting...\n");
		return 1;
	}
	else{
		token[cnt] = strtok(pattern,"/");
		cnt += 1;
		while ( token[cnt-1] != NULL && cnt < 7 ){
			token[cnt] = strtok( NULL,"/");
			if ( token[cnt] == NULL) break;
			cnt += 1;
		}			
	}
	if ( cnt < 3 || cnt > 7 ){
		printf("Not valid pattern format given.Exiting...\n");
		return 1;
	}
	else{
		for (i=0; i < cnt; i++){
			if (parse_token(pat_id, token[i])) return 1;
		}
	}
	return 0;
}

void resolve_request(int req_cnt, char * request)
{
	
	int cnt=0;
	int tmp;
	char * token[2];

	token[cnt] = strtok(request,":");
	tmp = atoi(token[cnt]);

	if ( tmp <= 0 || strlen(token[cnt]) > 9){
		printf("Not valid request size %d. Exiting...\n", tmp);
		MPI_Finalize();
		exit(-1);
	}
	requests[req_cnt]->size = tmp;

	cnt += 1;
	while ( token[cnt-1] != NULL && cnt < 2 ){
		token[cnt] = strtok( NULL,":");
		if ( token[cnt] == NULL) break;
		tmp = atoi(token[cnt]);
		if ( tmp <= 0 ){
			printf("Not valid request frequency %d. Exiting...\n", tmp);
			MPI_Finalize();
			exit(-1);
		}
		requests[req_cnt]->f = tmp;		
		cnt += 1;
	}			
	if ( cnt == 1 ){
		printf("Not valid request format given.Exiting...\n");
		MPI_Finalize();
		exit(-1);
	}
}

/* get a random number between 1 to 100
 * if for example the command line was:
 * --sequent 60 --strided 20 --random 20
 * then,
 * sequent is chosen for rand values 1 to 60
 * strided is chosen for rand values 61 and 80
 * random is chosen for rand values 81 and 100
 * etc...
 *
 * function returns 0 if sequential is selected, 1 for strided and 2 for random 
 */
int find_pattern( unsigned int *seedp ){

	int value;
	int seq_start=0, seq_end=0;
	int str_end=0;
	int rand_end=0;
	

        value = ((int)(my_rand(seedp)*100.0)) + 1;
        assert( value >= 1 && value <= 100);

	//printf("value is:%d\n",value);
	
	seq_start = 1;
	seq_end   = pats[SEQUENTIAL_PATTERN]->frequency;
	str_end   = seq_end + pats[STRIDED_PATTERN]->frequency;
	rand_end  = str_end + pats[RANDOM_PATTERN]->frequency;

	if ( value >= seq_start && value <= seq_end ) return SEQUENTIAL_PATTERN;
	else if ( value > seq_end && value <= str_end ) return STRIDED_PATTERN;
	else return RANDOM_PATTERN;
	
	return -1;
}
int find_operation( struct pattern * pat, unsigned int *seedp){

	int value;
	int r_start=0, r_end=0;
	int w_end=0, rw_end=0, rr_end=0, ww_end=0;
	
	value = ((int)(my_rand(seedp)*100.0)) + 1;
        assert( value >= 1 && value <= 100);

     	//printf("operation value is:%d\n",value);
	
	r_start = 1;
	r_end   = pat->read;
	w_end   = r_end + pat->write;
	rw_end = w_end + pat->readwrite;
	rr_end  = rw_end + pat->reread;
	ww_end  = rr_end  + pat->rewrite;

	if ( value >= r_start && value <= r_end )     return READ_OPERATION;
	else if (value > r_end   && value <= w_end  ) return WRITE_OPERATION;
	else if (value > w_end   && value <= rw_end) return READWRITE_OPERATION;
	else if (value > rw_end && value <= rr_end ) return REREAD_OPERATION;
	else if (value > rr_end  && value <= ww_end ) return REWRITE_OPERATION;
	else fprintf(stderr, "ERROR\n");
	return -1;
}

int find_request( void ){
	
	int i = 0;
	return i;
	
}

struct aio_context init_work(struct tmp_info *tmpInfo)
{
	uint32_t block_size;
	uint64_t dev_size;
	uint32_t max_sect;
	int fd;
	int i;
	int num_of_devs;
	struct aio_context ctx;
	struct aio_context *context;
	struct aio_context *stats[64]; //IMPROVE

        assert(tmpInfo->dev[0]);
	for (i=0; tmpInfo->dev[i] != NULL; ++i)
	{
		int flags = O_RDWR;
		if (direct) flags |= O_DIRECT;
		if (syncio) flags |= O_SYNC;
		fd = open(tmpInfo->dev[i], flags);
		printf(" opening dev : .[%s].\n", tmpInfo->dev[i]);
		if (fd == -1)
		{
			perror("open");
			MPI_Finalize();
			exit(1); 
		}
		if(!fileio) {
			if (ioctl(fd, BLKSSZGET, &block_size) == -1)
			{
				perror("ioctl");
				MPI_Finalize();
				exit(1); 
			}
			printf("BLOCK SIZE = %u\n", block_size);
			if (ioctl(fd, BLKSECTGET, &max_sect) == -1)
			{
				perror("ioctl");
				MPI_Finalize();
				exit(1); 
			}
			printf("MAX SECTORS = %u\n", max_sect);
			if (ioctl(fd, BLKGETSIZE64, &dev_size) == -1)
			{
				perror("ioctl");
				MPI_Finalize();
				exit(1); 
			}
			printf("DEVICE SIZE = %"PRIu64"\n", dev_size);
		} else {
			struct stat file_stat;

			if(stat(tmpInfo->dev[i], &file_stat) == -1) {
				perror("stat");
				MPI_Finalize();
				exit(1); 
			}
			block_size = (uint32_t) file_stat.st_blksize;
			printf("FS BLOCK SIZE = %"PRIu32"\n", block_size);
			dev_size = (uint64_t) file_stat.st_size;
			printf("SIZE of TARGET FILE = %"PRIu64"\n", dev_size);
		}

		context = create_aio_context(fd, tmpInfo->machine, tmpInfo->dev[i], tmpInfo->shared[i], tmpInfo->cpu_mask[i], block_size, dev_size, io_out, max_request_size);
		if (!context)
		{
			fprintf(stderr, "ERROR: create_aio_context() failed!!!\n");
		}
		if ( pthread_create(&tmpInfo->threads[i], NULL, do_work, context) )
		{
			perror("pthread_create");
			MPI_Finalize();
			exit(1); 
		}
	}
	num_of_devs = i;

	start_benchmark = 1; //start threads
        printf("etime = %d\n", etime);
	signal(SIGALRM, stop_handler);
	signal(SIGINT, sigint_handler);
	alarm(etime);

	for (i=0; i<num_of_devs; ++i)
	{
                fprintf(stderr, "JOINED\n");
		pthread_join(tmpInfo->threads[i], (void **)&stats[i]);
		print_device_statistics(stderr, stats[i]);
	}		

	ctx = calculate_statistics(stats, num_of_devs);
	//print_statistics(stdout, &ctx);

	return ctx;
}

void *do_work(void *ctx)
{
//	int i;
	struct timeval start_time, end_time;
	long milliseconds;

	struct aio_context *context = (struct aio_context *)ctx;
	//fprintf(stderr, "cpu_mask=0x%08lX\n", context->cpu_mask);
	//print_cpuset((int)context, &context->cpu_mask);
	sched_setaffinity(0, sizeof(cpu_set_t), &context->cpu_mask);

	while(!start_benchmark)
	{
		pthread_yield();
	}

	gettimeofday(&start_time, NULL);

	while(!stop_benchmark)
	{
         	do_pattern[find_pattern(&context->seed)](context);
	}
        fprintf(stderr,"%p: THREAD 4\n",ctx);

        stop_benchmark = 1;

//	for(i=0; i<context->queue_depth ;++i){
//		complete_aio(context);
//	}
	gettimeofday(&end_time, NULL);
	milliseconds = (end_time.tv_sec - start_time.tv_sec) * 1000;
	milliseconds += (end_time.tv_usec - start_time.tv_usec) / 1000;
	context->total_time_msec = milliseconds;

//	printf("Thread number %d\n", gettid());
	pthread_exit(ctx);
	return ctx;
}
void do_pattern_sequential(struct aio_context *context)
{
	int op = find_operation(pats[SEQUENTIAL_PATTERN], &context->seed);
	int req_size = pats[SEQUENTIAL_PATTERN]->unit;
	uint64_t *offset_p;

	offset_p = &context->current_offset;

		*offset_p = ((*offset_p + req_size) > context->device_size ) ? 0 : *offset_p;
		do_operation(context, req_size , op, *offset_p);
		*offset_p += req_size;
}
void do_pattern_random(struct aio_context *context)
{
	int op = find_operation(pats[RANDOM_PATTERN], &context->seed);
	int req_size = pats[RANDOM_PATTERN]->unit;
	uint64_t offset;

	do
	{
		offset = ((uint64_t)((my_rand(&context->seed)) * (context->device_size/context->block_size) )) * context->block_size;
	}
	while(offset+req_size > context->device_size);

	do_operation(context, req_size , op, offset);
}
void do_pattern_strided(struct aio_context *context)
{
	int rvalue;
	int op = find_operation(pats[STRIDED_PATTERN], &context->seed);
	int req_size = pats[STRIDED_PATTERN]->unit;
	uint64_t *offset_p;
	int seek_distance = pats[STRIDED_PATTERN]->seek_distance;

	offset_p = &context->current_offset;

	*offset_p = ((*offset_p + req_size) > context->device_size ) ? 0 : *offset_p;
	do_operation(context, req_size , op, *offset_p);

        rvalue = ((int)(my_rand(&context->seed)*100.0)) + 1;
        assert( rvalue >= 1 && rvalue <= 100);
	*offset_p += req_size;
	if(rvalue <= pats[STRIDED_PATTERN]->flip) {
		//*offset_p += req_size + seek_distance;
		*offset_p += seek_distance;
	}
}

void do_operation(struct aio_context *context, int req_size, int op, uint64_t offset)
{
	switch(op)
	{
	case READ_OPERATION: case WRITE_OPERATION:
		submit_aio(context, req_size, op, offset);
	break;
	case READWRITE_OPERATION:
		submit_aio(context, req_size, READ_OPERATION, offset);
		submit_aio(context, req_size, WRITE_OPERATION, offset);
	break;
	case REREAD_OPERATION:
		submit_aio(context, req_size, READ_OPERATION, offset);
		submit_aio(context, req_size, READ_OPERATION, offset);
	break;
	case REWRITE_OPERATION:
		submit_aio(context, req_size, WRITE_OPERATION, offset);
		submit_aio(context, req_size, WRITE_OPERATION, offset);
	break;
	default:
		fprintf(stderr, "do_operation: Unsupported operation (%d)!!!\n", op);
	break;
	}
}
