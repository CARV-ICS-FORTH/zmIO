nompi:
	gcc -O3 -D__USE_LARGEFILE64 -DNO_MPI -D_GNU_SOURCE -Wall -lpthread  zmIO.c -o zmIO

nompi_nolibaio:
	gcc -O3 -D__USE_LARGEFILE64 -DNO_MPI -D_GNU_SOURCE -DNO_LIBAIO -Wall -lpthread  zmIO.c -o zmIO
	
##########################################
#MPI support not functional at the moment#
##########################################
#mpi:
#	 /usr/local/mpich2-1.0.5p3/bin/mpicc -O3 -D__USE_LARGEFILE64 -D_GNU_SOURCE -Wall -lpthread -laio zmIO.c -o zmIO
#
clean:
	rm -rf zmIO
