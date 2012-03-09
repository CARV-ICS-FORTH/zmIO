# z'mIO

z'mIO is mainly a block device benchmark with emphasis on asynchronous I/O in the Linux kernel. The benchmark also works on regular files.

The Linux `libaio` API is used instead of the POSIX `aio` interface due to the low queue depths of the Linux POSIX `aio` implementation.

If you don't have the `libaio` header files installed, you should put `libaio.h` in the benchmark directory and execute: `make nompi_nolibaio`

For more information execute: `./zmIO --help`

**This code is solely the property of FORTH_ICS and is provided under a License from the Foundation of Research and Technology - Hellas (FORTH), Institute of Computer Science (ICS), Greece. Downloading this software implies accepting the [provided license](zmIO_license.pdf).**
