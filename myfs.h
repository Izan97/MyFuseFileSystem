//#include "fs.h"
#include <uuid/uuid.h>
#include <unqlite.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <fuse.h>
#include <stdbool.h>
#include <math.h>
#include <limits.h>

#define MY_MAX_PATH 255 // default max
// Any serious desktop or server OS nowadays uses 
//64 bits for file sizes and offsets, which puts the limit at 8EB.
//#define MY_MAX_FILE_SIZE 10000000 // ~10mbs
#define MY_MAX_FILES_PER_FOLDER 200

// This is a starting File Control Block for the 
// simplistic implementation provided.
//
// It combines the information for the root directory "/"
// and one single file inside this directory. This is why there
// is a one file limit for this filesystem
//
// Obviously, you will need to be change this into a
// more sensible FCB to implement a proper filesystem

// typedef struct mydirent{
// //    char** file_names;
//     uuid_t fcb_pointers[MY_MAX_FILES_PER_FOLDER];
// }mydirent;

typedef union fcb_content{
    uuid_t file_data_uuid;
    // max files per folder max that we can put in a long 2^64 - 1 ?
    uuid_t fcb_pointers[MY_MAX_FILES_PER_FOLDER];
}fcb_content;

//need to have bit for dir
// 2d array of names
// 2d array of inode*
// * to first data block ?
// THIS IS INODE
typedef struct _myfcb {
    char path[MY_MAX_PATH];
//    bool isReg;
    uuid_t fcb_uuid;
    fcb_content contents;
    // see 'man 2 stat' and 'man 2 chmod'
    //meta-data for the 'file'
    uid_t  uid;     /* user */
    gid_t  gid;     /* group */
    mode_t mode;    /* protection */
    time_t atime;   /* last access time */
    time_t mtime;   /* time of last modification */
    time_t ctime;   /* time of last change to meta-data (status) */
    off_t size;     /* size */
} myfcb;

// Some other useful definitions we might need

extern unqlite_int64 root_object_size_value;

// We need to use a well-known value as a key for the root object.
#define ROOT_OBJECT_KEY "rootrootrootroot"

// This is the size of a regular key used to fetch things from the 
// database. We use uuids as keys, so 16 bytes each
#define KEY_SIZE 16

// The name of the file which will hold our filesystem
// If things get corrupted, unmount it and delete the file
// to start over with a fresh filesystem
#define DATABASE_NAME "myfs.db"

extern unqlite *pDb;

extern void error_handler(int);
void print_id(uuid_t *);

extern FILE* init_log_file();
extern void write_log(const char *, ...);

extern uuid_t zero_uuid;

// We can use the fs_state struct to pass information to fuse, which our handler functions can
// then access. In this case, we use it to pass a file handle for the file used for logging
struct myfs_state {
    FILE *logfile;
};
#define NEWFS_PRIVATE_DATA ((struct myfs_state *) fuse_get_context()->private_data)




// Some helper functions for logging etc.

// In order to log actions while running through FUSE, we have to give
// it a file handle to use. We define a couple of helper functions to do
// logging. No need to change this if you don't see a need
//

FILE *logfile;

// Open a file for writing so we can obtain a handle
FILE *init_log_file(){
    //Open logfile.
    logfile = fopen("myfs.log", "w");
    if (logfile == NULL) {
		perror("Unable to open log file. Life is not worth living.");
		exit(EXIT_FAILURE);
    }
    //Use line buffering
    setvbuf(logfile, NULL, _IOLBF, 0);
    return logfile;
}

// Write to the provided handle
void write_log(const char *format, ...){
    va_list ap;
    va_start(ap, format);
    vfprintf(NEWFS_PRIVATE_DATA->logfile, format, ap);
}

// Simple error handler which cleans up and quits
void error_handler(int rc){
    write_log("FATAL ERROR !!!\n");
	if( rc != UNQLITE_OK ){
		const char *zBuf;
		int iLen;
		unqlite_config(pDb,UNQLITE_CONFIG_ERR_LOG,&zBuf,&iLen);
		if( iLen > 0 ){
			perror("error_handler: ");
			perror(zBuf);
		}
		if( rc != UNQLITE_BUSY && rc != UNQLITE_NOTIMPLEMENTED ){
			/* Rollback */
			unqlite_rollback(pDb);
		}
		exit(rc);
	}
}

void print_id(uuid_t *id){
 	size_t i; 
    for (i = 0; i < sizeof *id; i ++) {
        printf("%02x ", (*id)[i]);
    }
}

