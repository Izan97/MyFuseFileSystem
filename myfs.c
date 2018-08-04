/*
  MyFS. One directory, one file, 1000 bytes of storage. What more do you need?
  
  This Fuse file system is based largely on the HelloWorld example by Miklos Szeredi <miklos@szeredi.hu> (http://fuse.sourceforge.net/helloworld.html). Additional inspiration was taken from Joseph J. Pfeiffer's "Writing a FUSE Filesystem: a Tutorial" (http://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/).
*/
// can try change everything without pointers
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include "myfs.h"

// The one and only fcb that this implmentation will have. We'll keep it in memory. A better
// implementation would, at the very least, cache it's root directroy in memory.
myfcb the_root_fcb;
unqlite_int64 root_object_size_value = sizeof(myfcb);

// This is the pointer to the database we will use to store all our files
unqlite *pDb;
uuid_t zero_uuid;

// The functions which follow are handler functions for various things a filesystem needs to do:
// reading, getting attributes, truncating, etc. They will be called by FUSE whenever it needs
// your filesystem to do something, so this is where functionality goes.

//print uuid for logging
void print_uuid(uuid_t uuid)
{
	char uuid_str[37];
	uuid_unparse(uuid, uuid_str);
	write_log("UUID IS: %s\n", uuid_str);
}

// fetch data from db
myfcb fetch_uuid_data(uuid_t data_id)
{
	if (*data_id != 0)
	{
		myfcb target;
		unqlite_int64 nBytes;
		int rc = unqlite_kv_fetch(pDb, data_id, KEY_SIZE, &target, &nBytes);
		write_log("target found path is: %s\n", target.path);
		if (rc != UNQLITE_OK)
		{
			write_log("Not correct fcb required from db\n");
		}
		return target;
	}
	write_log("FCB not found in dir\n");
}

// search hierarchy for FCB given a path
myfcb find_fcb_from_path(const char *path)
{
	write_log("Final path to find is: %s\n", path);
	myfcb target;
	char curr_path[MY_MAX_PATH]; // null terminate (file path atm. in file system)
	memset(curr_path, '\0', sizeof(curr_path));
	write_log("curr path is:%s\n", curr_path);
	char path_cpy[strlen(path) + 1];
	strcpy(path_cpy, path);
	path_cpy[strlen(path)] = '\0';
	write_log("path_cpy is:%s with len %d\n", path_cpy, strlen(path_cpy) + 1);
	char *token = strtok(path_cpy, "/"); // tokenize path
	write_log("First token is %s\n", token);

	// go through hierarchy levels
	for (target = the_root_fcb;
		 strcmp(path, target.path);
		 token = strtok(NULL, "/"))
	{
		if (token == NULL)
			break;
		if (S_ISDIR(target.mode)) // search if dir
		{
			int i = 0; // counter
			strcat(curr_path, "/");
			strcat(curr_path, token); // add next part of path
			curr_path[strlen(curr_path)] = '\0';
			write_log("Path to find is %s\n", curr_path);
			myfcb try_blk; // blok to try hit correct
			//go through all subfiles in dir
			write_log("parent's path is %s\n", target.path);
			print_uuid(target.fcb_uuid);
			// values or pointers here ???+
			while (i < MY_MAX_FILES_PER_FOLDER)
			{
				write_log("possible match is:...\n");
				print_uuid(target.contents.fcb_pointers[i]);
				try_blk = fetch_uuid_data(target.contents.fcb_pointers[i++]);
				// check if we got the correct child fcb
				if (strcmp(curr_path, try_blk.path) == 0)
				{
					target = try_blk;
					break;
				}
			}
		}
	}
	write_log("last token is: %s\n", token);
	write_log("returning is %s\n", target.path);
	if (strcmp(the_root_fcb.path, target.path) == 0)
		return the_root_fcb;
	else
		return target;
}

myfcb find_parent_from_child_path(const char *path)
{
	char new_path[MY_MAX_PATH] = "/"; // parent path start with root
	//	char *curr_pos = new_path;
	int level_count = 0; // num of "/"
	char path_cpy[strlen(path) + 1];
	strcpy(path_cpy, path);
	path_cpy[strlen(path)] = '\0';
	char *token = strtok(path_cpy, "/");
	for (int i = 0; path_cpy[i] != '\0'; i++)
		if (path_cpy[i] == '/')
			level_count++;
	write_log("level is %d\n", level_count);

	// tokkens delimited by "/"
	char tokens[level_count][MY_MAX_PATH];

	// populate tokens
	for (level_count = 0; token != NULL; level_count++, token = strtok(NULL, "/"))
	{
		strcpy(tokens[level_count], token);
		tokens[level_count][strlen(token)] = '\0';
		write_log("tokens %d is %s\n", level_count, tokens[level_count]);
	}

	// build new path (parent path)
	for (int i = 0; i < level_count - 1; i++)
	{
		strcat(new_path, tokens[i]);
		strcat(new_path, "/");
		new_path[strlen(path)] = '\0';
		write_log("New path is: %s\n", new_path);
	}
	// retrieve parent from db
	myfcb parent_dir = find_fcb_from_path(new_path);
	if (strcmp(the_root_fcb.path, parent_dir.path) == 0)
		return the_root_fcb;
	else
		return parent_dir;
}

// apply update from child to root
void update_root_in_db(myfcb target_fcb, int written, bool isDeletion)
{
	int i;
	if (isDeletion)
	{
		for (i = 0; i < MY_MAX_FILES_PER_FOLDER; i++)
			if (uuid_compare(the_root_fcb.contents.fcb_pointers[i], target_fcb.fcb_uuid) == 0)
			{
				print_uuid(the_root_fcb.contents.fcb_pointers[i]);
				print_uuid(target_fcb.fcb_uuid);
				break;
			}
	}

	else
	{
		write_log("Should def !!!\n");
		for (i = 0; i < MY_MAX_FILES_PER_FOLDER; i++)
			if (*the_root_fcb.contents.fcb_pointers[i] == 0)
				break;
	}
	write_log("I (PLACE TO UPDATE) is %d\n", i);
	// null out the uuid of deleted target
	if (isDeletion)
	{
		memset(the_root_fcb.contents.fcb_pointers[i], 0, sizeof(uuid_t));
		print_uuid(the_root_fcb.contents.fcb_pointers[i]);
	}
	// copy uuid of new child to parent
	else
	{
		memcpy(the_root_fcb.contents.fcb_pointers[i], target_fcb.fcb_uuid, sizeof(uuid_t));
		print_uuid(the_root_fcb.contents.fcb_pointers[i]);
	}

	// overwrite parent
	// go back through dir tree and increase every size
	// dir update + or -
	if (written)
	{
		if (isDeletion)
			the_root_fcb.size -= written;
		else
		{
			the_root_fcb.size += written;
			write_log("ROOT SIZE AFTER UPDATE IS: %d\n", the_root_fcb.size);
		}
	}
	else
	{ // if child updated is dir
		if (isDeletion)
			the_root_fcb.size -= target_fcb.size;
		else
			the_root_fcb.size += target_fcb.size;
	}
	int rc = unqlite_kv_store(pDb, the_root_fcb.fcb_uuid, KEY_SIZE, &the_root_fcb, sizeof(myfcb));
	if (rc != UNQLITE_OK)
	{
		write_log("Error on updating root\n");
		error_handler(rc);
	}
}

//apply update of child to parents
void update_parent_in_db(myfcb target_fcb, myfcb parent, int written, bool isDeletion)
{
	// find place of insertion or deletion
	int i;
	if (isDeletion)
	{
		for (i = 0; i < MY_MAX_FILES_PER_FOLDER; i++)
			if (uuid_compare(parent.contents.fcb_pointers[i], target_fcb.fcb_uuid) == 0)
			{
				print_uuid(parent.contents.fcb_pointers[i]);
				print_uuid(parent.fcb_uuid);
				break;
			}

		write_log("found dir/file to delete at %d \n", i);
	}

	else
	{
		for (i = 0; i < MY_MAX_FILES_PER_FOLDER; i++)
			if (*parent.contents.fcb_pointers[i] == 0)
				break;
		write_log("found first free space at %d \n", i);
	}
	// add pointer to child in contents or remove child
	if (isDeletion)
		memset(parent.contents.fcb_pointers[i], 0, sizeof(uuid_t));
	else
		memcpy(parent.contents.fcb_pointers[i], target_fcb.fcb_uuid, sizeof(uuid_t));
	// overwrite parent
	// go back through dir tree and increase every size
	write_log("Starting to go through parents to update size\n");
	while (true)
	{
		if (strcmp(parent.path, the_root_fcb.path) == 0)
		{
			if (written)
			{
				if (isDeletion)
					the_root_fcb.size -= written;
				else
					the_root_fcb.size += written;
			}
			else
			{ // if child updated is dir or fcb
				if (isDeletion)
					the_root_fcb.size -= target_fcb.size;
				else
					the_root_fcb.size += target_fcb.size;
			}
			unqlite_kv_store(pDb, the_root_fcb.fcb_uuid, KEY_SIZE, &the_root_fcb, sizeof(myfcb));
			break;
		}
		if (written)
		{
			write_log("Child is file\n");
			if (isDeletion)
			{
				write_log("REDUCING FILE SIZE FROM DIR \n");
				parent.size -= written;
			}

			else
				parent.size += written;
		}
		else
		{ // if child updated is dir or new file fcb
			write_log("Child is dir\n");
			if (isDeletion)
			{
				write_log("REDUCING CHILD DIR SIZE FROM DIR \n");
				parent.size -= target_fcb.size;
			}
			else
				parent.size += target_fcb.size;
		}
		int rc = unqlite_kv_store(pDb, parent.fcb_uuid, KEY_SIZE, &parent, sizeof(myfcb));
		if (rc != UNQLITE_OK)
		{
			write_log("Error on updating parent\n");
			error_handler(rc);
		}
		parent = find_parent_from_child_path(parent.path);
	}
}

// Get file and directory attributes (meta-data).
// Read 'man 2 stat' and 'man 2 chmod'.
static int myfs_getattr(const char *path, struct stat *stbuf)
{
	write_log("myfs_getattr(path=\"%s\", statbuf=0x%08x)\n", path, stbuf);
	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0)
	{
		stbuf->st_size = the_root_fcb.size;
		stbuf->st_mode = the_root_fcb.mode;
		stbuf->st_nlink = 2;
		stbuf->st_ino = 0;
		stbuf->st_atime = the_root_fcb.atime;
		stbuf->st_mtime = the_root_fcb.mtime;
		stbuf->st_ctime = the_root_fcb.ctime;
		stbuf->st_blocks = sizeof(the_root_fcb) / 512 + 1; // round blocks
		stbuf->st_uid = the_root_fcb.uid;
		stbuf->st_gid = the_root_fcb.gid;
		return 0;
	}
	// not root
	else
	{
		write_log("not root stats required...\n");
		myfcb target = find_fcb_from_path(path);
		if (strcmp(path, target.path) != 0)
		{
			write_log("parent of target path is: %s\n", target.path);
			write_log("target with path %s not found\n", path);
			return -ENOENT;
		}
		// get file data block size
		if (S_ISREG(target.mode))
			stbuf->st_size = target.size - sizeof(myfcb);
		else
			stbuf->st_size = target.size;
		stbuf->st_mode = target.mode;
		stbuf->st_nlink = 1;
		stbuf->st_ino = 0;
		stbuf->st_atime = target.atime;
		stbuf->st_mtime = target.mtime;
		stbuf->st_ctime = target.ctime;
		// include num of 512 blocks in stat buf
		stbuf->st_blocks = sizeof(target) / 512 + 1;
		stbuf->st_uid = target.uid;
		stbuf->st_gid = target.gid;
		write_log("returns 0\n");
		return 0;
	}
	write_log("BAD PATH\n");
	write_log("myfs_getattr - ENOENT");
	return -ENOENT;
}

// Read a directory.
// Read 'man 2 readdir'.
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	write_log("write_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n", path, buf, filler, offset, fi);
	myfcb curr_dir = find_fcb_from_path(path);
	write_log("Got current fcb with uuid...\n");
	print_uuid(curr_dir.fcb_uuid);
	(void)offset; // This prevents compiler warnings
	(void)fi;
	// We always output . and .. first, by convention. See documentation for more info on filler()
	if (filler(buf, ".", NULL, 0) == 1)
		write_log("Problem in first filler\n");
	if (filler(buf, "..", NULL, 0) == 1)
		write_log("Problem in second\n");

	if (strcmp(the_root_fcb.path, curr_dir.path) != 0)
	{
		for (int i = 0; i < MY_MAX_FILES_PER_FOLDER; i++)
		{
			if (uuid_compare(zero_uuid, curr_dir.contents.fcb_pointers[i]) != 0)
			{
				myfcb fcb = fetch_uuid_data(curr_dir.contents.fcb_pointers[i]);
				write_log("file path to print name is: %s\n", fcb.path);
				char *f_name = strrchr(fcb.path, '/') + 1;
				write_log("pointer to name is: %s\n", f_name);
				if (filler(buf, f_name, NULL, 0) == 1)
					write_log("Problem in files/dirs\n");
			}
		}
		curr_dir.atime = time(0);
		unqlite_kv_store(pDb, curr_dir.fcb_uuid, KEY_SIZE, &curr_dir, sizeof(myfcb));
	}
	else
	{
		write_log("Going to loop over root contents:\n");
		for (int i = 0; i < MY_MAX_FILES_PER_FOLDER; i++)
		{
			if (uuid_compare(zero_uuid, the_root_fcb.contents.fcb_pointers[i]) != 0)
			{
				myfcb fcb = fetch_uuid_data(the_root_fcb.contents.fcb_pointers[i]);
				if (filler(buf, fcb.path + 1, NULL, 0) == 1)
					write_log("Problem in files/dirs\n");
			}
		}
		the_root_fcb.atime = time(0);
		unqlite_kv_store(pDb, the_root_fcb.fcb_uuid, KEY_SIZE, &the_root_fcb, sizeof(myfcb));
	}

	return 0;
}

// Read a file.
// Read 'man 2 read'.
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	size_t len;
	(void)fi;

	write_log("myfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);

	myfcb target = find_fcb_from_path(path);

	len = target.size - sizeof(myfcb); // calc data size
	write_log("Data block len is %d\n", len);

	uint8_t data_block[len];
	memset(data_block, 0, len);
	// Is there a data block?
	if (uuid_compare(zero_uuid, target.contents.file_data_uuid) != 0)
	{
		write_log("THERE IS A DATA BLOCK with uuid...\n");
		print_uuid(target.contents.file_data_uuid);
		unqlite_int64 nBytes; //Data length.
		int rc = unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, NULL, &nBytes);
		if (rc != UNQLITE_OK)
		{
			write_log("Error on data block read\n");
			error_handler(rc);
		}
		write_log("DATA FETCH SUCCESS WITH %d BYTES\n", nBytes);
		// Fetch the  block from the store.
		unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, &data_block, &nBytes);
	}

	if (offset < len)
	{
		if (offset + size > len)
		{
			write_log("adjusting bytes to read\n");
			size = len - offset;
		}
		write_log("reading %d bytes starting at %d from data block %s\n", size, offset, data_block);
		memcpy(buf, &data_block + offset, size);
		write_log("buffer is: %s \n", buf);
	}
	else
		size = 0;
	target.atime = time(0);
	unqlite_kv_store(pDb, target.fcb_uuid, KEY_SIZE, &target, sizeof(myfcb));
	return size;
}

// This file system only supports one file. Create should fail if a file has been created. Path must be '/<something>'.
// allocate new FCB, read appropriate dir into mem, update dir with filename and FCB, write dir data back
// Read 'man 2 creat'.
static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);

	int pathlen = strlen(path);
	if (pathlen >= MY_MAX_PATH)
	{
		write_log("myfs_create - ENAMETOOLONG");
		return -ENAMETOOLONG;
	}

	myfcb new_file;
	memset(&new_file, 0, sizeof(myfcb));
	strcpy(new_file.path, path);
	new_file.mode |= mode | S_IFREG;
	new_file.atime = time(0);
	new_file.mtime = time(0);
	new_file.ctime = time(0);
	struct fuse_context *context = fuse_get_context();
	new_file.uid = context->uid;
	new_file.gid = context->gid;
	new_file.size = sizeof(myfcb);
	uuid_generate(new_file.fcb_uuid); // self pointer
	int rc = unqlite_kv_store(pDb, new_file.fcb_uuid, KEY_SIZE, &new_file, sizeof(myfcb));
	//	rc = unqlite_kv_store(pDb, parent.contents.fcb_pointers[0], KEY_SIZE, &parent, sizeof(parent));
	write_log("File Should be stored\n");
	if (rc != UNQLITE_OK)
	{
		write_log("Error on making new file\n");
		error_handler(rc);
	}
	myfcb parent = find_parent_from_child_path(path);
	if (strcmp(the_root_fcb.path, parent.path) == 0)
	{
		the_root_fcb.mtime = time(0); // set mod time of dir
		update_root_in_db(new_file, 0, false);
	}

	else
	{
		parent.mtime = time(0); // set mod time of dir
		update_parent_in_db(new_file, parent, 0, false);
	}

	return 0;
}

// Set update the times (actime, modtime) for a file.
// Read 'man 2 utime'.
static int myfs_utime(const char *path, struct utimbuf *ubuf)
{
	write_log("myfs_utime(path=\"%s\", ubuf=0x%08x)\n", path, ubuf);
	myfcb target = find_fcb_from_path(path);
	target.mtime = ubuf->modtime;
	target.atime = ubuf->actime;

	// Write the fcb to the store.
	int rc = unqlite_kv_store(pDb, target.fcb_uuid, KEY_SIZE, &target, sizeof(myfcb));
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write - EIO");
		return -EIO;
	}

	return 0;
}

// Write to a file.
// Read 'man 2 write'
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	write_log("myfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);

	myfcb target = find_fcb_from_path(path);
	int curr_data_len = target.size - sizeof(myfcb);
	int new_data_len;
	// determine new data block size
	if (curr_data_len > offset + size)
		new_data_len = curr_data_len;
	else
		new_data_len = offset + size;

	uint8_t data_block[new_data_len];
	write_log("new data len size is %d\n", new_data_len);
	memset(data_block, '\0', sizeof(data_block));
	write_log("new data block size is %d \n", sizeof(data_block));

	uint8_t *data_block_p = data_block;
	// check if there is data in file
	if (uuid_compare(zero_uuid, target.contents.file_data_uuid) == 0)
	{
		write_log("DATA Block is empty\n");
		uuid_generate(target.contents.file_data_uuid);
		//		write_log("new data block uuid is: %d\n", *data_id);
		write_log("new data block uuid is:...\n");
		print_uuid(target.contents.file_data_uuid);
	}
	else
	{
		// First we will check the size of the obejct in the store to ensure that we won't overflow the buffer.
		unqlite_int64 nBytes; // Data length.
		int rc = unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, NULL, &nBytes);
		if (rc != UNQLITE_OK)
		{
			write_log("myfs_write - EIO on fetching data from DB\n");
			return -EIO;
		}

		unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, &data_block, &nBytes);
		write_log("Fetched buffer is %s\n", data_block);
	}

	int written = snprintf(data_block_p + offset, size + 1, buf);
	data_block[strlen(data_block)] = '\n';

	//write data block to store with all its size
	int rc = unqlite_kv_store(pDb, target.contents.file_data_uuid, KEY_SIZE, &data_block, sizeof(data_block));
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write - EIO on writing data to DB\n");
		return -EIO;
	}
	if (curr_data_len >= new_data_len)
	{
		// do not increase size for overwriting
		write_log("Entirely overwriting not increase\n");
		written = 0;
	}
	else
	{
		written = abs(new_data_len - curr_data_len);
		write_log("Increase by %d bytes\n", written);
	}

	target.size += written;

	// Write the fcb to the store.
	// the rest of the function could be shrinked and put externally
	write_log("Target after write size is %d bytes\n", target.size);
	write_log("Data block size is %d bytes\n", sizeof(data_block));
	target.mtime = time(0);
	rc = unqlite_kv_store(pDb, target.fcb_uuid, KEY_SIZE, &target, sizeof(myfcb));
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write - EIO on writing FCB back to DB\n");
		return -EIO;
	}

	write_log("Starting to update parents \n");
	myfcb parent = find_parent_from_child_path(path);
	if (curr_data_len < new_data_len)
	{
		if (strcmp(the_root_fcb.path, parent.path) == 0)
		{
			the_root_fcb.size += written;
			rc = unqlite_kv_store(pDb, the_root_fcb.fcb_uuid, KEY_SIZE, &the_root_fcb, sizeof(myfcb));
			if (rc != UNQLITE_OK)
			{
				write_log("myfs_write - EIO");
				return -EIO;
			}
		}
		else
		{
			while (true)
			{
				if (strcmp(parent.path, the_root_fcb.path) == 0)
				{
					the_root_fcb.size += written;
					unqlite_kv_store(pDb, the_root_fcb.fcb_uuid, KEY_SIZE, &the_root_fcb, sizeof(myfcb));
					break;
				}
				parent.size += written;
				rc = unqlite_kv_store(pDb, parent.fcb_uuid, KEY_SIZE, &parent, sizeof(myfcb));
				if (rc != UNQLITE_OK)
				{
					write_log("myfs_write - EIO");
					return -EIO;
				}
				parent = find_parent_from_child_path(parent.path);
			}
		}
		write_log("Finishing to update parents \n");
	}
	return written;
}

// Set the size of a file.
// Read 'man 2 truncate'.
int myfs_truncate(const char *path, off_t newsize)
{
	write_log("myfs_truncate(path=\"%s\", newsize=%lld)\n", path, newsize);

	// Update the FCB in-memory
	myfcb target = find_fcb_from_path(path);
	int curr_data_size = target.size - sizeof(myfcb);
	uint8_t data_block[newsize];
	write_log("new data len size is %d\n", newsize);
	memset(data_block, '\0', sizeof(data_block));
	write_log("new data block size is %d \n", sizeof(data_block));
	if (uuid_compare(zero_uuid, target.contents.file_data_uuid) == 0)
	{
		write_log("DATA Block to truncate is empty\n");
		uuid_generate(target.contents.file_data_uuid);
		//		write_log("new data block uuid is: %d\n", *data_id);
		write_log("new data block to truncate uuid is: %s...\n");
		print_uuid(target.contents.file_data_uuid);
	}
	else
	{
		// First we will check the size of the obejct in the store to ensure that we won't overflow the buffer.
		unqlite_int64 nBytes; // Data length.
		unqlite_int64 fetch_bytes = (unqlite_int64)newsize;
		int rc = unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, NULL, &nBytes);
		if (rc != UNQLITE_OK)
		{
			write_log("myfs_write - EIO on fetching data from DB\n");
			return -EIO;
		}
		// fetch shrinked size
		if (fetch_bytes < curr_data_size)
		{
			unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, &data_block, &fetch_bytes);
			write_log("Number of bytes fetched is: %d\n", fetch_bytes);
		}
		// fetch whole data block rest is 0s
		else
		{
			unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, &data_block, &nBytes);
			write_log("Number of bytes fetched is: %d\n", nBytes);
		}
	}

	// write data block back to store
	write_log("NEW DATA BLOCK BEFORE STORE IS %s \n", data_block);
	int rc = unqlite_kv_store(pDb, target.contents.file_data_uuid, KEY_SIZE, &data_block, newsize);
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write on data block write - EIO");
		return -EIO;
	}

	myfcb parent = find_parent_from_child_path(path);
	if (strcmp(the_root_fcb.path, parent.path) == 0)
	{
		if (newsize < curr_data_size)
			the_root_fcb.size -= (curr_data_size - newsize);
		else
			the_root_fcb.size += (newsize - curr_data_size);
		write_log("new root size is: %d\n", the_root_fcb.size);
		rc = unqlite_kv_store(pDb, the_root_fcb.fcb_uuid, KEY_SIZE, &the_root_fcb, sizeof(myfcb));
		if (rc != UNQLITE_OK)
		{
			write_log("myfs_write - EIO");
			return -EIO;
		}
	}
	else
	{
		while (true)
		{
			if (strcmp(parent.path, the_root_fcb.path) == 0)
			{
				if (newsize < curr_data_size)
					the_root_fcb.size -= (curr_data_size - newsize);
				else
					the_root_fcb.size += (newsize - curr_data_size);
				unqlite_kv_store(pDb, the_root_fcb.fcb_uuid, KEY_SIZE, &the_root_fcb, sizeof(myfcb));
				break;
			}
			if (newsize < curr_data_size)
				parent.size -= (curr_data_size - newsize);
			else
				parent.size += (newsize - curr_data_size);
			rc = unqlite_kv_store(pDb, parent.fcb_uuid, KEY_SIZE, &parent, sizeof(myfcb));
			if (rc != UNQLITE_OK)
			{
				write_log("myfs_write - EIO");
				return -EIO;
			}
			parent = find_parent_from_child_path(parent.path);
		}
	}
	write_log("Finishing to update parents \n");

	target.size = sizeof(myfcb) + newsize; // update fcb size
	write_log("TRUNCATED DATA IS %s\n", data_block);
	write_log("TRUNCATED DATA SIZE IS %d\n", target.size);
	// Write the fcb to the store.
	target.mtime = time(0);
	rc = unqlite_kv_store(pDb, target.fcb_uuid, KEY_SIZE, &target, sizeof(myfcb));
	write_log("TRUNCATED TARGET with path %s has uuid... \n", target.path);
	print_uuid(target.fcb_uuid);
	if (rc != UNQLITE_OK)
	{
		write_log("myfs_write - EIO");
		return -EIO;
	}
	return 0;
}

// Set permissions.
// Read 'man 2 chmod'.
int myfs_chmod(const char *path, mode_t mode)
{
	write_log("myfs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);
	myfcb target = find_fcb_from_path(path);
	target.mode = mode;
	target.ctime = time(0);
	unqlite_kv_store(pDb, target.fcb_uuid, KEY_SIZE, &target, sizeof(myfcb));

	return 0;
}

// Set ownership.
// Read 'man 2 chown'.
int myfs_chown(const char *path, uid_t uid, gid_t gid)
{
	write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
	myfcb target = find_fcb_from_path(path);
	target.uid = uid;
	target.gid = gid;
	target.ctime = time(0);
	unqlite_kv_store(pDb, target.fcb_uuid, KEY_SIZE, &target, sizeof(myfcb));

	return 0;
}

// Create a directory.
// Read 'man 2 mkdir'.
int myfs_mkdir(const char *path, mode_t mode)
{
	int rc;
	write_log("myfs_mkdir: %s\n", path);
	int pathlen = strlen(path);
	if (pathlen >= MY_MAX_PATH)
	{
		write_log("myfs_mkdir - ENAMETOOLONG");
		return -ENAMETOOLONG;
	}
	myfcb new_dir;
	memset(&new_dir, 0, sizeof(myfcb));
	memset(new_dir.contents.fcb_pointers, 0, sizeof(new_dir.contents.fcb_pointers));
	strcpy(new_dir.path, path);
	new_dir.path[strlen(new_dir.path)] = '\0';
	write_log("new dir path is: %s\n", new_dir.path);
	new_dir.mode |= mode | S_IFDIR;
	new_dir.atime = time(0);
	new_dir.mtime = time(0);
	new_dir.ctime = time(0);
	new_dir.uid = getuid();
	new_dir.gid = getgid();
	new_dir.size = sizeof(myfcb);
	uuid_generate(new_dir.fcb_uuid); // self pointer
	print_uuid(new_dir.fcb_uuid);
	rc = unqlite_kv_store(pDb, new_dir.fcb_uuid, KEY_SIZE, &new_dir, sizeof(myfcb));
	write_log("Should be stored\n");
	if (rc != UNQLITE_OK)
	{
		write_log("Error on making new dir\n");
		error_handler(rc);
	}

	myfcb parent = find_parent_from_child_path(path);
	if (strcmp(the_root_fcb.path, parent.path) == 0)
	{
		the_root_fcb.mtime = time(0);
		write_log("WILL UPDATE ROOT\n");
		update_root_in_db(new_dir, 0, false);
	}
	else
	{
		write_log("WILL UPDATE PARENT\n");
		parent.mtime = time(0);
		update_parent_in_db(new_dir, parent, 0, false);
	}

	return 0;
}

// Delete a file.
// Read 'man 2 unlink'.
int myfs_unlink(const char *path)
{
	write_log("myfs_unlink: %s\n", path);
	myfcb target = find_fcb_from_path(path);
	int rc;
	// check if file exists
	if (strcmp(target.path, path) != 0)
	{
		write_log("myfs_unlink: file not found\n");
		return -ENOENT;
	}
	write_log("Curr file size is + fcb: %d\n", target.size);
	// delete data
	if (uuid_compare(zero_uuid, target.contents.file_data_uuid) != 0)
	{
		rc = unqlite_kv_delete(pDb, target.contents.file_data_uuid, KEY_SIZE);
		if (rc != UNQLITE_OK)
		{
			write_log("Error on deleting file data\n");
			error_handler(rc);
		}
	}
	// delete fcb
	rc = unqlite_kv_delete(pDb, target.fcb_uuid, KEY_SIZE);
	if (rc != UNQLITE_OK)
	{
		write_log("Error on deleting file fcb\n");
		error_handler(rc);
	}

	myfcb parent = find_parent_from_child_path(path);
	if (strcmp(the_root_fcb.path, parent.path) == 0)
		update_root_in_db(target, target.size, true);
	else
		update_parent_in_db(target, parent, target.size, true);

	return 0;
}

// Delete a directory.
// Read 'man 2 rmdir'.
int myfs_rmdir(const char *path)
{
	write_log("myfs_rmdir: %s\n", path);
	myfcb curr_dir = find_fcb_from_path(path);
	//fail to delete if dir is not empty
	write_log("Curr dir size is: %d\n", curr_dir.size);
	if (curr_dir.size > sizeof(myfcb))
	{
		return -ENOTEMPTY;
	}
	write_log("Self uuid of dir to delete is:...\n");
	print_uuid(curr_dir.fcb_uuid);
	write_log("Size of FCB dir to delete is: %d\n", sizeof(curr_dir));
	int rc = unqlite_kv_delete(pDb, curr_dir.fcb_uuid, KEY_SIZE);
	if (rc != UNQLITE_OK)
	{
		write_log("Error on deleting dir\n");
		error_handler(rc);
	}

	myfcb parent = find_parent_from_child_path(path);
	parent.mtime = time(0);
	if (strcmp(the_root_fcb.path, parent.path) == 0)
		update_root_in_db(curr_dir, 0, true);
	else
		update_parent_in_db(curr_dir, parent, 0, true);

	return 0;
}

// Extension
int myfs_rename(const char *from, const char *to)
{
	write_log("myfs_rename: from %s to %s\n", from, to);
	int pathlen = strlen(to);
	if (pathlen >= MY_MAX_PATH)
	{
		write_log("myfs_rename - ENAMETOOLONG");
		return -ENAMETOOLONG;
	}

	myfcb target = find_fcb_from_path(from);

	//save data contents
	unqlite_int64 nBytes;				   //Data length.
	int len = target.size - sizeof(myfcb); // calc data size
	uint8_t data_block[len];
	memset(data_block, 0, len);
	if (S_ISREG(target.mode))
	{
		if (uuid_compare(zero_uuid, target.contents.file_data_uuid) != 0)
		{
			write_log("Starting to write file data %s for rename\n", to);
			write_log("Data block len is %d\n", len);
			write_log("THERE IS A DATA BLOCK to copy with uuid...\n");
			print_uuid(target.contents.file_data_uuid);
			unqlite_int64 nBytes; //Data length.
			int rc = unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, NULL, &nBytes);
			if (rc != UNQLITE_OK)
			{
				write_log("Error on data block read\n");
				error_handler(rc);
			}
			write_log("DATA FETCH SUCCESS WITH %d BYTES\n", nBytes);
			// Fetch the  block from the store.
			unqlite_kv_fetch(pDb, target.contents.file_data_uuid, KEY_SIZE, &data_block, &nBytes);
			printf("LEN of data block initially i %d actual length in store is %d\n", len, nBytes);
		}
	}

	//remove from everything behind
	// rmdir
	if (S_ISDIR(target.mode))
	{
		write_log("Starting to remove dir with path %s for rename\n", target.path);
		myfs_rmdir(target.path);
	}
	//unlink
	else
	{
		write_log("Starting to unlink file with path %s for rename\n", target.path);
		myfs_unlink(target.path);
	}

	//add in everything new
	//mkdir
	if (S_ISDIR(target.mode))
	{
		write_log("Starting to mkdir with path %s for rename\n", to);
		myfs_mkdir(to, target.mode);
	}

	//create and write
	else
	{
		write_log("Starting to create file %s for rename\n", to);
		myfs_create(to, target.mode, NULL);
		// Is there a data block?
		if (uuid_compare(zero_uuid, target.contents.file_data_uuid) != 0)
			myfs_write(to, data_block, nBytes, 0, NULL);
	}
	//yo're done !!
	return 0;
}

// OPTIONAL - included as an example
// Flush any cached data.
int myfs_flush(const char *path, struct fuse_file_info *fi)
{
	int retstat = 0;

	write_log("myfs_flush(path=\"%s\", fi=0x%08x)\n", path, fi);

	return retstat;
}

// OPTIONAL - included as an example
// Release the file. There will be one call to release for each call to open.
int myfs_release(const char *path, struct fuse_file_info *fi)
{
	int retstat = 0;

	write_log("myfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);

	return retstat;
}

// OPTIONAL - included as an example
// Open a file. Open should check if the operation is permitted for the given flags (fi->flags).
// lec 7 slide 8
// Read 'man 2 open'.
// may implement
static int myfs_open(const char *path, struct fuse_file_info *fi)
{

	write_log("myfs_open(path\"%s\", fi=0x%08x)\n", path, fi);

	//return -EACCES if the access is not permitted.

	return 0;
}

// This struct contains pointers to all the functions defined above
// It is used to pass the function pointers to fuse
// fuse will then execute the methods as required
static struct fuse_operations myfs_oper = {
	.rmdir = myfs_rmdir,
	.mkdir = myfs_mkdir,
	.getattr = myfs_getattr,
	.readdir = myfs_readdir,
	.open = myfs_open,
	.read = myfs_read,
	.create = myfs_create,
	.utime = myfs_utime,
	.write = myfs_write,
	.truncate = myfs_truncate,
	.flush = myfs_flush,
	.release = myfs_release,
	.unlink = myfs_unlink,
	.chmod = myfs_chmod,
	.chown = myfs_chown,
	.rename = myfs_rename,
};

// Initialise the in-memory data structures from the store. If the root object (from the store) is empty then create a root fcb (directory)
// and write it to the store. Note that this code is executed outide of fuse. If there is a failure then we have failed toi initlaise the
// file system so exit with an error code.
void init_fs()
{
	int rc;
	printf("init_fs\n");
	//Initialise the store.

	uuid_clear(zero_uuid);

	// Open the database.
	rc = unqlite_open(&pDb, DATABASE_NAME, UNQLITE_OPEN_CREATE);
	if (rc != UNQLITE_OK)
	{
		write_log("Error on unqlite db open\n");
		error_handler(rc);
	}

	unqlite_int64 nBytes; // Data length

	// Try to fetch the root element
	// The last parameter is a pointer to a variable which will hold the number of bytes actually read
	rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, KEY_SIZE, &the_root_fcb, &nBytes);

	// if it doesn't exist, we need to create one and put it into the database. This will be the root
	// directory of our filesystem i.e. "/"
	if (rc == UNQLITE_NOTFOUND)
	{

		printf("init_store: root object was not found\n");

		memset(&the_root_fcb, 0, sizeof(myfcb));
		memset(the_root_fcb.contents.fcb_pointers, 0, sizeof(the_root_fcb.contents.fcb_pointers));

		// Sensible initialisation for the root FCB
		//See 'man 2 stat' and 'man 2 chmod'.
		strcpy(the_root_fcb.path, "/");
		memcpy(the_root_fcb.fcb_uuid, ROOT_OBJECT_KEY, KEY_SIZE);
		the_root_fcb.mode |= S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IXUSR | S_IXGRP;
		the_root_fcb.size = sizeof(myfcb);
		the_root_fcb.atime = time(0);
		the_root_fcb.mtime = time(0);
		the_root_fcb.ctime = time(0);
		the_root_fcb.uid = getuid();
		the_root_fcb.gid = getgid();

		// Write the root FCB
		printf("init_fs: writing root fcb\n");
		rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, KEY_SIZE, &the_root_fcb, sizeof(myfcb));

		if (rc != UNQLITE_OK)
		{
			write_log("Error on fs init\n");
			error_handler(rc);
		}
	}
	else
	{
		if (rc == UNQLITE_OK)
		{
			printf("init_store: root object was found\n");
		}
		if (nBytes != sizeof(myfcb))
		{
			printf("Data object has unexpected size. Doing nothing.\n");
			exit(-1);
		}
	}
}

void shutdown_fs()
{
	unqlite_close(pDb);
}

int main(int argc, char *argv[])
{
	int fuserc;
	struct myfs_state *myfs_internal_state;

	//Setup the log file and store the FILE* in the private data object for the file system.
	myfs_internal_state = malloc(sizeof(struct myfs_state));
	myfs_internal_state->logfile = init_log_file();

	//Initialise the file system. This is being done outside of fuse for ease of debugging.
	init_fs();

	// Now pass our function pointers over to FUSE, so they can be called whenever someone
	// tries to interact with our filesystem. The internal state contains a file handle
	// for the logging mechanism
	fuserc = fuse_main(argc, argv, &myfs_oper, myfs_internal_state);

	//Shutdown the file system.
	shutdown_fs();

	return fuserc;
}
