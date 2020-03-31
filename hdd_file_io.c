////////////////////////////////////////////////////////////////////////////////
//
//  File           : hdd_file_io.c
//  Description    : This is the implementation of the standardized IO functions
//                   for used to access the HDD storage.
//
//  Author         : Tianjian Gao
//  Last Modified  : Thu Nov 30th 20:00:00 EDT 2017
//

// Includes
#include <malloc.h>
#include <string.h>

// Project Includes
#include <hdd_file_io.h>
#include <hdd_driver.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <hdd_network.h>

// Defines
#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define HDD_IO_UNIT_TEST_ITERATIONS 10240

// Type for UNIT test interface
typedef enum {
	CIO_UNIT_TEST_READ   = 0,
	CIO_UNIT_TEST_WRITE  = 1,
	CIO_UNIT_TEST_APPEND = 2,
	CIO_UNIT_TEST_SEEK   = 3,
} HDD_UNIT_TEST_TYPE;

char *cio_utest_buffer = NULL;  // Unit test buffer

////////////////////////////////////////////////////////////////////////////////////////
// User structs
// Define a HDD_FILE type to store block info (global structure)
typedef struct {
	uint32_t id; //Block ID returned by hdd_client_operation
	uint8_t open; //Indicates whether the file is open. 1 open; 0 closed
	uint32_t position; //current position
	uint32_t size; //...NEW IN ASSG4: BLOCK SIZE OF THE CURRENT FILE
	char name[MAX_FILENAME_LENGTH]; //File name
} HDD_FILE;

//Define a HDD_CMD type to store and generate HDD_IO command
typedef struct {
	uint32_t block; //Block ID
	uint8_t r; //Result bit. 1 fail; 0 success
	uint8_t flags; //flags
	uint32_t block_size; //Block size
	uint8_t op; //Op code indicating if the block is read, overwritten or created
} HDD_CMD;

// HDD Interface
//
//Command generator to generate command to pass into hdd_client_operation
//Input: block: block id, r: result bit (1 failure, 0 success), flags: hdd flags, block_size, op: op code
//Output: HddBitCmd command for hdd_client_operation
HddBitCmd cmd_generator(uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op){
	uint32_t block_size_comp = 0;
	HddBitCmd command;
	op = op & 3;
	r = r & 1;
	flags = flags & 7;
	
	block_size_comp = ~block_size_comp >> 6;
	block_size = block_size & block_size_comp;
	//Generate command
	command = (HddBitCmd) op << 62;
	command = command | (HddBitCmd) block_size << 36;
	command |= command | (HddBitCmd) flags << 33;
	command |= command | (HddBitCmd) r << 32;
	command |= command | (HddBitCmd) block;
	return command;
}

//Reads the HDD response into a HDD_CMD type structure for later use.
//Input: HddBitResp type command
//Output: HDD_CMD type struct
HDD_CMD cmd_reader(HddBitResp cmd){
	uint32_t block_size_comp = 0;
	uint32_t block_comp = 0;

	HDD_CMD command;
	command.op = (uint8_t) (cmd >> 62);
	
	block_size_comp = (~block_size_comp) >> 6;
	command.block_size = ((uint32_t) (cmd >> 36)) & block_size_comp;
	command.flags = ((uint8_t) (cmd >> 33)) & 7;
	command.r = ((uint8_t) (cmd >> 32)) & 1;
	
	block_comp = ~block_comp;
	command.block = ((uint32_t) cmd) & block_comp;
	return command;
}

//Global Variable
int hdd_init = 0; //Flag if the block storage is initialized. 1 if succeeded, 0 if failed.

/////////////////////////////////////////////////////////////////////////////////
//
//Global data structure initialization
HDD_FILE hdd_files[MAX_HDD_FILEDESCR]; //Initialize a file list that takes up to MAX_HDD_FILEDESCR(1024) of HDD_FILE objects

//Function to initialize the global structure that can take up to MAX_HDD_FILEDESCR(1024) of HDD_FILE objects
void hdd_file_initialization(){
	int i;
	for (i = 0; i < MAX_HDD_FILEDESCR; i++){
		strcpy(hdd_files[i].name, "");
		hdd_files[i].id = 0;
		hdd_files[i].position = 0;
		hdd_files[i].open = 0;
		hdd_files[i].size = 0; //NEW IN ASSG4!
	}
}

/////////////////////////////////////////////////////////////////////////////////////
//
// Implementation
// Author: Tianjian Gao

////////////////////////////////////////////////////////////////////////////////
//
// Function     : hdd_format
// Description  : format the block storage and the global structure. 1. Initialize the device. 
//		  2. read hdd_content.svd 3. format the block 4. create the meta block
//
// Inputs       : void
// Outputs      : 0 on success and -1 on failure 
//
uint16_t hdd_format(void) {
	//Check if initialized HDD 
	if (hdd_init == 0) {
		//Create cmd to initialize hdd
		HddBitCmd initialize = cmd_generator(0,0,HDD_INIT,0,HDD_DEVICE); //generate hdd initialization request. fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
		HDD_CMD initialize_result = cmd_reader(hdd_client_operation(initialize,NULL)); //USE hdd_client_operation to communicate with and format the hdd
		//If HDD failed to initialize
		if (initialize_result.r == 1)
			return -1;
		hdd_init = 1;
	}

	//Now format all the blocks once hdd is initialized
	HddBitCmd format = cmd_generator(0, 0, HDD_FORMAT, 0, HDD_DEVICE); //generate format request. fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
	HDD_CMD format_result = cmd_reader(hdd_client_operation(format, NULL)); //USE hdd_client_operation to communicate with and format the hdd
	//Check format result
	if (format_result.r == 1)
		return -1;
	
	//Now initializing the global structure
	hdd_file_initialization(); //Initialize the hdd_files structure to store file open info
	
	//Create the meta block and save the global structure to it
	HddBitCmd create_meta = cmd_generator(0, 0, HDD_META_BLOCK, MAX_HDD_FILEDESCR*sizeof(HDD_FILE), HDD_BLOCK_CREATE); //fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
	HDD_CMD create_result = cmd_reader(hdd_client_operation(create_meta, hdd_files)); //USE hdd_client_operation to communicate with and format the hdd
	//Check create result
	if (create_result.r == 1)
		return -1;

	//Save meta block info into hdd_files
	hdd_files[0].id = create_result.block;
	strcpy(hdd_files[0].name, "Meta Block");
	hdd_files[0].size = create_result.block_size;
	hdd_files[0].open = 1;

	//Return 0 if all succeeded
	return 0;
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : hdd_mount
// Description  : mount the device and read the meta data to the global structure for further use.
//		  1. initialize the HDD. 2. read the meta block into the global structure
//
// Inputs       : void
// Outputs      : 0 on success and -1 on failure 
//
uint16_t hdd_mount(void) {
	//Check if initialized HDD 
	if (hdd_init == 0) {
		//Create cmd to initialize hdd
		HddBitCmd initialize = cmd_generator(0,0,HDD_INIT,0,HDD_DEVICE); //generate hdd initialization request. fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
		HDD_CMD initialize_result = cmd_reader(hdd_client_operation(initialize,NULL)); //USE hdd_client_operation to communicate with and format the hdd
		//If HDD failed to initialize
		if (initialize_result.r == 1)
			return -1;
		hdd_init = 1;
	}

	//Re-initializing the global structure
	hdd_file_initialization(); //Initialize the hdd_files structure to store file open info
	
	//Create the meta block and save the global structure to it
	HddBitCmd read_meta = cmd_generator(hdd_files[0].id, 0, HDD_META_BLOCK, MAX_HDD_FILEDESCR*sizeof(HDD_FILE), HDD_BLOCK_READ); //generate read meta block command. fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
	HDD_CMD read_result = cmd_reader(hdd_client_operation(read_meta, hdd_files)); //USE hdd_client_operation to communicate with and overwrite the hdd meta block
	
	//Check read result
	if (read_result.r == 1)
		return -1;
	
	//Check if bytes read to the global structure equals to the size of the meta block
	if (read_result.block_size != MAX_HDD_FILEDESCR*sizeof(HDD_FILE))
		return -1;

	//Save meta block info into hdd_files
	hdd_files[0].id = read_result.block;
	strcpy(hdd_files[0].name, "Meta Block");
	hdd_files[0].size = read_result.block_size;
	hdd_files[0].open = 1;

	//Return 0 if all succeeded
	return 0;
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : hdd_unmount
// Description  : unmount the device. 
//		  1. check hdd initialization 2. save the meta block to the global structure
//		  3. request to save and close the device
//
// Inputs       : void
// Outputs      : 0 if success or 1 if failure
//
uint16_t hdd_unmount(void) {
	// Check if hdd is initialized
    	if (hdd_init == 0)
        	return -1;
	
	//Write back the metadata to the global structure
	HddBitCmd save_meta = cmd_generator(hdd_files[0].id, 0, HDD_META_BLOCK, MAX_HDD_FILEDESCR*sizeof(HDD_FILE), HDD_BLOCK_OVERWRITE); //generate save meta command. fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
	HDD_CMD save_result = cmd_reader(hdd_client_operation(save_meta, hdd_files)); //USE hdd_client_operation to communicate with and load the meta data to the global structure
	
	//Check create result
	if (save_result.r == 1)
		return -1;

	//Send a request to save and close the hdd data block
	HddBitCmd update_meta = cmd_generator(0, 0, HDD_SAVE_AND_CLOSE, 0, HDD_DEVICE); //fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
	HDD_CMD update_result = cmd_reader(hdd_client_operation(update_meta, NULL)); //USE hdd_client_operation to communicate with and load the meta data to the global structure

	//Check create result
	if (update_result.r == 1)
		return -1;

	//Uninitialize the device
	hdd_init = 0;

	//Return 0 if all succeeded
	return 0;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : hdd_open
// Description  : opens a single file and returns a unique file handle
//
// Inputs       : a char pointer to a file
// Outputs      : a file handle (integer) referring to a particular file, or -1 if file not found
//
// Progress 	: 100%
int16_t hdd_open(char *path) { 
	int file_handle = 0;

	// Check if hdd is initialized
    	if (hdd_init == 0)
        	return -1;
	
	//Check the path
	if (path == NULL || strlen(path) > MAX_FILENAME_LENGTH)
		return -1;
	//Search if file already exists
	while (file_handle < MAX_HDD_FILEDESCR && strcmp(hdd_files[file_handle].name, path) != 0)
		file_handle++;

	//Case the file does not exist
	if (file_handle == MAX_HDD_FILEDESCR){
		
		int search = 0; //use as an index to search for an empty slot in the global structure

		while (strcmp(hdd_files[search].name, "") != 0)
			search ++;

		//Fail if file handle exceeds max file handle available 
		if (search == MAX_HDD_FILEDESCR)
			return -1;

		//Initialize the block if an empty spot is found
		strcpy(hdd_files[search].name, path);
		hdd_files[search].open = 1;

		return search;
	}
	//Case the file already exists
	else {
		//if the file is not already opened
		if (hdd_files[file_handle].open == 0) {
		hdd_files[file_handle].open = 1;
		hdd_files[file_handle].position = 0;
		}

		return file_handle;
	}
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : hdd_close
// Description  : closes a file referenced by a file handle
//
// Inputs       : a file handle fh
// Outputs      : 0 on success or -1 if failed
//
// Progress	: 100%
int16_t hdd_close(int16_t fh) {
	// Check if hdd is initialized
    	if (hdd_init == 0)
        	return -1;

	//Check file handle
	if (fh < 0 || fh > MAX_HDD_FILEDESCR)
		return -1;

	//If if hdd file is already closed
	if (hdd_files[fh].open == 0) 
		return -1;

	//Close the file
	hdd_files[fh].open = 0;
	hdd_files[fh].position = 0;
	return 0;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : hdd_read
// Description  : reads a count number of bytes from the current position in the file
//		  and place them into data buffer
// Inputs       : integer file handle fh, pointer -> data in file, integer count as in byte count
// Outputs      : -1 if failed or number of bytes read
//
//Progress: 100%
int32_t hdd_read(int16_t fh, void * data, int32_t count) {
	if (hdd_init == 0) {
		//Create cmd to initialize hdd
		HddBitCmd initialize = cmd_generator(0,0,HDD_INIT,0,HDD_DEVICE); //generate hdd initialization request. fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
		HDD_CMD initialize_result = cmd_reader(hdd_client_operation(initialize,NULL)); //USE hdd_client_operation to communicate with and format the hdd
		//If HDD failed to initialize
		if (initialize_result.r == 1)
			return -1;
		hdd_init = 1;
	}

	//Check file at fh exists
	if (fh < 0 || fh > MAX_HDD_FILEDESCR)
		return -1;
	
	//Check data validity
	if (data == NULL)
		return -1;
	
	//Check count
	if (count < 0 || count > HDD_MAX_BLOCK_SIZE)
		return -1;
	
	//If the file is not open
	if (hdd_files[fh].open == 0)
		return -1;
	
	//If all succeeded, create read command and read the file
	HddBitCmd read_block = cmd_generator(hdd_files[fh].id, 0, 0, hdd_files[fh].size, HDD_BLOCK_READ);

	//Create a buffer to copy block content
	char *read_buff = malloc(hdd_files[fh].size);
	HDD_CMD read_result = cmd_reader(hdd_client_operation(read_block, read_buff));

	if (read_result.r == 1){
		free(read_buff);
		return -1;
	}
	else {

		//Check if the read request count exceeds the block size
		if ((hdd_files[fh].size - hdd_files[fh].position) > count){ //more bytes than asked
			memcpy(data, &read_buff[hdd_files[fh].position], count);
			hdd_files[fh].position += count;
			free(read_buff);
			return count;
		}
		else { //less bytes than asked, read till the end of the block
			memcpy(data, &read_buff[hdd_files[fh].position], (hdd_files[fh].size - hdd_files[fh].position));
			int bytes_read = hdd_files[fh].size - hdd_files[fh].position;
			hdd_files[fh].position = hdd_files[fh].size;
			free(read_buff);
			return bytes_read;
		}
	}	
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : hdd_write
// Description  : writes a count number of bytes from current position in the file
//		  and place them into data buffer
//
// Inputs       : integer file handle fh, pointer -> data in file, integer count as in byte count
// Outputs      : -1 if failed or number of bytes written
//
//Progress: 100%
int32_t hdd_write(int16_t fh, void *data, int32_t count) {
	if (hdd_init == 0) {
		//Create cmd to initialize hdd
		HddBitCmd initialize = cmd_generator(0,0,HDD_INIT,0,HDD_DEVICE); //generate hdd initialization request. fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
		HDD_CMD initialize_result = cmd_reader(hdd_client_operation(initialize,NULL)); //USE hdd_client_operation to communicate with and format the hdd
		//If HDD failed to initialize
		if (initialize_result.r == 1)
			return -1;
		hdd_init = 1;
	}

	//Check file at fh exists
	if (fh < 0 || fh > MAX_HDD_FILEDESCR)
		return -1;
	
	//Check data validity
	if (data == NULL)
		return -1;
	
	//Check count and overflow from HDD_MAX_BLOCK_SIZE
	if (count < 0 || (count + hdd_files[fh].position) > HDD_MAX_BLOCK_SIZE) 
		return -1;
	
	//If file at fh is not opened 
	if (hdd_files[fh].open == 0)
		return -1;
	//Write data
	//First step: check if block in hdd has content already
	if (hdd_files[fh].id == 0){ //Block is not written
		//generate command to create a new block
		HddBitCmd create = cmd_generator(0, 0, HDD_NULL_FLAG, count, HDD_BLOCK_CREATE);//fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
		HDD_CMD check_create = cmd_reader(hdd_client_operation(create, data));

		//..Check if block creation failed
		if (check_create.r == 1)
			return -1;
		
		//create a new block and write it
		hdd_files[fh].id = check_create.block;
		hdd_files[fh].position = count;
		hdd_files[fh].size = count; //...NEW IN ASSG4!
		return count;
	}
	else { //Block has been written before
		HddBitCmd read_block = cmd_generator(hdd_files[fh].id, 0, 0, hdd_files[fh].size, HDD_BLOCK_READ); //read block data
		
		char *read_buff = malloc(hdd_files[fh].size); //create a read buff
		HDD_CMD read_result = cmd_reader(hdd_client_operation(read_block, read_buff));
		
		//If read failed
		if (read_result.r == 1){
			free(read_buff); //free buff to prevent memory leak
			return -1; 
		}

		//If block does not to be resized
		if (hdd_files[fh].position + count < hdd_files[fh].size){
			
			memcpy(&read_buff[hdd_files[fh].position], data, count); //copy data from the seek position
			//generate a block write command
			HddBitCmd write_block = cmd_generator(hdd_files[fh].id, 0, 0, hdd_files[fh].size, HDD_BLOCK_OVERWRITE);
			HDD_CMD check_write = cmd_reader(hdd_client_operation(write_block, read_buff));
			
			free(read_buff);

			if (check_write.r == 1)
				return -1;

			hdd_files[fh].position += count;
			return count;
		}
		else { //Need to allocate and expand the block. Then delete the old block
			//Allocate a bigger write buffer and copy the old data into it
			char *extend_write_buff = malloc(hdd_files[fh].position + count);
			memcpy(extend_write_buff, read_buff, hdd_files[fh].size);
           		memcpy(&extend_write_buff[hdd_files[fh].position], data, count);
			
			free(read_buff);
			//generate block create command
			HddBitCmd create_block = cmd_generator(0, 0, 0, hdd_files[fh].position + count, HDD_BLOCK_CREATE);
			HDD_CMD create_result = cmd_reader(hdd_client_operation(create_block, extend_write_buff));
			
			free(extend_write_buff);
			
			if (create_result.r == 1)
				return -1;
			
			//generate cmd to delete the old block
			HddBitCmd old_block_delete = cmd_generator(hdd_files[fh].id, 0, 0, 0, HDD_BLOCK_DELETE); //generate block delete request. fileds: uint32_t block, uint8_t r, uint8_t flags, uint32_t block_size, uint8_t op
			HDD_CMD delete_result = cmd_reader(hdd_client_operation(old_block_delete, NULL));

			if (delete_result.r == 1)
				return -1;
				
			hdd_files[fh].size = hdd_files[fh].position + count;
			hdd_files[fh].id = create_result.block; //New Block ID of the extended block
			hdd_files[fh].position += count;
			
		
			return count;
		}

	}
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : HDD_SEEK
// Description  : Changes the current seek position of the file associated with the file handle fh to the position 
//		  loc
// Inputs       : File handle fh and a seek location loc
// Outputs      : Returns 0 on success and -1 on failure
//
int32_t hdd_seek(int16_t fh, uint32_t loc) {
	// Check if hdd is initialized
    	if (hdd_init == 0)
        	return -1;

	//Check file handle
	if (fh < 0 || fh > MAX_HDD_FILEDESCR)
		return -1;
	
	//If file at fh is not opened 
	if (hdd_files[fh].open == 0)
		return -1;

	//Check location value
	if (loc < 0 || loc > hdd_files[fh].size) //NEW FEATURE IN ASSG4! FINALLY CAN STORE BLOCK SIZE!
		return -1;
	//Set the seek position to loc
	hdd_files[fh].position = loc;
	return 0;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : hddIOUnitTest
// Description  : Perform a test of the HDD IO implementation
//
// Inputs       : None
// Outputs      : 0 if successful or -1 if failure

int hddIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fh, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	HDD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(HDD_MAX_BLOCK_SIZE);
	tbuf = malloc(HDD_MAX_BLOCK_SIZE);
	memset(cio_utest_buffer, 0x0, HDD_MAX_BLOCK_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (hdd_format() || hdd_mount()) {
		logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fh = hdd_open("temp_file.txt");
	if (fh == -1) {
		logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<HDD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}
		logMessage(LOG_INFO_LEVEL, "----------");

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "HDD_IO_UNIT_TEST : read %d at position %d", count, cio_utest_position);
			bytes = hdd_read(fh, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "HDD_IO_UNIT_TEST : read %d match", bytes);


			// update the position pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= HDD_MAX_BLOCK_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "HDD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "HDD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (hdd_seek(fh, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = hdd_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < HDD_MAX_BLOCK_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "HDD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = hdd_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : write failed [%d].", count);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "HDD_IO_UNIT_TEST : seek to position %d", count);
			if (hdd_seek(fh, count)) {
				logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "HDD_IO_UNIT_TEST : illegal test command.");
			break;

		}

	}

	// Close the files and cleanup buffers, assert on failure
	if (hdd_close(fh)) {
		logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : Failure close close.", fh);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (hdd_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "HDD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}
