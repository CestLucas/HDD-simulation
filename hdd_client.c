////////////////////////////////////////////////////////////////////////////////
//
//  File          : hdd_client.c
//  Description   : This is the client side of the CRUD communication protocol.
//
//   Author       : Patrick McDaniel
//  Last Modified : Thu Oct 30 06:59:59 EDT 2014
//

// Include Files
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <stdint.h>

// Project Include Files
#include <hdd_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <hdd_driver.h>

////////////////////////////////////////////////////////////////////////////////
//
// Function     : hdd_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : cmd - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

//Global Variable
int socket_fd = -1; //socket file descriptor
struct sockaddr_in caddr; //sockaddr_in

HddBitResp hdd_client_operation(HddBitCmd cmd, void *buf) {
	uint8_t op = (uint8_t) (cmd >> 62); //extract the op field from the command
	uint8_t flag = ((uint8_t) (cmd >> 33)) & 7; //extract the flag from the cmd
	//Get the buf size
	uint32_t buf_size_comp = 0;
	buf_size_comp = (~buf_size_comp) >> 6;
	uint32_t buf_size = ((uint32_t) (cmd >> 36)) & buf_size_comp;

	//Step 1: check if needs to make a connection to the server
	if (flag == HDD_INIT){
		//Step 1: create socket
		caddr.sin_family = AF_INET;
		caddr.sin_port = htons(HDD_DEFAULT_PORT);

		//If failed to convert IPv4 address to binary
		if (inet_aton(HDD_DEFAULT_IP, &caddr.sin_addr) == 0)
			return -1;

        	socket_fd = socket(PF_INET, SOCK_STREAM, 0);
		//If failed to create socket
        	if (socket_fd == -1)
            		return(-1);

		//If cocket connection failed
		if (connect(socket_fd, (const struct sockaddr *)&caddr, sizeof(struct sockaddr)) == -1)
			return -1;
	}
	
	//Step 2: send cmd and/or buf to the server
	//Local variables used during send
	HddBitCmd *converted_cmd = malloc(sizeof(HddBitCmd)); //converted cmd
	int cmd_byte_sent = 0; //..use to send cmd
	int buf_byte_sent = 0; //..use to send buf
	//1. Convert the cmd to binary
	*converted_cmd = htonll64(cmd); //converted cmd
	//2. send the cmd
	//use a while loop to make sure all bytes are correctly sent
	while (cmd_byte_sent < sizeof(HddBitCmd)){
		cmd_byte_sent += write(socket_fd, &((char *)converted_cmd)[cmd_byte_sent], sizeof(HddBitCmd) - cmd_byte_sent);
	}
	free(converted_cmd);
	//Check if bytes are sent correctly
	if (cmd_byte_sent == -1)
		return -1;
	//3. send buf if the cmd is block create or block overwrite
	if (op == HDD_BLOCK_CREATE || op == HDD_BLOCK_OVERWRITE){
		while (buf_byte_sent < buf_size)
			buf_byte_sent += write(socket_fd, &((char *)buf)[buf_byte_sent], buf_size - buf_byte_sent);
		//Check if bytes are sent correctly
		if (buf_byte_sent == -1)
			return -1;
	}
	//..done sending cmd & buf

	//Step 3: receive server response
	//Local variables used during receive
	int res_byte_read = 0; //..use below to read the server response
	int res_block_byte_read = 0; //.. use to read bytes of block
	HddBitResp *res = malloc(sizeof(HddBitResp)); //server response
	HddBitResp converted_res; //converted back server response (one to be returned in hdd_client_operation)
	//1. get server response
	while (res_byte_read < sizeof(HddBitResp))
		res_byte_read += read(socket_fd, &((char *)res)[res_byte_read], sizeof(HddBitResp)-res_byte_read);
	//Check if bytes are received correctly
	if (res_byte_read == -1)
		return -1;
	//2. translate server response back
	converted_res = ntohll64(*res);
	free(res); //free the unconverted response
	//3. check if needed to read block
	uint8_t res_op = (uint8_t) (converted_res >> 62);
	if (res_op == HDD_BLOCK_READ){
		uint32_t res_size; //block size in the server response
		uint32_t res_size_comp = 0; //complementary to read the block size field in the response
		res_size_comp = (~res_size_comp) >> 6; //get the block_size in the response
		res_size = ((uint32_t) (converted_res >> 36)) & res_size_comp;
		while (res_block_byte_read < res_size)
			res_block_byte_read += read(socket_fd, &((char *)buf)[res_block_byte_read], res_size-res_block_byte_read);
		//Check if bytes are received correctly
		if (res_block_byte_read == -1)
			return -1;
	}
	//..done receiving

	//Step 4: close the connection if needed
	if (flag == HDD_SAVE_AND_CLOSE){
		close(socket_fd);
		socket_fd = -1;
	}

	//Finally...
	return converted_res;
}


