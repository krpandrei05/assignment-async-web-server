// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	char buffer[BUFSIZ] = "HTTP/1.1 200 OK\r\n"
		"Content-Length: %zu\r\n"
		"Connection: close\r\n"
		"\r\n";

	int len = snprintf(conn->send_buffer, BUFSIZ, buffer, conn->file_size);

	if (len < 0)
		len = 0;
	size_t send_len = (size_t)len;

	if (send_len >= BUFSIZ)
		send_len = BUFSIZ - 1;
	conn->send_len = send_len;
	conn->send_pos = 0;
	conn->state = STATE_SENDING_HEADER;
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	char buffer[BUFSIZ] = "HTTP/1.1 404 Not Found\r\n"
		"Content-Length: 0\r\n"
		"Connection: close\r\n"
		"\r\n";

	memcpy(conn->send_buffer, buffer, strlen(buffer) + 1);
	conn->send_len = strlen(buffer);
	conn->send_pos = 0;
	conn->state = STATE_SENDING_404;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	if (conn->have_path == 0)
		return RESOURCE_TYPE_NONE;

	if (strncmp(conn->request_path, "/static/", 8) == 0) {
		snprintf(conn->filename, BUFSIZ, "%s%s", AWS_ABS_STATIC_FOLDER, conn->request_path + 8);
		conn->res_type = RESOURCE_TYPE_STATIC;
		return RESOURCE_TYPE_STATIC;
	}

	if (strncmp(conn->request_path, "/dynamic/", 9) == 0) {
		snprintf(conn->filename, BUFSIZ, "%s%s", AWS_ABS_DYNAMIC_FOLDER, conn->request_path + 9);
		conn->res_type = RESOURCE_TYPE_DYNAMIC;
		return RESOURCE_TYPE_DYNAMIC;
	}

	conn->res_type = RESOURCE_TYPE_NONE;
	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn;

	conn = calloc(1, sizeof(*conn));
	DIE(conn == NULL, "calloc");

	conn->sockfd = sockfd;
	conn->fd = -1;
	conn->eventfd = eventfd(0, EFD_NONBLOCK);
	DIE(conn->eventfd < 0, "eventfd");

	conn->ctx = ctx;
	conn->piocb[0] = &conn->iocb;
	conn->state = STATE_INITIAL;
	conn->res_type = RESOURCE_TYPE_NONE;

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	size_t bytes_to_read;

	if (conn->file_pos >= conn->file_size) {
		conn->async_read_len = 0;
		return;
	}

	bytes_to_read = conn->file_size - conn->file_pos;
	if (bytes_to_read > BUFSIZ)
		bytes_to_read = BUFSIZ;

	conn->send_len = 0;
	conn->send_pos = 0;

	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, bytes_to_read, (long long)conn->file_pos);
	io_set_eventfd(&conn->iocb, conn->eventfd);

	int rc = io_submit(conn->ctx, 1, conn->piocb);

	DIE(rc < 0, "io_submit");

	conn->state = STATE_ASYNC_ONGOING;
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	int rc;

	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr(sockfd)");

	rc = w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr(eventfd)");

	if (conn->fd >= 0)
		close(conn->fd);

	close(conn->eventfd);
	tcp_close_connection(conn->sockfd);

	conn->state = STATE_CONNECTION_CLOSED;
	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	int rc;

	/* TODO: Accept new connection. */
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;

	int sockfd = accept(listenfd, (SSA *)&addr, &addrlen);

	DIE(sockfd < 0, "accept");

	/* TODO: Set socket to be non-blocking. */
	int flags = fcntl(sockfd, F_GETFL, 0);

	DIE(flags < 0, "fcntl F_GETFL");

	rc = fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
	DIE(rc < 0, "fcntl F_SETFL");

	/* TODO: Instantiate new connection handler. */
	struct connection *conn = connection_create(sockfd);

	/* TODO: Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_ptr_in sockfd");

	rc = w_epoll_add_ptr_in(epollfd, conn->eventfd, conn);
	DIE(rc < 0, "w_epoll_add_ptr_in eventfd");

	/* TODO: Initialize HTTP_REQUEST parser. */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	char abuffer[64];
	int rc = get_peer_address(conn->sockfd, abuffer, sizeof(abuffer));

	if (rc < 0)
		strcpy(abuffer, "unknown");

	ssize_t bytes_recv = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ - conn->recv_len, 0);

	if (bytes_recv < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return;
		dlog(LOG_ERR, "Error in communication from %s\n", abuffer);
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}
	if (bytes_recv == 0) {
		dlog(LOG_INFO, "Conncection closed from %s\n", abuffer);
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	conn->recv_len += bytes_recv;
	if (conn->recv_len < BUFSIZ)
		conn->recv_buffer[conn->recv_len] = '\0';
	if (strstr(conn->recv_buffer, "\r\n\r\n"))
		conn->state = STATE_REQUEST_RECEIVED;
	else
		conn->state = STATE_RECEIVING_DATA;
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	struct stat st;

	if (conn->fd >= 0) {
		close(conn->fd);
		conn->fd = -1;
	}

	conn->fd = open(conn->filename, O_RDONLY);
	if (conn->fd < 0) {
		ERR("open");
		return -1;
	}

	if (fstat(conn->fd, &st) < 0) {
		ERR("fstat");
		close(conn->fd);
		conn->fd = -1;
		return -1;
	}

	conn->file_size = (size_t)st.st_size;
	conn->file_pos = 0;

	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	eventfd_t done;

	int rc = eventfd_read(conn->eventfd, &done);

	if (rc < 0 && errno != EAGAIN) {
		ERR("eventfd_read");
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	struct io_event event;

	rc = io_getevents(conn->ctx, 1, 1, &event, NULL);
	if (rc < 0) {
		ERR("io_getevents");
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	if (event.res < 0) {
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	conn->send_len = event.res;
	conn->send_pos = 0;
	conn->file_pos += conn->send_len;

	if (conn->send_len == 0)
		conn->state = STATE_DATA_SENT;
	else
		conn->state = STATE_SENDING_DATA;

	rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_update_ptr_out");
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	conn->have_path = 0;
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;

	size_t bytes_parsed = http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);

	if (bytes_parsed != conn->recv_len || !conn->have_path)
		return -1;

	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	if (conn->file_size <= conn->file_pos) {
		conn->state = STATE_DATA_SENT;
		return STATE_DATA_SENT;
	}

	size_t remaining = conn->file_size - conn->file_pos;
	off_t offset = (off_t)conn->file_pos;

	ssize_t bytes_sent = sendfile(conn->sockfd, conn->fd, &offset, remaining);

	if (bytes_sent < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			conn->state = STATE_SENDING_DATA;
			return STATE_SENDING_DATA;
		}
		ERR("sendfile");
		conn->state = STATE_CONNECTION_CLOSED;
		return STATE_CONNECTION_CLOSED;
	}

	conn->file_pos = (size_t)offset;

	if (conn->file_size <= conn->file_pos)
		conn->state = STATE_DATA_SENT;
	else
		conn->state = STATE_SENDING_DATA;

	return conn->state;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	if (conn->send_len <= conn->send_pos)
		return 0;

	size_t remaining = conn->send_len - conn->send_pos;
	ssize_t bytes_sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, remaining, 0);

	if (bytes_sent < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0;
		return -1;
	}

	if (bytes_sent == 0)
		return 0;

	conn->send_pos += (size_t)bytes_sent;

	return (int)bytes_sent;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	if (conn->state == STATE_ASYNC_ONGOING)
		return 0;

	if (conn->file_size <= conn->file_pos) {
		conn->state = STATE_DATA_SENT;
		return 0;
	}

	connection_start_async_io(conn);

	int rc = w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);

	if (rc < 0) {
		ERR("w_epoll_updatE_ptr_in");
		return -1;
	}

	return 0;
}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */
	if (conn->state == STATE_ASYNC_ONGOING) {
		connection_complete_async_io(conn);
		return;
	}

	switch (conn->state) {
	case STATE_INITIAL:
	case STATE_RECEIVING_DATA:
		receive_data(conn);
		if (conn->state != STATE_REQUEST_RECEIVED)
			return;
	case STATE_REQUEST_RECEIVED:
		if (parse_header(conn) < 0 || connection_get_resource_type(conn) == RESOURCE_TYPE_NONE
		|| connection_open_file(conn) < 0)
			connection_prepare_send_404(conn);
		else
			connection_prepare_send_reply_header(conn);
		int rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);

		DIE(rc < 0, "w_epoll_update_ptr_out");
		break;
	default:
		printf("shouldn't get here %d\n", conn->state);
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */
	int rc;

	switch (conn->state) {
	case STATE_SENDING_HEADER:
		rc = connection_send_data(conn);
		if (rc < 0) {
			conn->state = STATE_CONNECTION_CLOSED;
			break;
		}
		if (conn->send_len <= conn->send_pos) {
			conn->state = STATE_HEADER_SENT;
			if (conn->res_type == RESOURCE_TYPE_STATIC)
				conn->state = connection_send_static(conn);
			else if (conn->res_type == RESOURCE_TYPE_DYNAMIC)
				connection_send_dynamic(conn);
		}
		break;
	case STATE_HEADER_SENT:
		if (conn->res_type == RESOURCE_TYPE_STATIC)
			conn->state = connection_send_static(conn);
		else if (conn->res_type == RESOURCE_TYPE_DYNAMIC)
			connection_send_dynamic(conn);
		break;
	case STATE_SENDING_DATA:
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			conn->state = connection_send_static(conn);
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			rc = connection_send_data(conn);
			if (rc < 0) {
				conn->state = STATE_CONNECTION_CLOSED;
				break;
			}
			if (conn->send_len <= conn->send_pos) {
				if (conn->file_size <= conn->file_pos)
					conn->state = STATE_DATA_SENT;
				else
					connection_send_dynamic(conn);
			}
		}
		break;
	case STATE_SENDING_404:
		rc = connection_send_data(conn);
		if (rc < 0) {
			conn->state = STATE_CONNECTION_CLOSED;
			break;
		}
		if (conn->send_len <= conn->send_pos)
			conn->state = STATE_404_SENT;
		break;
	default:
		ERR("Unexpected state\n");
		exit(1);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	if (event & EPOLLIN)
		handle_input(conn);

	if (event & EPOLLOUT)
		handle_output(conn);

	if (conn->state == STATE_CONNECTION_CLOSED || conn->state == STATE_DATA_SENT || conn->state == STATE_404_SENT)
		connection_remove(conn);
}

int main(void)
{
	int rc;

	/* TODO: Initialize asynchronous operations. */
	rc = io_setup(1, &ctx);
	DIE(rc < 0, "io_setup");

	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	/* TODO: Add server socket to epoll object*/
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	/* Uncomment the following line for debugging. */
	// dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w-epoll_wait_infinite");

		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */
		if (rev.data.fd == listenfd) {
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			handle_client(rev.events, rev.data.ptr);
		}
	}

	return 0;
}
