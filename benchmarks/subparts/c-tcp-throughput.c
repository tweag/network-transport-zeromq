#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>

// Note: this is not consistent across CPUs (and hence across threads on multicore machines) 
double timestamp() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  return ((double) tp.tv_sec) * 1e6 + (double) tp.tv_usec;
}

int server(int pings, int size) {
  printf("starting server\n");

  struct addrinfo hints, *res;
  int error, server_socket;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family   = PF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags    = AI_PASSIVE;

  error = getaddrinfo(NULL, "8080", &hints, &res); 
  if(error) {
    printf("server error: %s\n", gai_strerror(error));
    return -1;
  }

  server_socket = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if(server_socket < 0) {
    printf("server error: could not create socket\n");
    return -1;
  }

  int yes = 1;
  if(setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) < 0) {
    printf("server error: could not set socket options\n");
    return -1;
  }

  if(bind(server_socket, res->ai_addr, res->ai_addrlen) < 0) {
    printf("server error: could not bind to socket\n");
    return -1;
  }

  listen(server_socket, 5);

  int client_socket;
  struct sockaddr_storage client_addr;
  socklen_t addr_size;
  client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &addr_size); 

  int i = 0, counter=0;
  for(i=0;i<pings;i++) {
    char* buf = malloc(size);
    ssize_t read = 0;
    for (;size-read;) read+=recv(client_socket, buf, size-read, 0);
    counter++;
    free(buf);
  }
  size_t sent;
  for (;sizeof(int)-sent;) sent+=send(client_socket, &counter, sizeof(int)-sent, 0);

  freeaddrinfo(res);
  return 0;
}

int client(int pings, int size) {
  printf("starting client\n");
  
  struct addrinfo hints, *res;
  int error, client_socket, i;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family   = PF_INET;
  hints.ai_socktype = SOCK_STREAM;

  error = getaddrinfo("127.0.0.1", "8080", &hints, &res); 
  if(error) {
    printf("client error: %s\n", gai_strerror(error));
    return -1;
  }

  client_socket = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if(client_socket < 0) {
    printf("client error: could not create socket\n");
    return -1;
  }

  if(connect(client_socket, res->ai_addr, res->ai_addrlen) < 0) {
    printf("client error: could not connect: %s\n", strerror(errno));
    return -1;
  }

  char * msg = malloc(size);
  double timestamp_before = timestamp();
  for(i = 0; i < pings; i++) {
    int sent=0;
    for (;size-sent;) sent+=send(client_socket, msg, size-sent, 0);

  }

  size_t read = 0;
  for (;sizeof(int)-read;) read+=recv(client_socket, msg, sizeof(int)-read, 0);
  double timestamp_after = timestamp();
  fprintf(stderr, "%i %lf\n", size, timestamp_after - timestamp_before);
  printf("client did %d pings\n", pings);

  freeaddrinfo(res);
  return 0;
}

int usage(int argc, char** argv) {
  printf("usage: %s <number of pings>\n", argv[0]);
  return -1;
}

int main(int argc, char** argv) {
  if(argc != 3) {
    return usage(argc, argv);
  } 

  int pings = 0;
  int size  = 0;
  sscanf(argv[1], "%d", &pings);
  sscanf(argv[2], "%d", &size);
  if(fork() == 0) {
    // TODO: we should wait until we know the server is ready
    return client(pings,size);
  } else {
    return server(pings,size);
  }
}
