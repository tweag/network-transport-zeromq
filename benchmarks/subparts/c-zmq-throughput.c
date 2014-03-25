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
#include <zmq.h>

// Note: this is not consistent across CPUs (and hence across threads on multicore machines) 
double timestamp() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  return ((double) tp.tv_sec) * 1e6 + (double) tp.tv_usec;
}

int server(void *ctx, int pings, int size) {
  printf("starting server\n");
  int rc = 0;

  void *pull = zmq_socket(ctx, ZMQ_PULL);
  rc = zmq_bind(pull, "tcp://127.0.0.1:5876");

  void *push = zmq_socket(ctx, ZMQ_PUSH);
  rc = zmq_connect(push, "tcp://127.0.0.1:5877");

  int counter = 0, i = 0;
  for(i=0;i<pings;i++) {
    char* buf = malloc(size);
    int read = zmq_recv(pull, buf, size, 0);
    counter++;
    // printf("server received '%s'\n", buf);
    free(buf);
  }
  zmq_send(push, &counter, sizeof(int), 0);
  return 0;
}

int client(void *ctx, int pings, int size) {
  int rc = 0;
  printf("starting client\n");
  
  void *pull = zmq_socket(ctx, ZMQ_PULL);
  rc = zmq_bind(pull, "tcp://127.0.0.1:5877");

  void *push = zmq_socket(ctx, ZMQ_PUSH);
  rc = zmq_connect(push, "tcp://127.0.0.1:5876");

  char * msg = malloc(size);
  double timestamp_before = timestamp();
  int i;
  for(i = 0; i < pings; i++) {
    zmq_send(push, msg, size, 0);
    // printf("client received '%s'\n", buf);
  }
  zmq_send(push, "", 0, 0);
  zmq_recv(pull, msg, sizeof(int), 0);

  double timestamp_after = timestamp();
  fprintf(stderr, "%i %lf\n", size, timestamp_after - timestamp_before);

  printf("client did %d pings\n", pings);
  return 0;
}

int usage(int argc, char** argv) {
  printf("usage: %s <number of pings> <packet size>\n", argv[0]);
  return -1;
}

int main(int argc, char** argv) {
  if(argc != 3) {
    return usage(argc, argv);
  } 

  void* ctx = zmq_ctx_new();

  int pings = 0;
  int size  = 0;
  sscanf(argv[1], "%d", &pings);
  sscanf(argv[2], "%d", &size);

  if(fork() == 0) {
    // TODO: we should wait until we know the server is ready
    return client(ctx, pings, size);
  } else {
    return server(ctx, pings, size);
  }
}
