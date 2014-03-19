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

int server(void *ctx) {
  printf("starting server\n");
  int rc = 0;

  void *pull = zmq_socket(ctx, ZMQ_PULL);
  rc = zmq_bind(pull, "tcp://127.0.0.1:5876");

  void *push = zmq_socket(ctx, ZMQ_PUSH);
  rc = zmq_connect(push, "tcp://127.0.0.1:5877");

  for(;;) {
    char* buf = malloc(8);
    int read = zmq_recv(pull, buf, 8, 0);
    if(read == 0) {
      zmq_send(push, "", 0, 0);
      free(buf);
      break;
    }
    // printf("server received '%s'\n", buf);
    zmq_send(push, buf, 8, ZMQ_DONTWAIT);
    free(buf);
  }
  return 0;
}

int client(void *ctx, int pings) {
  int rc = 0;
  printf("starting client\n");
  
  void *pull = zmq_socket(ctx, ZMQ_PULL);
  rc = zmq_bind(pull, "tcp://127.0.0.1:5877");

  void *push = zmq_socket(ctx, ZMQ_PUSH);
  rc = zmq_connect(push, "tcp://127.0.0.1:5876");

  int i;
  for(i = 0; i < pings; i++) {
    double timestamp_before = timestamp();
    
    zmq_send(push, "ping123", 8, 0);

    char *buf = malloc(8);
    ssize_t read = zmq_recv(pull, buf, 8, 0);

    if(read == 0) {
      printf("server exited prematurely\n");
      free(buf);
      break;
    }

    // printf("client received '%s'\n", buf);
    free(buf);
    
    double timestamp_after = timestamp();
    fprintf(stderr, "%i %lf\n", i, timestamp_after - timestamp_before);
  }
  zmq_send(push, "", 0, 0);

  printf("client did %d pings\n", pings);
  return 0;
}

int usage(int argc, char** argv) {
  printf("usage: %s <number of pings>\n", argv[0]);
  return -1;
}

int main(int argc, char** argv) {
  if(argc != 2) {
    return usage(argc, argv);
  } 

  void* ctx = zmq_ctx_new();

  if(fork() == 0) {
    // TODO: we should wait until we know the server is ready
    int pings = 0;
    sscanf(argv[1], "%d", &pings);
    return client(ctx, pings);
  } else {
    return server(ctx);
  }
}
