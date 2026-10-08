COMPILER    := g++
CPP_STD     := -std=c++20
CPP_FLAGS   := -O2 -Wall -Wextra
INCLUDES    := -I../../include
LDLIBS      := -luring -pthread

SERVER      := echoserver
CLIENT      := echoclient
SERVER_SRC  := echoserver.cc
CLIENT_SRC  := echoclient.cc

all: $(SERVER) $(CLIENT)

$(SERVER): $(SERVER_SRC)
	$(COMPILER) $(CPP_STD) $(CPP_FLAGS) -o $@ $^ $(INCLUDES) $(LDLIBS)

$(CLIENT): $(CLIENT_SRC)
	$(COMPILER) $(CPP_STD) $(CPP_FLAGS) -o $@ $^ $(INCLUDES) $(LDLIBS)

.PHONY: all tcpserver tcpclient udpserver udpclient tcpserver6 tcpclient6 udpserver6 udpclient6 clean

tcpserver:
	./$(SERVER) --mode=tcp-echo --ip=0.0.0.0 --port=8080 --thread=2

tcpclient:
	./$(CLIENT) --mode=tcp-echo --ip=127.0.0.1 --port=8080

udpserver:
	./$(SERVER) --mode=udp-echo --ip=0.0.0.0 --port=8080 --thread=2

udpclient:
	./$(CLIENT) --mode=udp-echo --ip=127.0.0.1 --port=8080

tcpserver6:
	./$(SERVER) --mode=tcp-echo --ip=:: --port=8080 --thread=2

tcpclient6:
	./$(CLIENT) --mode=tcp-echo --ip=::1 --port=8080

udpserver6:
	./$(SERVER) --mode=udp-echo --ip=:: --port=8080 --thread=2

udpclient6:
	./$(CLIENT) --mode=udp-echo --ip=::1 --port=8080

clean:
	rm -f $(SERVER) $(CLIENT)
	rm -rf ./log