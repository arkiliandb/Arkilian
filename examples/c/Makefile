CC ?= clang
CFLAGS ?= -O2 -Wall -Wextra -I./include
LDFLAGS ?= -L./lib -larkilian -lcurl -lpthread -lm -Wl,-rpath,@loader_path/lib -Wl,-rpath,./lib

TARGETS = 01_simple_quickstart 02_advanced_s3_failover

all: $(TARGETS)

01_simple_quickstart: 01_simple_quickstart.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

02_advanced_s3_failover: 02_advanced_s3_failover.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS) *.sqlite*

.PHONY: all clean
