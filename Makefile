CC=gcc
CFLAGS=-std=c11 -Wall -Wextra -Wpedantic -O0 -g

INCLUDES=-Isrc/include
SRC=src/main.c src/pager.c src/btree.c src/db.c
OUT=tinydb

all: $(OUT)

$(OUT): $(SRC)
	$(CC) $(CFLAGS) $(INCLUDES) -o $(OUT) $(SRC)

run: all
	./$(OUT)

clean:
	rm -f $(OUT) test.db
