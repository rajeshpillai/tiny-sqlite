CC := gcc
CLFLAGS := -std=c11 -Wall -Wextra -Wpedantic -O0 -g 
LDFLAGS :=

BIN := minidb 
SRC := src/main.c 
INC := -Iinclude 

.PHONY: all run clean 

all: $(BIN) 

$(BIN): $(SRC)
	$(CC) $(CLFLAGS) $(INC) -o $@ $^ $(LDFLAGS) 

run: $(BIN)
	./$(BIN)

clean:
	rm -f $(BIN)
