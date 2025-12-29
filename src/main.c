#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

#define INPUT_BUFFER_SIZE 1024

static void print_prompt(void) {
    printf("minidb> ");
}

static bool read_input(char *buffer, size_t buffer_size) {
    if (!fgets(buffer, buffer_size, stdin)) {
        puts("\n");
        return false;
    }

    // Remove trailing newline if present
    size_t len = strlen(buffer);
    if (len > 0 && buffer[len - 1] == '\n') {
        buffer[len - 1] = '\0';
    }
    return true;
}

/* --------- Meta commands -------------*/
static bool do_meta_command(const char *input) {
    if (strcmp(input, ".exit") == 0) {
        return false;
    }
    if (strcmp(input, ".help") == 0) {
        puts("Meta commands: ");
        puts("  .help   Show this help");
        puts("  .exit   Exit the program");
        return true;
    }

    printf("Unrecognized meta command: '%s'\n", input);
    return true;
}

/* ------ SQL Statements stub*/

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
} Statement;


/* Case-insensitive prefix check */
static bool starts_with_ignore_case(const char *str, const char *prefix) {
    while (*prefix && *str) {
        if (tolower((unsigned char)*str) != tolower((unsigned char)*prefix)) {
            return false;
        }
        str++;
        prefix++;
    }
    return *prefix == '\0';
}

static bool prepare_statement(const char *input, Statement *statement) {
    if (starts_with_ignore_case(input, "insert")) {
        statement->type = STATEMENT_INSERT;
        return true;
    }
    if (starts_with_ignore_case(input, "select")) {
        statement->type = STATEMENT_SELECT;
        return true;
    }

    printf("Unrecognized SQL statement: '%s'\n", input);
    return false;
}

static void execute_statement(const Statement *statement) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            puts("Insert not implemented yet");
            break;
        case STATEMENT_SELECT:
            puts("Select not implemented yet");
            break;
    }
}


int main(void) {
    char input_buffer[INPUT_BUFFER_SIZE];
    puts("mindb (sqlite-like toy DB)");
    puts("Enter .help to show available commands.");

    while(1) {
        print_prompt();
        
        if (!read_input(input_buffer, sizeof(input_buffer))) {
            break;
        }

        if (strlen(input_buffer) == 0) {
            continue;
        }

        if (input_buffer[0] == '.') {
            if (!do_meta_command(input_buffer)) {
                break;
            }
            continue;
        }

        Statement statement;
        if (!prepare_statement(input_buffer, &statement)) {
            continue;
        }

        execute_statement(&statement);
    }

    puts("Bye!");
    return 0;
}