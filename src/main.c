#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

#define INPUT_BUFFER_SIZE 1024

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_ROWS 1000



// Row and Table structure
typedef struct {
    int id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    Row rows[TABLE_MAX_ROWS];
    size_t num_rows;
} Table;

static Table table;

// Input handling

static void print_prompt(void) {
    printf("tinydb> ");
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
    Row row_to_insert;
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

/* INSERT syntax (simple version)*/
static bool prepare_insert(const char *input, Statement *statement) {
    statement->type = STATEMENT_INSERT;
    int args_assigned = sscanf(
        input,
        "insert %d %32s %255s",
        &statement->row_to_insert.id,
        statement->row_to_insert.username,
        statement->row_to_insert.email
    );
    
    if (args_assigned < 3) {
        puts("Syntax error. Usage: insert <id> <username> <email>");
        return false;
    }

    return true;
}

static bool prepare_statement(const char *input, Statement *statement) {
    if (starts_with_ignore_case(input, "insert")) {
        return prepare_insert(input, statement);
    }
    if (starts_with_ignore_case(input, "select")) {
        statement->type = STATEMENT_SELECT;
        return true;
    }

    printf("Unrecognized SQL statement: '%s'\n", input);
    return false;
}

/* execution stub*/

static void execute_insert(const Statement *statement) {
    if (table.num_rows >= TABLE_MAX_ROWS) {
        puts("Error: Table is full");
        return;
    }
    table.rows[table.num_rows] = statement->row_to_insert;
    table.num_rows++;

    puts("Executed.");
}

static void print_row(const Row *row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

static void execute_select(void) {
    for (size_t i = 0; i < table.num_rows; i++) {
        print_row(&table.rows[i]);
    }
}

static void execute_statement(const Statement *statement) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            execute_insert(statement);
            break;
        case STATEMENT_SELECT:
            execute_select();
            break;
    }
}


int main(void) {
    char input_buffer[INPUT_BUFFER_SIZE];
    table.num_rows = 0;

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