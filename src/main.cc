#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <string>

#include "util.h"

struct InputBuffer {
  std::string buffer;
};

enum MetaCommandResult {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND,
};
enum PrepareResult {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR,
};
enum ExecuteResult {
  EXECUTE_SUCCESS,
  EXECUTE_FAIL,
  EXECUTE_TABLE_FULL,
};

enum StatementType {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
};

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

struct Row {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE];
  char email[COLUMN_EMAIL_SIZE];
};

#define size_of_attribute(Struct, Attribute) \
  sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void serialize_row(Row* source, void* destination)
{
  memcpy((char*)destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy((char*)destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy((char*)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination)
{
  memcpy(&(destination->id), (const char*)source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), (const char*)source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), (const char*)source + EMAIL_OFFSET, EMAIL_SIZE);
}

void print_row(const Row* row)
{
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table {
  uint32_t num_rows;
  void* pages[TABLE_MAX_PAGES];
};

void* row_slot(Table* table, uint32_t row_num)
{
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void* page = table->pages[page_num];
  if (page == nullptr) {
    // allocate memory only when we try to access page
    page = table->pages[page_num] = malloc(PAGE_SIZE);
  }
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return (char*)page + byte_offset;
}

struct Statement {
  StatementType type;
  Row row_to_insert;  // only used by insert statement
};

InputBuffer* new_input_buffer()
{
  InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
  return input_buffer;
}

void print_prompt() {
  printf("db > ");
}

void read_input(InputBuffer* input_buffer)
{
  if (!std::getline(std::cin, input_buffer->buffer))
  {
      printf("Error reading input\n");
      exit(EXIT_FAILURE);
  }
}

void close_input_buffer(InputBuffer* input_buffer)
{
  free(input_buffer);
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer)
{
  if (input_buffer->buffer == ".exit") {
    printf("byte...\n");
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement)
{
  if (strutil::startsWith(input_buffer->buffer, "insert")) {
    statement->type = STATEMENT_INSERT;
    int args_assigned = sscanf(
        input_buffer->buffer.c_str(),
        "insert %d %s %s",
        &(statement->row_to_insert.id),
        statement->row_to_insert.username,
        statement->row_to_insert.email);
    if (args_assigned < 3) {
      return PREPARE_SYNTAX_ERROR;
    }
    return PREPARE_SUCCESS;
  }
  if (strutil::startsWith(input_buffer->buffer, "select")) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement, Table* table)
{
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(statement->row_to_insert);
  serialize_row(row_to_insert, row_slot(table, table->num_rows));
  table->num_rows += 1;

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table)
{
  Row row;
  for (uint32_t i = 0; i < table->num_rows; i++) {
    deserialize_row(row_slot(table, i), &row);
    print_row(&row);
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table)
{
  switch (statement->type) {
    case STATEMENT_INSERT:
    return execute_insert(statement, table);
    case STATEMENT_SELECT:
    return execute_select(statement, table);
  }
  return EXECUTE_FAIL;
}

Table* new_table()
{
  Table* table = (Table*)malloc(sizeof(Table));
  table->num_rows = 0;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    table->pages[i] = nullptr;
  }
  return table;
}

void free_table(Table* table)
{
  for (int i = 0; table->pages[i]; i++) {
    free(table->pages[i]);
  }
  free(table);
}

int main(int argc, char** argv)
{
  Table* table = new_table();

  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);
    if (input_buffer->buffer.empty()) {
      continue;
    }
    //printf("input: %s\n", input_buffer->buffer.c_str());

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer)) {
      case META_COMMAND_SUCCESS:
        continue;
      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command '%s'\n", input_buffer->buffer.c_str());
        continue;
      default:
        break;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case PREPARE_SUCCESS:
      break;
      case PREPARE_SYNTAX_ERROR:
      printf("Syntax error. Could not parse statement.\n");
      continue;
      case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer.c_str());
      continue; // go to wait for input
    }

    switch (execute_statement(&statement, table)) {
      case EXECUTE_SUCCESS:
      printf("Executed.\n");
      break;
      case EXECUTE_TABLE_FULL:
      printf("Error: Table full.\n");
      break;
      default:
      break;
    }
  }
  return 0;
}
